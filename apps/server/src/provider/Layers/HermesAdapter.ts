import {
  ApprovalRequestId,
  EventId,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeRequestId,
  type HermesSettings,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import type * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import { HERMES_DEFAULT_MODEL, makeHermesAcpRuntime } from "../acp/HermesAcpSupport.ts";
import { buildRuntimeInstructions } from "../RuntimeInstructions.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const PROVIDER = ProviderDriverKind.make("hermes");
const RESUME_VERSION = 1 as const;

interface PendingApproval {
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
}

interface HermesSession {
  readonly threadId: ThreadId;
  readonly acpSessionId: string;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly acp: AcpSessionRuntime.AcpSessionRuntime["Service"];
  activeTurnId: TurnId | undefined;
  stopped: boolean;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseResume(raw: unknown): { sessionId: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== RESUME_VERSION) return undefined;
  if (typeof raw.sessionId !== "string" || !raw.sessionId.trim()) return undefined;
  return { sessionId: raw.sessionId.trim() };
}

function permissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  const kind =
    decision === "acceptForSession"
      ? "allow_always"
      : decision === "accept"
        ? "allow_once"
        : "reject_once";
  const match = request.options.find((entry) => entry.kind === kind);
  const id = match?.optionId.trim();
  if (id) return id;
  if (decision === "acceptForSession") {
    return request.options.find((entry) => entry.kind === "allow_once")?.optionId.trim();
  }
  return undefined;
}

export interface HermesAdapterOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

export const makeHermesAdapter = Effect.fn("makeHermesAdapter")(function* (
  settings: HermesSettings,
  options?: HermesAdapterOptions,
): Effect.fn.Return<
  ProviderAdapterShape<ProviderAdapterError>,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
> {
  const crypto = yield* Crypto.Crypto;
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const boundInstanceId = options?.instanceId;
  const sessions = new Map<ThreadId, HermesSession>();
  const runtimeEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const randomId = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "crypto/randomUUIDv4",
          detail: "Failed to generate a Hermes runtime identifier.",
          cause,
        }),
    ),
  );
  const stamp = () =>
    Effect.all({
      eventId: Effect.map(randomId, (id) => EventId.make(id)),
      createdAt: nowIso,
    });

  const requireSession = (threadId: ThreadId) => {
    const ctx = sessions.get(threadId);
    if (!ctx || ctx.stopped) {
      return Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
    }
    return Effect.succeed(ctx);
  };

  /** A picked model id (e.g. `custom:dsv4.1-flash`), or undefined to keep Hermes' own. */
  const selectedHermesModel = (
    selection: { readonly instanceId: string; readonly model?: string } | undefined,
  ) => {
    if (boundInstanceId === undefined || selection?.instanceId !== boundInstanceId)
      return undefined;
    const model = selection.model?.trim();
    return model && model !== HERMES_DEFAULT_MODEL ? model : undefined;
  };

  const stopSessionInternal = (ctx: HermesSession) =>
    Effect.gen(function* () {
      if (ctx.stopped) return;
      ctx.stopped = true;
      for (const pending of ctx.pendingApprovals.values()) {
        yield* Deferred.succeed(pending.decision, "cancel" as const).pipe(Effect.ignore);
      }
      ctx.pendingApprovals.clear();
      yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
      sessions.delete(ctx.threadId);
      yield* PubSub.publish(runtimeEvents, {
        type: "session.exited",
        ...(yield* stamp()),
        provider: PROVIDER,
        threadId: ctx.threadId,
        payload: { exitKind: "graceful" },
      });
    });

  const consumeEvents = (ctx: HermesSession) =>
    Stream.runDrain(
      Stream.mapEffect(ctx.acp.getEvents(), (event) =>
        Effect.gen(function* () {
          if (event._tag === "EventStreamBarrier") {
            yield* Deferred.succeed(event.acknowledge, undefined);
            return;
          }
          const turnId = ctx.activeTurnId;
          if (turnId === undefined) return;
          const eventStamp = yield* stamp();
          switch (event._tag) {
            case "AssistantItemStarted":
            case "AssistantItemCompleted":
              yield* PubSub.publish(
                runtimeEvents,
                makeAcpAssistantItemEvent({
                  stamp: eventStamp,
                  provider: PROVIDER,
                  threadId: ctx.threadId,
                  turnId,
                  itemId: event.itemId,
                  lifecycle:
                    event._tag === "AssistantItemStarted" ? "item.started" : "item.completed",
                }),
              );
              return;
            case "PlanUpdated":
              yield* PubSub.publish(
                runtimeEvents,
                makeAcpPlanUpdatedEvent({
                  stamp: eventStamp,
                  provider: PROVIDER,
                  threadId: ctx.threadId,
                  turnId,
                  payload: event.payload,
                  source: "acp.jsonrpc",
                  method: "session/update",
                  rawPayload: event.rawPayload,
                }),
              );
              return;
            case "ToolCallUpdated":
              yield* PubSub.publish(
                runtimeEvents,
                makeAcpToolCallEvent({
                  stamp: eventStamp,
                  provider: PROVIDER,
                  threadId: ctx.threadId,
                  turnId,
                  toolCall: event.toolCall,
                  rawPayload: event.rawPayload,
                }),
              );
              return;
            case "ThoughtDelta":
            case "ContentDelta":
              yield* PubSub.publish(
                runtimeEvents,
                makeAcpContentDeltaEvent({
                  stamp: eventStamp,
                  provider: PROVIDER,
                  threadId: ctx.threadId,
                  turnId,
                  ...(event._tag === "ContentDelta" && event.itemId
                    ? { itemId: event.itemId }
                    : {}),
                  ...(event._tag === "ThoughtDelta"
                    ? { streamKind: "reasoning_text" as const }
                    : {}),
                  text: event.text,
                  rawPayload: event.rawPayload,
                }),
              );
              return;
            default:
              return;
          }
        }),
      ),
    ).pipe(Effect.ignore, Effect.forkIn(ctx.scope));

  const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
    Effect.gen(function* () {
      if (input.provider !== undefined && input.provider !== PROVIDER) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
        });
      }
      if (!input.cwd?.trim()) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: "cwd is required and must be non-empty.",
        });
      }
      const cwd = input.cwd.trim();
      const existing = sessions.get(input.threadId);
      if (existing && !existing.stopped) {
        yield* stopSessionInternal(existing);
      }

      const sessionScope = yield* Scope.make("sequential");
      let transferred = false;
      yield* Effect.addFinalizer(() =>
        transferred ? Effect.void : Scope.close(sessionScope, Exit.void),
      );

      const resumeSessionId = parseResume(input.resumeCursor)?.sessionId;
      const acp = yield* makeHermesAcpRuntime({
        hermesSettings: settings,
        childProcessSpawner,
        cwd,
        clientInfo: { name: "t3-code", version: "0.0.0" },
        ...(resumeSessionId ? { resumeSessionId } : {}),
        ...(options?.environment ? { environment: options.environment } : {}),
      }).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.provideService(Scope.Scope, sessionScope),
        Effect.mapError(
          (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: input.threadId,
              detail: cause.message,
              cause,
            }),
        ),
      );

      const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
      yield* acp.handleRequestPermission((params) =>
        Effect.gen(function* () {
          if (input.runtimeMode === "full-access" || input.runtimeMode === "auto") {
            const optionId =
              permissionOptionId(params, "acceptForSession") ??
              permissionOptionId(params, "accept");
            if (optionId) {
              return { outcome: { outcome: "selected" as const, optionId } };
            }
          }
          const requestId = ApprovalRequestId.make(yield* randomId);
          const decision = yield* Deferred.make<ProviderApprovalDecision>();
          pendingApprovals.set(requestId, { decision });
          const turnId = sessions.get(input.threadId)?.activeTurnId;
          const permissionRequest = parsePermissionRequest(params);
          yield* PubSub.publish(
            runtimeEvents,
            makeAcpRequestOpenedEvent({
              stamp: yield* stamp(),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              requestId: RuntimeRequestId.make(requestId),
              permissionRequest,
              detail: permissionRequest.detail ?? "Hermes Agent is asking for approval.",
              args: params,
              source: "acp.jsonrpc",
              method: "session/request_permission",
              rawPayload: params,
            }),
          );
          const resolved = yield* Deferred.await(decision);
          pendingApprovals.delete(requestId);
          yield* PubSub.publish(
            runtimeEvents,
            makeAcpRequestResolvedEvent({
              stamp: yield* stamp(),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              requestId: RuntimeRequestId.make(requestId),
              permissionRequest,
              decision: resolved,
            }),
          );
          const optionId = resolved === "cancel" ? undefined : permissionOptionId(params, resolved);
          return {
            outcome: optionId
              ? { outcome: "selected" as const, optionId }
              : ({ outcome: "cancelled" } as const),
          };
        }).pipe(
          Effect.mapError(
            (cause) =>
              new EffectAcpErrors.AcpTransportError({
                detail: "Failed to answer a Hermes permission request.",
                cause,
              }),
          ),
        ),
      );

      const started = yield* acp
        .start()
        .pipe(
          Effect.mapError((error) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", error),
          ),
        );
      const requestedModel = selectedHermesModel(input.modelSelection);
      if (requestedModel) {
        yield* acp.setSessionModel(requestedModel).pipe(Effect.catchCause(() => Effect.void));
      }

      const now = yield* nowIso;
      const session: ProviderSession = {
        provider: PROVIDER,
        ...(boundInstanceId ? { providerInstanceId: boundInstanceId } : {}),
        status: "ready",
        runtimeMode: input.runtimeMode,
        cwd,
        ...(requestedModel ? { model: requestedModel } : {}),
        threadId: input.threadId,
        resumeCursor: { schemaVersion: RESUME_VERSION, sessionId: started.sessionId },
        createdAt: now,
        updatedAt: now,
      };
      const ctx: HermesSession = {
        threadId: input.threadId,
        acpSessionId: started.sessionId,
        session,
        scope: sessionScope,
        acp,
        activeTurnId: undefined,
        stopped: false,
        turns: [],
        pendingApprovals,
      };
      sessions.set(input.threadId, ctx);
      yield* consumeEvents(ctx);
      transferred = true;
      yield* PubSub.publish(runtimeEvents, {
        type: "session.started",
        ...(yield* stamp()),
        provider: PROVIDER,
        threadId: input.threadId,
        payload: { resume: started.initializeResult },
      });
      yield* PubSub.publish(runtimeEvents, {
        type: "thread.started",
        ...(yield* stamp()),
        provider: PROVIDER,
        threadId: input.threadId,
        payload: { providerThreadId: started.sessionId },
      });
      return session;
    }).pipe(Effect.scoped);

  const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const ctx = yield* requireSession(input.threadId);
      const turnId = TurnId.make(yield* randomId);
      ctx.activeTurnId = turnId;
      ctx.session = {
        ...ctx.session,
        status: "running",
        activeTurnId: turnId,
        updatedAt: yield* nowIso,
      };
      const model = selectedHermesModel(input.modelSelection);
      if (model) {
        yield* ctx.acp
          .setSessionModel(model)
          .pipe(
            Effect.mapError((error) =>
              mapAcpToAdapterError(PROVIDER, input.threadId, "session/set_model", error),
            ),
          );
      }
      const text = input.input?.trim() ?? "";
      const instructions = buildRuntimeInstructions({
        harness: "Hermes Agent",
        ...(model ? { model } : {}),
      });
      yield* PubSub.publish(runtimeEvents, {
        type: "turn.started",
        ...(yield* stamp()),
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        payload: model ? { model } : {},
      });
      const response = yield* ctx.acp
        .prompt({
          prompt: [
            ...(text ? [{ type: "text" as const, text }] : []),
            { type: "text" as const, text: instructions },
          ],
        })
        .pipe(
          Effect.mapError((error) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", error),
          ),
        );
      yield* ctx.acp.drainEvents;
      const stopReason = response.stopReason;
      const failed = stopReason === "refusal" || stopReason === "max_tokens";
      ctx.turns.push({ id: turnId, items: [] });
      ctx.activeTurnId = undefined;
      const { activeTurnId: _omit, ...ready } = ctx.session;
      ctx.session = { ...ready, status: "ready", updatedAt: yield* nowIso };
      yield* PubSub.publish(runtimeEvents, {
        type: "turn.completed",
        ...(yield* stamp()),
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        payload: failed
          ? { state: "failed", errorMessage: `Hermes Agent stopped: ${stopReason}`, stopReason }
          : {
              state: stopReason === "cancelled" ? "cancelled" : "completed",
              stopReason: stopReason ?? null,
            },
      });
      return { threadId: input.threadId, turnId, resumeCursor: ctx.session.resumeCursor };
    });

  const interruptTurn: ProviderAdapterShape<ProviderAdapterError>["interruptTurn"] = (
    threadId,
    turnId,
  ) =>
    Effect.gen(function* () {
      const ctx = yield* requireSession(threadId);
      const active = turnId ?? ctx.activeTurnId;
      yield* ctx.acp.cancel.pipe(
        Effect.mapError((error) =>
          mapAcpToAdapterError(PROVIDER, threadId, "session/cancel", error),
        ),
      );
      if (active) {
        ctx.activeTurnId = undefined;
        const { activeTurnId: _omit, ...ready } = ctx.session;
        ctx.session = { ...ready, status: "ready", updatedAt: yield* nowIso };
        yield* PubSub.publish(runtimeEvents, {
          type: "turn.completed",
          ...(yield* stamp()),
          provider: PROVIDER,
          threadId,
          turnId: active,
          payload: { state: "cancelled", stopReason: "cancelled" },
        });
      }
    });

  return {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "in-session",
      supportsConversationRollback: false,
    },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest: (threadId, requestId, decision) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/request_permission",
            detail: `No pending Hermes approval for '${requestId}'.`,
          });
        }
        yield* Deferred.succeed(pending.decision, decision);
      }),
    respondToUserInput: (threadId) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "elicitation/create",
          detail: "Hermes Agent does not ask structured questions through this connector yet.",
        });
      }),
    stopSession: (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        yield* stopSessionInternal(ctx);
      }),
    listSessions: () => Effect.succeed([...sessions.values()].map((ctx) => ctx.session)),
    hasSession: (threadId) => Effect.succeed(sessions.has(threadId)),
    readThread: (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.turns };
      }),
    rollbackThread: (threadId) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/rollback",
          detail: "Hermes Agent cannot rewind a conversation from T3 Code.",
        });
      }),
    stopAll: () =>
      Effect.forEach([...sessions.values()], (ctx) => stopSessionInternal(ctx), { discard: true }),
    streamEvents: Stream.fromPubSub(runtimeEvents),
  } satisfies ProviderAdapterShape<ProviderAdapterError>;
});
