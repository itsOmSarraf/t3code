import { type HermesSettings } from "@t3tools/contracts";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

/** Picker entry that keeps whatever model Hermes itself is configured with. */
export const HERMES_DEFAULT_MODEL = "default";

type HermesAcpSettings = Pick<HermesSettings, "binaryPath" | "launchArgs" | "workingDirectory">;

interface HermesAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly hermesSettings: HermesAcpSettings | null | undefined;
  readonly environment?: NodeJS.ProcessEnv;
}

/**
 * `<binaryPath> <launchArgs...> <subcommand...>`. Launch args let the connector
 * reach a Hermes on another machine, e.g. binary `ssh` with args
 * `-T hermes-mac ~/.local/bin/hermes`.
 */
export function hermesCommandArgs(
  settings: HermesAcpSettings | null | undefined,
  subcommand: ReadonlyArray<string>,
): { readonly command: string; readonly args: ReadonlyArray<string> } {
  return {
    command: settings?.binaryPath || "hermes",
    args: [...tokenizeCliArgs(settings?.launchArgs ?? ""), ...subcommand],
  };
}

/**
 * Hermes advertises exactly the auth method of the provider it is configured
 * with (for example `custom` or `openrouter`) and rejects any other id.
 */
export function hermesAuthMethodId(
  initialize: EffectAcpSchema.InitializeResponse,
): string | undefined {
  return initialize.authMethods?.[0]?.id;
}

/**
 * The cwd sent in `session/new`. A remote Hermes cannot see this machine's
 * paths, so `workingDirectory` overrides the thread cwd when set.
 */
export function hermesSessionCwd(
  settings: HermesAcpSettings | null | undefined,
  threadCwd: string,
): string {
  return settings?.workingDirectory?.trim() || threadCwd;
}

export function buildHermesAcpSpawnInput(
  settings: HermesAcpSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): AcpSessionRuntime.AcpSpawnInput {
  return {
    ...hermesCommandArgs(settings, ["acp"]),
    cwd,
    ...(environment ? { env: environment } : {}),
  };
}

export const makeHermesAcpRuntime = (
  input: HermesAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        cwd: hermesSessionCwd(input.hermesSettings, input.cwd),
        spawn: buildHermesAcpSpawnInput(input.hermesSettings, input.cwd, input.environment),
        authMethodId: hermesAuthMethodId,
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
      Effect.provide(acpContext),
    );
  });
