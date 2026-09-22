import { type RuntimeMode, type SarvamSettings } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

const SARVAM_AUTH_METHOD_CACHED_TOKEN = "cached_token";

type SarvamAcpSettings = Pick<SarvamSettings, "binaryPath" | "model">;

interface SarvamAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly sarvamSettings: SarvamAcpSettings | null | undefined;
  readonly environment?: NodeJS.ProcessEnv;
}

/**
 * Same argv the VS Code extension uses: `sarvam-code acp`, plus an optional
 * `-c model="..."` override from settings.
 */
export function sarvamAcpSpawnArgs(model: string | undefined): ReadonlyArray<string> {
  const args = ["acp"];
  const trimmed = model?.trim();
  if (trimmed) {
    args.push("-c", `model="${trimmed}"`);
  }
  return args;
}

/** ACP mode ids advertised by `session/new` on sarvam-code. */
export function sarvamPermissionMode(runtimeMode: RuntimeMode | undefined): string {
  switch (runtimeMode) {
    case "full-access":
    case "auto":
    case "auto-accept-edits":
      return "auto";
    case "approval-required":
    case undefined:
      return "on-request";
  }
}

export function buildSarvamAcpSpawnInput(
  settings: SarvamAcpSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): AcpSessionRuntime.AcpSpawnInput {
  return {
    command: settings?.binaryPath || "sarvam-code",
    args: [...sarvamAcpSpawnArgs(settings?.model)],
    cwd,
    ...(environment ? { env: environment } : {}),
  };
}

export const makeSarvamAcpRuntime = (
  input: SarvamAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        spawn: buildSarvamAcpSpawnInput(input.sarvamSettings, input.cwd, input.environment),
        authMethodId: SARVAM_AUTH_METHOD_CACHED_TOKEN,
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
