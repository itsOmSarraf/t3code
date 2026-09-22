import {
  type CustomModelSetting,
  type ModelCapabilities,
  type SarvamSettings,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PRESENTATION = {
  displayName: "Sarvam Code",
  supportsConversationRollback: false,
  badgeLabel: "Early Access",
  showInteractionModeToggle: true,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "glm-5.3",
    name: "glm-5.3",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "dsv4.1-flash",
    name: "dsv4.1-flash",
    isCustom: false,
    isDefault: true,
    capabilities: EMPTY_CAPABILITIES,
  },
];

function modelsFromSettings(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(BUILT_IN_MODELS, customModels ?? [], EMPTY_CAPABILITIES);
}

export function buildInitialSarvamProviderSnapshot(
  settings: SarvamSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = modelsFromSettings(settings.customModels);
    if (!settings.enabled) {
      return buildServerProvider({
        presentation: PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Sarvam Code is disabled in T3 Code settings.",
        },
      });
    }
    return buildServerProvider({
      presentation: PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Sarvam Code...",
      },
    });
  });
}

export const checkSarvamProviderStatus = Effect.fn("checkSarvamProviderStatus")(function* (
  settings: SarvamSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const models = modelsFromSettings(settings.customModels);
  if (!settings.enabled) {
    return buildServerProvider({
      presentation: PRESENTATION,
      enabled: false,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Sarvam Code is disabled in T3 Code settings.",
      },
    });
  }

  const command = settings.binaryPath || "sarvam-code";
  const versionResult = yield* Effect.gen(function* () {
    const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  }).pipe(Effect.result);

  if (versionResult._tag === "Failure") {
    const missing = isCommandMissingCause(versionResult.failure);
    return buildServerProvider({
      presentation: PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: !missing,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: missing
          ? "sarvam-code was not found. Install the CLI or set its binary path."
          : "Could not run sarvam-code --version.",
      },
    });
  }

  const version = parseGenericCliVersion(versionResult.success.stdout);
  return buildServerProvider({
    presentation: PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    probe: {
      installed: true,
      version,
      status: "ready",
      auth: {
        status: "authenticated",
        type: "cached_token",
        label: "Sarvam keyring",
      },
    },
  });
});
