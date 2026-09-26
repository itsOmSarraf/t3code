import { TextGenerationError, type HermesSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type * as TextGeneration from "./TextGeneration.ts";

const unsupported = (operation: string) =>
  new TextGenerationError({
    operation,
    detail: "Hermes Agent does not generate commit messages or titles from this connector yet.",
  });

export const makeHermesTextGeneration = (
  _settings: HermesSettings,
  _environment: NodeJS.ProcessEnv,
): Effect.Effect<TextGeneration.TextGeneration["Service"]> =>
  Effect.succeed({
    generateCommitMessage: () => Effect.fail(unsupported("generateCommitMessage")),
    generatePrContent: () => Effect.fail(unsupported("generatePrContent")),
    generateBranchName: () => Effect.fail(unsupported("generateBranchName")),
    generateThreadTitle: () => Effect.fail(unsupported("generateThreadTitle")),
  });
