import { describe, expect, it } from "vite-plus/test";

import {
  buildHermesAcpSpawnInput,
  hermesAuthMethodId,
  hermesCommandArgs,
  hermesSessionCwd,
} from "./HermesAcpSupport.ts";

const local = { binaryPath: "hermes", launchArgs: "", workingDirectory: "" };
const remote = {
  binaryPath: "ssh",
  launchArgs: "-T hermes-mac ~/.local/bin/hermes",
  workingDirectory: "/Users/remote",
};

describe("Hermes ACP spawn", () => {
  it("runs hermes acp locally, or through launch args on another machine", () => {
    expect(buildHermesAcpSpawnInput(local, "/tmp/repo")).toMatchObject({
      command: "hermes",
      args: ["acp"],
      cwd: "/tmp/repo",
    });
    expect(buildHermesAcpSpawnInput(remote, "/tmp/repo")).toMatchObject({
      command: "ssh",
      args: ["-T", "hermes-mac", "~/.local/bin/hermes", "acp"],
      cwd: "/tmp/repo",
    });
    expect(hermesCommandArgs(remote, ["--version"]).args.at(-1)).toBe("--version");
  });

  it("sends the remote working directory to Hermes but spawns in the project folder", () => {
    expect(hermesSessionCwd(local, "/tmp/repo")).toBe("/tmp/repo");
    expect(hermesSessionCwd(remote, "/tmp/repo")).toBe("/Users/remote");
  });

  it("authenticates with the provider Hermes advertises, or skips when it advertises none", () => {
    expect(
      hermesAuthMethodId({
        protocolVersion: 1,
        authMethods: [{ id: "custom", name: "custom runtime credentials" }],
      }),
    ).toBe("custom");
    expect(hermesAuthMethodId({ protocolVersion: 1 })).toBeUndefined();
  });
});
