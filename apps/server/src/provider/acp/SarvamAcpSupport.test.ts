import { describe, expect, it } from "vite-plus/test";

import {
  buildSarvamAcpSpawnInput,
  sarvamAcpSpawnArgs,
  sarvamPermissionMode,
} from "./SarvamAcpSupport.ts";

describe("Sarvam ACP spawn", () => {
  it("launches the same sarvam-code acp process as the editor extension", () => {
    expect(sarvamAcpSpawnArgs(undefined)).toEqual(["acp"]);
    expect(sarvamAcpSpawnArgs("  glm-5.3  ")).toEqual(["acp", "-c", 'model="glm-5.3"']);
    expect(
      buildSarvamAcpSpawnInput({ binaryPath: "sarvam-code", model: "" }, "/tmp/repo"),
    ).toMatchObject({
      command: "sarvam-code",
      args: ["acp"],
      cwd: "/tmp/repo",
    });
  });

  it("maps T3 permission modes onto Sarvam ACP modes", () => {
    expect(sarvamPermissionMode("approval-required")).toBe("on-request");
    expect(sarvamPermissionMode(undefined)).toBe("on-request");
    expect(sarvamPermissionMode("auto")).toBe("auto");
    expect(sarvamPermissionMode("full-access")).toBe("auto");
  });
});
