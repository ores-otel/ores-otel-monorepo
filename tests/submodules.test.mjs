import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import {audit_submodules} from "../scripts/check-submodules.mjs";

const manifest = readFileSync(".gitmodules", "utf8");

test("current app manifest contains only explicit org-owned sources", () => {
  const result = audit_submodules(manifest);
  assert.ok(result.valid && result.count >= 1);
});

test("rejects credentialed, external and ambiguous submodules", () => {
  const cases = [
    manifest.replace("https://github.com/ores-otel/", "https://token@github.com/ores-otel/"),
    manifest.replace("https://github.com/ores-otel/", "https://github.com/untrusted-org/"),
    manifest.replace("path = apps/ores-otel-cli", "path = apps/../ores-otel-cli"),
    manifest + "\n[submodule \"apps/ores-otel-cli\"]\n  path = apps/ores-otel-cli\n  url = https://github.com/ores-otel/ores-otel-cli.git\n",
    manifest.replace("apps/ores-otel-cli", "apps/ores-otel-collector.rs"),
  ];
  for (const candidate of cases) assert.throws(() => audit_submodules(candidate));
});

test("rejects unknown git options and unexpected nesting", () => {
  assert.throws(() => audit_submodules(manifest + "\n  branch = main\n"));
  assert.throws(() => audit_submodules("[submodule \"apps/nested/x\"]\n  path = apps/nested/x\n  url = https://github.com/ores-otel/nested/x.git"));
});
