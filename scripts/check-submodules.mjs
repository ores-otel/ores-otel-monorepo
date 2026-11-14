import {readFileSync} from "node:fs";
import {fileURLToPath} from "node:url";

const HEADER = /^\[submodule "([a-z0-9][a-z0-9._/-]*)"\]$/;
const ASSIGN = /^\s+(path|url) = (\S+)$/;

/**
 * App-of-apps source boundary: no arbitrary Git URL, unreviewed nested paths,
 * credentials, duplicate entries, or collector-plane source in the deployment manifest.
 */
export function audit_submodules(source) {
  if (typeof source !== "string" || source.length > 65536) {
    throw new Error("invalid_gitmodules");
  }
  const paths = new Set();
  const entries = [];
  let current;
  for (const line of source.split(/\r?\n/)) {
    if (!line.trim() || line.trim().startsWith("#")) continue;
    const heading = HEADER.exec(line);
    if (heading) {
      if (current) entries.push(current);
      current = {name: heading[1]};
      continue;
    }
    const field = ASSIGN.exec(line);
    if (!current || !field || Object.hasOwn(current, field[1])) {
      throw new Error("invalid_gitmodules");
    }
    current[field[1]] = field[2];
  }
  if (current) entries.push(current);
  if (!entries.length) throw new Error("empty_gitmodules");
  for (const entry of entries) {
    const name = entry.name;
    if (!/^apps\/[a-z0-9][a-z0-9._-]*$/.test(name) ||
        name !== entry.path || paths.has(entry.path) ||
        name.includes("collector") ||
        entry.url !== `https://github.com/ores-otel/${name.slice(5)}.git`) {
      throw new Error("invalid_submodule_authority");
    }
    paths.add(entry.path);
  }
  return Object.freeze({valid: true, count: entries.length});
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    const file = process.argv[2] ?? ".gitmodules";
    const result = audit_submodules(readFileSync(file, "utf8"));
    process.stdout.write(`validated ${result.count} app submodules\n`);
  } catch {
    process.stderr.write("submodule admission failed\n");
    process.exitCode = 1;
  }
}
