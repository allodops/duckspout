// Shell configuration for all scripts/ — the one place where cwd, env, and
// error policy live (SEED s§5.2). Import `$` from here, never raw `Bun.$`.
//
// Convention (enforced by review, stated here once): `$` invokes external
// tools only, ONE command per invocation — no in-shell pipes, so there are no
// pipefail semantics to get wrong. File and text work is plain JS (Bun.file,
// Bun.Glob).

import { $ } from "bun";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/** Repository root, resolved from this file's location (scripts/lib/ -> root). */
export const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");

// Policy: every nonzero exit throws (callers opt out per-invocation with
// `.nothrow()` when an exit code is data, not an error); default cwd is the
// repo root so scripts behave identically from any directory.
$.throws(true);
$.cwd(repoRoot);

export { $ };

/**
 * Spawn an external command with inherited stdio (live streaming), cwd = repo
 * root. Returns the exit code; never throws on nonzero — callers decide.
 *
 * @param {string[]} cmd argv vector (no shell involved)
 * @param {object} [opts] extra Bun.spawn options (env, cwd override, ...)
 * @returns {Promise<number>} exit code
 */
export async function run(cmd, opts = {}) {
  const proc = Bun.spawn(cmd, {
    cwd: repoRoot,
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
    ...opts,
  });
  return await proc.exited;
}
