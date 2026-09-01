// CI-aware logging (SEED s§5.2): emit GitHub Actions workflow commands when
// running under GHA, plain prefixed lines locally. No other module prints
// annotations directly.

const inCI = !!process.env.GITHUB_ACTIONS;

/**
 * Log group. With a function, runs it inside the group and always closes the
 * group (even on throw). Without one, opens the group; pair with endGroup().
 *
 * @template T
 * @param {string} name
 * @param {(() => T | Promise<T>)=} fn
 * @returns {Promise<T> | undefined}
 */
export function group(name, fn) {
  console.log(inCI ? `::group::${name}` : `--- ${name} ---`);
  if (fn === undefined) return undefined;
  return (async () => {
    try {
      return await fn();
    } finally {
      endGroup();
    }
  })();
}

/** Close a group opened by group(name) without a function. */
export function endGroup() {
  if (inCI) console.log("::endgroup::");
}

/** Error annotation in CI, `ERROR:` line locally. Always goes to stderr locally. */
export function error(msg) {
  if (inCI) console.log(`::error::${msg}`);
  else console.error(`ERROR: ${msg}`);
}

/** Notice annotation in CI, `NOTICE:` line locally. */
export function notice(msg) {
  console.log(inCI ? `::notice::${msg}` : `NOTICE: ${msg}`);
}

/** Plain informational line (no annotation level in CI). */
export function info(msg) {
  console.log(msg);
}
