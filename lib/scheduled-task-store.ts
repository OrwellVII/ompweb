import { closeSync, existsSync, mkdirSync, openSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync, writeSync } from "fs";
import { join, resolve } from "path";
import { getAgentDir } from "./omp/paths";
import { emptyScheduledTaskStore, parseScheduledTaskStore, type ScheduledTaskStore } from "./scheduled-tasks";

// Scheduled tasks on disk: ~/.omp/agent/omp-web/scheduled-tasks.json, shared by
// every browser and by every omp-web instance on the agent dir (an installed
// service plus `npm run dev`). The file is replaced atomically (temp file +
// rename) and every read-modify-write runs under a cross-process lock, so two
// instances never lose each other's updates. A store that cannot be read or
// parsed fails loudly and is never overwritten.
//
// Coordination between instances uses owner files created with O_EXCL that
// hold the owner's pid; a file whose owner process is gone is reclaimed, never
// one that is merely old:
// - the store lock (`scheduled-tasks.json.lock`), held for one update;
// - a run lease per task (`scheduled-task-leases/<task>`), held while a run of
//   that task is in flight, so runs of one task never overlap across instances;
// - a claim per schedule slot (`scheduled-task-claims/<task>@<slotMs>`), kept,
//   so a slot fires in exactly one instance.

const LOCK_TIMEOUT_MS = 3_000;
const LOCK_RETRY_MS = 20;
/** An owner file still empty after this long lost its writer mid-create. */
const UNWRITTEN_OWNER_STALE_MS = 10_000;

function ompWebDir(): string {
  return resolve(getAgentDir(), "omp-web");
}

function storePath(): string {
  return join(ompWebDir(), "scheduled-tasks.json");
}

function claimsDir(): string {
  return join(ompWebDir(), "scheduled-task-claims");
}

function leasePath(taskId: string): string {
  return join(ompWebDir(), "scheduled-task-leases", encodeURIComponent(taskId));
}

export function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM: the process exists but belongs to another user.
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/** Pid recorded in an owner file; null while unwritten, undefined when gone. */
function readOwner(path: string): number | null | undefined {
  try {
    const text = readFileSync(path, "utf8").trim();
    const pid = Number(text);
    return text && Number.isInteger(pid) && pid > 0 ? pid : null;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
}

/**
 * Create `path` exclusively with this process's pid. An existing file whose
 * owner is dead (or, with `reclaimOwn`, is this process) is taken over.
 * Returns false while a live owner holds it.
 */
function tryOwn(path: string, reclaimOwn = false): boolean {
  mkdirSync(resolve(path, ".."), { recursive: true });
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      const fd = openSync(path, "wx");
      try {
        writeSync(fd, String(process.pid));
      } finally {
        closeSync(fd);
      }
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    const owner = readOwner(path);
    if (owner === undefined) continue; // released in between
    if (owner === process.pid && reclaimOwn) return true;
    if (owner !== null && owner !== process.pid && isProcessAlive(owner)) return false;
    if (owner === null) {
      // Created but not written yet: give the writer time unless it is long gone.
      try {
        if (Date.now() - statSync(path).mtimeMs < UNWRITTEN_OWNER_STALE_MS) return false;
      } catch {
        continue;
      }
    }
    if (owner === process.pid) return false; // held by this process
    rmSync(path, { force: true }); // dead owner
  }
  return false;
}

/** Remove `path` if this process owns it. */
function release(path: string): void {
  try {
    if (readOwner(path) === process.pid) rmSync(path, { force: true });
  } catch {
    // Already gone, or unreadable: nothing of ours to remove.
  }
}

/** Current store. Throws when the file exists but cannot be read or parsed. */
export function loadScheduledTaskStore(): ScheduledTaskStore {
  const path = storePath();
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return emptyScheduledTaskStore();
    throw error;
  }
  try {
    return parseScheduledTaskStore(text);
  } catch (error) {
    // Not a SyntaxError: routes map those to "invalid request body".
    throw new Error(`Cannot parse ${path}; fix or remove it: ${error instanceof Error ? error.message : String(error)}`);
  }
}

function saveScheduledTaskStore(store: ScheduledTaskStore): void {
  const path = storePath();
  mkdirSync(ompWebDir(), { recursive: true });
  const temp = `${path}.tmp-${process.pid}-${Date.now()}`;
  try {
    writeFileSync(temp, `${JSON.stringify(store, null, 2)}\n`, "utf8");
    renameSync(temp, path);
  } finally {
    try {
      if (existsSync(temp)) rmSync(temp);
    } catch {
      // ignore cleanup failures
    }
  }
}

/**
 * Apply `update` to the current store and persist the result, under the
 * cross-process store lock (re-read inside it). `update` may mutate the store
 * it receives; returning `false` skips the write.
 */
export function updateScheduledTaskStore<T>(update: (store: ScheduledTaskStore) => T | false): T | false {
  const lockPath = `${storePath()}.lock`;
  const deadline = Date.now() + LOCK_TIMEOUT_MS;
  while (!tryOwn(lockPath)) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${lockPath} (another omp-web process holds it)`);
    sleepSync(LOCK_RETRY_MS);
  }
  try {
    const store = loadScheduledTaskStore();
    const result = update(store);
    if (result !== false) saveScheduledTaskStore(store);
    return result;
  } finally {
    release(lockPath);
  }
}

/** Take the task's run lease; false while another live process runs it. A
 *  lease left by this process (its run record was lost) is reclaimed. */
export function acquireScheduledRunLease(taskId: string): boolean {
  return tryOwn(leasePath(taskId), true);
}

export function releaseScheduledRunLease(taskId: string): void {
  release(leasePath(taskId));
}

/** True when this process is the first to claim the slot. */
export function claimScheduledSlot(taskId: string, slot: Date): boolean {
  mkdirSync(claimsDir(), { recursive: true });
  const name = `${encodeURIComponent(taskId)}@${slot.getTime()}`;
  try {
    closeSync(openSync(join(claimsDir(), name), "wx"));
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
    throw error;
  }
}

/** Remove claim files older than `maxAgeMs`; best effort. */
export function pruneScheduledSlotClaims(now: Date, maxAgeMs = 30 * 24 * 60 * 60_000): void {
  try {
    for (const name of readdirSync(claimsDir())) {
      const path = join(claimsDir(), name);
      if (now.getTime() - statSync(path).mtimeMs > maxAgeMs) rmSync(path, { force: true });
    }
  } catch {
    // A missing directory or a racing prune by another instance is harmless.
  }
}
