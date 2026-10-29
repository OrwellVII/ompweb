import { createHash, randomUUID } from "crypto";
import { closeSync, existsSync, linkSync, mkdirSync, openSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "fs";
import { dirname, join, resolve } from "path";
import { getAgentDir } from "./omp/paths";
import { emptyScheduledTaskStore, parseScheduledTaskStore, type ScheduledTaskStore } from "./scheduled-tasks";

// Scheduled tasks on disk: ~/.omp/agent/omp-web/scheduled-tasks.json, shared by
// every browser and by every omp-web instance on the agent dir (an installed
// service plus `npm run dev`). The file is replaced atomically (temp file +
// rename) and every read-modify-write runs under a cross-process lock, so two
// instances never lose each other's updates. A store that cannot be read or
// parsed fails loudly and is never overwritten.
//
// Coordination between instances uses owner files that hold the owner's pid
// and a token unique to that acquisition; a file whose owner process is gone
// is reclaimed, never one that is merely old:
// - the store lock (`scheduled-tasks.json.lock`), held for one update;
// - a run lease per task (`scheduled-task-leases/<task>`), held while a run of
//   that task is in flight, so runs of one task never overlap across instances;
// - a claim per schedule slot (`scheduled-task-claims/<task>@<slotMs>`, an
//   empty O_EXCL file), kept, so a slot fires in exactly one instance.

const LOCK_TIMEOUT_MS = 3_000;
const LOCK_RETRY_MS = 20;

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

interface OwnerFile {
  /** null when the content is not an owner token (treated as a dead owner). */
  pid: number | null;
  /** The whole token: unique per acquisition, so it identifies this file. */
  content: string;
}

/** The owner file at `path`, or undefined when there is none. */
function readOwner(path: string): OwnerFile | undefined {
  let content: string;
  try {
    content = readFileSync(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
  const pid = Number(content.trim().split(/\s+/, 1)[0]);
  return { pid: Number.isInteger(pid) && pid > 0 ? pid : null, content };
}

/**
 * Create `path` holding "<pid> <token>" in one step: the content goes to a
 * private temp file that is hard-linked into place (link never replaces an
 * existing file). The owner file therefore never exists empty or half
 * written, and a failed write leaves nothing at `path`. False when `path`
 * already exists.
 */
function createOwnerFile(path: string): boolean {
  const temp = `${path}.${process.pid}-${randomUUID()}.tmp`;
  try {
    writeFileSync(temp, `${process.pid} ${randomUUID()}\n`, { flag: "wx" });
    try {
      linkSync(temp, path);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
      throw error;
    }
  } finally {
    try {
      rmSync(temp, { force: true });
    } catch {
      // A leftover private temp file blocks nothing.
    }
  }
}

export interface OwnFileOptions {
  /** A file this process already owns counts as acquired (a run lease whose
   *  run record was lost). */
  reclaimOwn?: boolean;
  /** Test seam: runs after a dead owner was read, before it is reclaimed. */
  beforeReclaim?: () => void;
}

/**
 * Create `path` as this process's owner file. An existing file whose owner
 * is dead is taken over. Returns false while a live owner holds it, or
 * another live process is reclaiming it.
 */
export function tryOwnFile(path: string, options: OwnFileOptions = {}): boolean {
  mkdirSync(dirname(path), { recursive: true });
  for (let attempt = 0; attempt < 3; attempt += 1) {
    if (createOwnerFile(path)) return true;
    const owner = readOwner(path);
    if (owner === undefined) continue; // released in between
    if (owner.pid === process.pid) return options.reclaimOwn === true;
    if (owner.pid !== null && isProcessAlive(owner.pid)) return false;
    options.beforeReclaim?.();
    if (!removeDeadOwner(path, owner.content)) return false;
  }
  return false;
}

/**
 * Remove `path` only if it still holds `content`, a dead owner's token.
 *
 * Reading the token and removing the file cannot be one atomic step, so the
 * removal of each token is serialized instead: only the process that owns
 * `<path>.reclaim-<token hash>` may remove the file holding that token. A
 * token is unique to one acquisition and its owner is dead, so while the
 * marker is held nothing else removes or replaces that file; a reclaimer that
 * finds a different token (a newer owner took over) leaves it alone. The
 * marker is itself an owner file, so one left by a reclaimer that died is
 * reclaimed the same way. Returns false while another live process holds it.
 */
function removeDeadOwner(path: string, content: string): boolean {
  const marker = `${path}.reclaim-${createHash("sha256").update(content).digest("hex").slice(0, 16)}`;
  if (!tryOwnFile(marker)) return false;
  try {
    if (readOwner(path)?.content === content) rmSync(path, { force: true });
  } finally {
    release(marker);
  }
  return true;
}

/** Remove `path` if this process owns it. */
function release(path: string): void {
  try {
    if (readOwner(path)?.pid === process.pid) rmSync(path, { force: true });
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
  while (!tryOwnFile(lockPath)) {
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
  return tryOwnFile(leasePath(taskId), { reclaimOwn: true });
}

export function releaseScheduledRunLease(taskId: string): void {
  release(leasePath(taskId));
}

function slotClaimPath(taskId: string, slot: Date): string {
  return join(claimsDir(), `${encodeURIComponent(taskId)}@${slot.getTime()}`);
}

/** True when this process is the first to claim the slot. */
export function claimScheduledSlot(taskId: string, slot: Date): boolean {
  mkdirSync(claimsDir(), { recursive: true });
  try {
    closeSync(openSync(slotClaimPath(taskId, slot), "wx"));
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
    throw error;
  }
}

/** Give back a slot claim (only one this process made); a missing claim is fine. */
export function releaseScheduledSlot(taskId: string, slot: Date): void {
  rmSync(slotClaimPath(taskId, slot), { force: true });
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
