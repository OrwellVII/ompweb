import { closeSync, existsSync, mkdirSync, openSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "fs";
import { join, resolve } from "path";
import { getAgentDir } from "./omp/paths";
import { emptyScheduledTaskStore, parseScheduledTaskStore, type ScheduledTaskStore } from "./scheduled-tasks";

// Scheduled tasks on disk: ~/.omp/agent/omp-web/scheduled-tasks.json, shared by
// every browser and by every omp-web instance on the agent dir. Read-modify-
// write is one synchronous call, so a single process never interleaves two
// updates; the file is replaced atomically (temp file + rename).
//
// Several instances (an installed service plus `npm run dev`) may run the
// scheduler against one agent dir. A schedule slot is fired only by the
// instance that creates its claim file first (O_EXCL), so a slot never runs
// twice; the losers just advance their watermark.

function ompWebDir(): string {
  return resolve(getAgentDir(), "omp-web");
}

function storePath(): string {
  return join(ompWebDir(), "scheduled-tasks.json");
}

function claimsDir(): string {
  return join(ompWebDir(), "scheduled-task-claims");
}

export function loadScheduledTaskStore(): ScheduledTaskStore {
  const path = storePath();
  if (!existsSync(path)) return emptyScheduledTaskStore();
  try {
    return parseScheduledTaskStore(readFileSync(path, "utf8"));
  } catch {
    return emptyScheduledTaskStore();
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

/** Apply `update` to the current store and persist the result. `update` may
 *  mutate the store it receives; returning `false` skips the write. */
export function updateScheduledTaskStore<T>(update: (store: ScheduledTaskStore) => T | false): T | false {
  const store = loadScheduledTaskStore();
  const result = update(store);
  if (result !== false) saveScheduledTaskStore(store);
  return result;
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
