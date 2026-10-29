import assert from "node:assert/strict";
import fs, { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createJiti } from "jiti";
import { fileURLToPath } from "node:url";

process.env.TZ = "America/New_York";
const agentDir = mkdtempSync(join(tmpdir(), "omp-web-scheduled-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
test.after(() => rmSync(agentDir, { recursive: true, force: true }));

const jiti = createJiti(import.meta.url, { alias: { "@/": fileURLToPath(new URL("../", import.meta.url)) } });
const { createScheduledTaskRunner, ScheduledTaskRunningError, startScheduledTaskScheduler } = await jiti.import("./scheduled-task-runner.ts");
const { createScheduledTask, emptyScheduledTaskStore } = await jiti.import("./scheduled-tasks.ts");
const {
  acquireScheduledRunLease,
  claimScheduledSlot,
  loadScheduledTaskStore,
  releaseScheduledRunLease,
  releaseScheduledSlot,
  tryOwnFile,
  updateScheduledTaskStore,
} = await jiti.import("./scheduled-task-store.ts");
const listRoute = await jiti.import("../app/api/scheduled-tasks/route.ts");
const taskRoute = await jiti.import("../app/api/scheduled-tasks/[id]/route.ts");

const local = (y, mo, d, h = 0, mi = 0, s = 0) => new Date(y, mo - 1, d, h, mi, s);
const input = (overrides = {}) => ({
  name: "Journal",
  cwd: "/work/journal",
  prompt: "Update the journal",
  schedule: { kind: "weekly", days: [5], time: "15:30" },
  sessionMode: "new",
  model: null,
  thinkingLevel: null,
  enabled: true,
  ...overrides,
});

/** A session whose turn ends when the test says so. */
function fakeSession(id) {
  const session = {
    sessionId: id,
    running: false,
    sent: [],
    destroyed: false,
    endTurn: null,
    failTurn: null,
    rejectPrompt: null,
    isRunning: () => session.running,
    async send(command) {
      session.sent.push(command);
      if (command.type === "prompt") {
        if (session.rejectPrompt) throw session.rejectPrompt;
        session.running = true;
      }
      return null;
    },
    waitForTurnEnd() {
      return new Promise((resolve, reject) => {
        session.endTurn = () => { session.running = false; resolve(); };
        session.failTurn = (error) => { session.running = false; reject(error); };
      });
    },
    async destroyAndWait() { session.destroyed = true; },
  };
  return session;
}

function harness({ start = local(2026, 10, 22, 12), claim = () => true, releaseClaim = () => {}, alive = () => false } = {}) {
  let store = emptyScheduledTaskStore();
  const h = {
    now: start,
    sessions: [],
    opened: [],
    createError: null,
    /** Error the next spawned session throws when it receives the prompt. */
    promptError: null,
    claims: [],
    /** Task ids whose run lease another omp-web instance holds. */
    foreignLeases: new Set(),
    leases: new Set(),
    /** When set, every store write throws it (a full or unwritable disk). */
    writeError: null,
    /** Number of upcoming store writes that fail once each. */
    failWrites: 0,
    /** Retry waits, with the leases held at that moment. */
    delays: [],
    releasedSlots: [],
    logs: [],
  };
  h.runner = createScheduledTaskRunner({
    now: () => h.now,
    load: () => structuredClone(store),
    update(fn) {
      if (h.writeError) throw h.writeError;
      if (h.failWrites > 0) {
        h.failWrites -= 1;
        throw new Error("EBUSY: resource busy or locked");
      }
      const draft = structuredClone(store);
      const result = fn(draft);
      if (result !== false) store = draft;
      return result;
    },
    claimSlot: (taskId, slot) => { h.claims.push([taskId, slot.toISOString()]); return claim(taskId, slot); },
    releaseSlot: (taskId, slot) => { h.releasedSlots.push([taskId, slot.toISOString()]); releaseClaim(taskId, slot); },
    acquireRunLease: (taskId) => {
      if (h.foreignLeases.has(taskId)) return false;
      h.leases.add(taskId);
      return true;
    },
    releaseRunLease: (taskId) => { h.leases.delete(taskId); },
    directoryExists: (path) => path !== "/missing",
    isProcessAlive: alive,
    async createSession(cwd) {
      if (h.createError) throw h.createError;
      const session = fakeSession(`s${h.sessions.length + 1}`);
      session.cwd = cwd;
      session.rejectPrompt = h.promptError;
      h.sessions.push(session);
      return session;
    },
    async openSession(id) {
      h.opened.push(id);
      return h.sessions.find((session) => session.sessionId === id) ?? null;
    },
    formatSessionTime: () => "2026-10-23 15:30",
    log: (message) => h.logs.push(message),
    delay: async (ms) => { h.delays.push({ ms, leases: [...h.leases] }); },
  });
  h.store = () => store;
  /** Replace the stored runs, as another process may have left them. */
  h.setRuns = (runs) => { store = { ...store, runs }; };
  h.runsOf = (taskId) => store.runs.filter((run) => run.taskId === taskId);
  return h;
}

test("a due slot starts one session with the task's name, model and prompt, then completes", async () => {
  const h = harness();
  const task = h.runner.create(input({ model: { provider: "anthropic", modelId: "claude" }, thinkingLevel: "high" }));
  h.now = local(2026, 10, 23, 15, 30, 10);
  const [run] = await h.runner.tick();
  assert.equal(run.trigger, "schedule");
  assert.equal(run.status, "running");
  assert.equal(run.sessionId, "s1");
  assert.deepEqual(h.sessions[0].sent.map((command) => command.type), ["set_session_name", "set_model", "set_thinking_level", "prompt"]);
  assert.equal(h.sessions[0].sent[0].name, "Journal · 2026-10-23 15:30");
  assert.equal(h.sessions[0].sent.at(-1).message, "Update the journal");
  assert.equal(h.sessions[0].cwd, "/work/journal");
  assert.equal(h.runner.list(true).tasks[0].running, true);

  // The same slot never fires again.
  h.now = local(2026, 10, 23, 15, 31);
  assert.deepEqual(await h.runner.tick(), []);

  h.sessions[0].endTurn();
  await new Promise((resolve) => setImmediate(resolve));
  const [stored] = h.runsOf(task.id);
  assert.equal(stored.status, "completed");
  assert.equal(stored.pid, null);
  assert.equal(h.runner.isActive(task.id), false);
  assert.deepEqual(h.runner.list(true).tasks[0].nextRunAt, local(2026, 10, 30, 15, 30).toISOString());
});

test("an instance that loses the slot claim advances its watermark without running", async () => {
  const h = harness({ claim: () => false });
  const task = h.runner.create(input());
  h.now = local(2026, 10, 23, 15, 30, 5);
  assert.deepEqual(await h.runner.tick(), []);
  assert.equal(h.sessions.length, 0);
  assert.equal(h.store().tasks[0].lastSlotAt, local(2026, 10, 23, 15, 30).toISOString());
  assert.deepEqual(h.runsOf(task.id), []);
});

test("a slot whose watermark write fails is given back and fires on the next tick", async () => {
  const claims = new Set();
  const key = (taskId, slot) => `${taskId}@${slot.getTime()}`;
  const h = harness({
    claim: (taskId, slot) => !claims.has(key(taskId, slot)) && Boolean(claims.add(key(taskId, slot))),
    releaseClaim: (taskId, slot) => claims.delete(key(taskId, slot)),
  });
  const task = h.runner.create(input());
  h.now = local(2026, 10, 23, 15, 30, 5);
  h.writeError = new Error("Timed out waiting for scheduled-tasks.json.lock");
  await assert.rejects(h.runner.tick(), /Timed out/);
  assert.deepEqual([...claims], []);
  h.writeError = null;
  const [run] = await h.runner.tick();
  assert.equal(run.status, "running");
  assert.equal(run.scheduledFor, local(2026, 10, 23, 15, 30).toISOString());
  assert.equal(h.runsOf(task.id).length, 1);
});

test("a failed watermark write keeps a slot claim this tick did not make", async () => {
  const h = harness({ claim: () => false });
  h.runner.create(input());
  h.now = local(2026, 10, 23, 15, 30, 5);
  h.writeError = new Error("Timed out waiting for scheduled-tasks.json.lock");
  await assert.rejects(h.runner.tick(), /Timed out/);
  assert.deepEqual(h.releasedSlots, []);
});

test("a slot missed while the server was down runs once as catch-up", async () => {
  const h = harness();
  h.runner.create(input());
  h.now = local(2026, 11, 2, 9);
  const runs = await h.runner.tick();
  assert.equal(runs.length, 1);
  assert.equal(runs[0].trigger, "catch-up");
  assert.equal(runs[0].scheduledFor, local(2026, 10, 30, 15, 30).toISOString());
  assert.deepEqual(h.claims, [[runs[0].taskId, local(2026, 10, 30, 15, 30).toISOString()]]);
});

test("a slot that would overlap the previous run is recorded as skipped", async () => {
  const h = harness();
  const task = h.runner.create(input({ schedule: { kind: "interval", everyMinutes: 30 } }));
  h.now = local(2026, 10, 22, 12, 30, 5);
  await h.runner.tick();
  h.now = local(2026, 10, 22, 13, 0, 5);
  const [second] = await h.runner.tick();
  assert.equal(second.status, "skipped");
  assert.match(second.error, /still running/);
  assert.equal(h.sessions.length, 1);
  await assert.rejects(h.runner.runNow(task.id), ScheduledTaskRunningError);
});

test("continue mode reuses the previous run's session and skips while it is busy", async () => {
  const h = harness();
  const task = h.runner.create(input({ sessionMode: "continue" }));
  const first = await h.runner.runNow(task.id);
  assert.equal(first.sessionId, "s1");
  assert.equal(h.store().tasks[0].lastSessionId, "s1");

  // The scheduled turn ended, but the user is now working in that session:
  // the next run is skipped instead of interrupting them or forking a new one.
  h.sessions[0].endTurn();
  await new Promise((resolve) => setImmediate(resolve));
  h.sessions[0].running = true;
  const busy = await h.runner.runNow(task.id);
  assert.equal(busy.status, "skipped");
  assert.equal(busy.sessionId, "s1");

  h.sessions[0].running = false;
  const second = await h.runner.runNow(task.id);
  assert.equal(second.sessionId, "s1");
  assert.equal(h.sessions.length, 1);
  assert.deepEqual(h.opened, ["s1", "s1"]);
  // A continued session is not renamed again.
  assert.equal(h.sessions[0].sent.filter((command) => command.type === "set_session_name").length, 1);
});

test("continue mode starts a fresh session when the previous one is gone", async () => {
  const h = harness();
  const task = h.runner.create(input({ sessionMode: "continue" }));
  await h.runner.runNow(task.id);
  h.sessions[0].endTurn();
  await new Promise((resolve) => setImmediate(resolve));
  h.sessions.length = 0; // deleted outside omp-web
  const next = await h.runner.runNow(task.id);
  assert.equal(next.sessionId, "s1");
  assert.equal(next.status, "running");
});

test("start failures are recorded and never leave an orphaned session", async () => {
  const h = harness();
  const missing = h.runner.create(input({ cwd: "/missing" }));
  const noDir = await h.runner.runNow(missing.id);
  assert.equal(noDir.status, "failed");
  assert.match(noDir.error, /does not exist/);

  const task = h.runner.create(input({ name: "Other" }));
  h.createError = new Error("omp not found");
  assert.equal((await h.runner.runNow(task.id)).error, "omp not found");
  h.createError = null;

  // The prompt fails after a fresh session was spawned: that child is torn down.
  const rejected = h.runner.create(input({ name: "Rejected" }));
  h.promptError = new Error("model unavailable");
  const run = await h.runner.runNow(rejected.id);
  assert.equal(run.status, "failed");
  assert.equal(run.error, "model unavailable");
  assert.equal(h.sessions.at(-1).destroyed, true);
  for (const id of [missing.id, task.id, rejected.id]) assert.equal(h.runner.isActive(id), false);
});

test("a turn that fails marks the run failed with the error", async () => {
  const h = harness();
  const task = h.runner.create(input());
  await h.runner.runNow(task.id);
  h.sessions[0].failTurn(new Error("Target session turn failed."));
  await new Promise((resolve) => setImmediate(resolve));
  const [run] = h.runsOf(task.id);
  assert.equal(run.status, "failed");
  assert.equal(run.error, "Target session turn failed.");
  assert.equal(h.runner.isActive(task.id), false);
});

test("runs owned by a dead process or forgotten by this one become interrupted", () => {
  const h = harness({ alive: (pid) => pid === 4242 });
  const task = h.runner.create(input());
  const base = { taskId: task.id, trigger: "manual", scheduledFor: null, startedAt: "2026-10-22T16:00:00.000Z", finishedAt: null, sessionId: null, status: "running", error: null };
  h.setRuns([{ ...base, id: "dead", pid: 999_999 }, { ...base, id: "mine", pid: process.pid }, { ...base, id: "other-alive", pid: 4242 }]);
  h.runner.reconcileRuns();
  const byId = Object.fromEntries(h.store().runs.map((run) => [run.id, run.status]));
  assert.deepEqual(byId, { dead: "interrupted", mine: "interrupted", "other-alive": "running" });
});

test("a run another instance is running is skipped on schedule and refused on Run now", async () => {
  const h = harness();
  const task = h.runner.create(input({ schedule: { kind: "interval", everyMinutes: 30 } }));
  h.foreignLeases.add(task.id);
  h.now = local(2026, 10, 22, 12, 30, 5);
  const [run] = await h.runner.tick();
  assert.equal(run.status, "skipped");
  await assert.rejects(h.runner.runNow(task.id), ScheduledTaskRunningError);
  assert.equal(h.sessions.length, 0);

  h.foreignLeases.clear();
  const started = await h.runner.runNow(task.id);
  assert.equal(started.status, "running");
  assert.deepEqual([...h.leases], [task.id]);
  h.sessions[0].endTurn();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual([...h.leases], []);
});

test("a failed history write when a run ends is logged and still frees the task", async () => {
  const h = harness();
  const task = h.runner.create(input());
  const { done } = await h.runner.startRun(h.store().tasks[0], "manual", null);
  h.writeError = new Error("ENOSPC: no space left on device");
  h.sessions[0].endTurn();
  const finished = await done; // resolves; an unhandled rejection would exit the server
  assert.equal(finished.status, "completed");
  assert.match(h.logs.at(-1), /could not record the end of run/);
  assert.equal(h.runner.isActive(task.id), false);
  assert.deepEqual([...h.leases], []);
});

test("a failed history write when a run ends is retried while the task's lease is held", async () => {
  const h = harness();
  const task = h.runner.create(input());
  const { done } = await h.runner.startRun(h.store().tasks[0], "manual", null);
  h.failWrites = 2;
  h.sessions[0].endTurn();
  await done;
  assert.equal(h.runsOf(task.id)[0].status, "completed");
  assert.deepEqual(h.delays, [{ ms: 1000, leases: [task.id] }, { ms: 1000, leases: [task.id] }]);
  assert.equal(h.logs.some((message) => /could not record/.test(message)), false);
  assert.deepEqual([...h.leases], []);
});

test("a failed history write at start frees the task instead of wedging it as running", async () => {
  const h = harness();
  const task = h.runner.create(input());
  h.writeError = new Error("EACCES: permission denied");
  await assert.rejects(h.runner.runNow(task.id), /EACCES/);
  assert.equal(h.runner.isActive(task.id), false);
  assert.deepEqual([...h.leases], []);
  h.writeError = null;
  assert.equal((await h.runner.runNow(task.id)).status, "running");
});

test("slot claims are exclusive on disk, and a released claim can be made again", () => {
  const slot = local(2026, 10, 23, 15, 30);
  assert.equal(claimScheduledSlot("st_disk", slot), true);
  assert.equal(claimScheduledSlot("st_disk", slot), false);
  assert.equal(claimScheduledSlot("st_disk", local(2026, 10, 30, 15, 30)), true);
  releaseScheduledSlot("st_disk", slot);
  releaseScheduledSlot("st_disk", slot); // already gone: fine
  assert.equal(claimScheduledSlot("st_disk", slot), true);
});

/** A process that stays alive until the test ends, so an owner file naming
 *  it has a live owner on every platform. */
async function liveProcess(t) {
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
  t.after(() => { child.kill(); });
  await once(child, "spawn");
  return child;
}

const ownerDir = join(agentDir, "owner-files");
mkdirSync(ownerDir, { recursive: true });
const reclaimMarker = (path, content) => `${path}.reclaim-${createHash("sha256").update(content).digest("hex").slice(0, 16)}`;

test("run leases on disk block a live owner and are reclaimed from a dead one", async (t) => {
  const leaseFile = join(agentDir, "omp-web", "scheduled-task-leases", "st_lease");
  mkdirSync(join(agentDir, "omp-web", "scheduled-task-leases"), { recursive: true });
  const other = await liveProcess(t);
  writeFileSync(leaseFile, `${other.pid} other-instance`);
  assert.equal(acquireScheduledRunLease("st_lease"), false);
  releaseScheduledRunLease("st_lease"); // not ours: left alone
  assert.equal(readFileSync(leaseFile, "utf8"), `${other.pid} other-instance`);
  writeFileSync(leaseFile, "999999 exited-instance"); // an owner that has exited
  assert.equal(acquireScheduledRunLease("st_lease"), true);
  assert.match(readFileSync(leaseFile, "utf8"), new RegExp(`^${process.pid} \\S+\\n$`));
  assert.equal(acquireScheduledRunLease("st_lease"), true); // ours already
  releaseScheduledRunLease("st_lease");
  assert.equal(existsSync(leaseFile), false);
  assert.deepEqual(readdirSync(join(agentDir, "omp-web", "scheduled-task-leases")), []);
});

test("two reclaimers of one dead owner never both take the file", () => {
  const path = join(ownerDir, "two-reclaimers");
  writeFileSync(path, "999999 dead-owner");
  let second = null;
  let secondContent = null;
  // The second reclaimer runs entirely after the first read the dead owner
  // and before it reclaimed: it removes the stale file and takes the path.
  const first = tryOwnFile(path, {
    beforeReclaim: () => {
      second = tryOwnFile(path);
      secondContent = readFileSync(path, "utf8");
    },
  });
  assert.equal(second, true);
  assert.equal(first, false);
  assert.equal(readFileSync(path, "utf8"), secondContent);
  rmSync(path);
  assert.deepEqual(readdirSync(ownerDir), []);
});

test("a reclaimer never removes a live owner's file that replaced the dead one", async (t) => {
  const other = await liveProcess(t);
  for (const reclaimOwn of [false, true]) {
    const path = join(ownerDir, `replaced-${reclaimOwn}`);
    writeFileSync(path, "999999 dead-owner");
    // Another instance reclaims the dead owner's file and takes the path
    // between this reclaimer's read and its removal.
    const taken = tryOwnFile(path, { reclaimOwn, beforeReclaim: () => writeFileSync(path, `${other.pid} live-owner`) });
    assert.equal(taken, false);
    assert.equal(readFileSync(path, "utf8"), `${other.pid} live-owner`);
    rmSync(path);
  }
  assert.deepEqual(readdirSync(ownerDir), []);
});

test("a reclaim left unfinished by a dead reclaimer is taken over", () => {
  const path = join(ownerDir, "dead-reclaimer");
  writeFileSync(path, "999999 dead-owner");
  writeFileSync(reclaimMarker(path, "999999 dead-owner"), "999998 dead-reclaimer");
  assert.equal(tryOwnFile(path), true);
  assert.match(readFileSync(path, "utf8"), new RegExp(`^${process.pid} `));
  rmSync(path);
  assert.deepEqual(readdirSync(ownerDir), []);
});

test("an owner file whose write fails is never left behind", async (t) => {
  const path = join(ownerDir, "failed-write");
  const realWriteFileSync = fs.writeFileSync;
  const writeMock = t.mock.method(fs, "writeFileSync", (file, data, options) => {
    if (!String(file).startsWith(`${path}.`)) return realWriteFileSync(file, data, options);
    realWriteFileSync(file, "", options); // created, then the disk fills up
    throw Object.assign(new Error("ENOSPC: no space left on device"), { code: "ENOSPC" });
  });
  syncBuiltinESMExports();
  t.after(() => { writeMock.mock.restore(); syncBuiltinESMExports(); });
  const isolated = createJiti(import.meta.url, { moduleCache: false, tryNative: false });
  const { tryOwnFile: tryOwnWithFullDisk } = await isolated.import("./scheduled-task-store.ts");
  assert.throws(() => tryOwnWithFullDisk(path), /ENOSPC/);
  assert.equal(writeMock.mock.callCount() > 0, true, "positive control: the owner write went through the mock");
  writeMock.mock.restore();
  syncBuiltinESMExports();
  assert.deepEqual(readdirSync(ownerDir), []);
  // Nothing blocks the next owner.
  assert.equal(tryOwnFile(path), true);
  rmSync(path);
});

test("a damaged store file is reported and never overwritten", async () => {
  const storeFile = join(agentDir, "omp-web", "scheduled-tasks.json");
  mkdirSync(join(agentDir, "omp-web"), { recursive: true });
  const before = existsSync(storeFile) ? readFileSync(storeFile, "utf8") : null;
  writeFileSync(storeFile, "{\"version\":1,\"tasks\":[");
  try {
    assert.throws(() => updateScheduledTaskStore(() => true), /Cannot parse/);
    assert.equal(readFileSync(storeFile, "utf8"), "{\"version\":1,\"tasks\":[");
    assert.equal((await listRoute.GET()).status, 500);
  } finally {
    if (before === null) rmSync(storeFile, { force: true });
    else writeFileSync(storeFile, before);
  }
});

test("a write that would drop malformed store entries keeps the previous file first", (t) => {
  const dir = join(agentDir, "omp-web");
  const storeFile = join(dir, "scheduled-tasks.json");
  mkdirSync(dir, { recursive: true });
  const before = existsSync(storeFile) ? readFileSync(storeFile, "utf8") : null;
  const backups = () => readdirSync(dir).filter((name) => name.startsWith("scheduled-tasks.json.dropped-"));
  const warn = t.mock.method(console, "warn", () => {});
  const good = createScheduledTask(input(), "st_good", local(2026, 10, 1));
  const broken = { ...good, id: "st_broken", schedule: { kind: "daily", time: "7:00" } };
  const brokenRun = { id: "r_broken", taskId: "st_broken", trigger: "manual", scheduledFor: null, startedAt: good.createdAt, finishedAt: null, sessionId: null, status: "failed", error: null, pid: null };
  const original = JSON.stringify({ version: 1, tasks: [good, broken], runs: [brokenRun] });
  writeFileSync(storeFile, original);
  try {
    // Reading never writes, and a skipped write keeps everything.
    assert.deepEqual(loadScheduledTaskStore().tasks.map((task) => task.id), ["st_good"]);
    assert.equal(updateScheduledTaskStore(() => false), false);
    assert.deepEqual(backups(), []);
    assert.equal(readFileSync(storeFile, "utf8"), original);

    updateScheduledTaskStore((store) => { store.tasks[0].name = "Renamed"; return true; });
    const [backup] = backups();
    assert.ok(backup, "the file with the dropped entries is kept");
    assert.equal(readFileSync(join(dir, backup), "utf8"), original);
    assert.match(warn.mock.calls.at(-1).arguments[0], /2 malformed entries were dropped/);
    assert.deepEqual(loadScheduledTaskStore().tasks.map((task) => task.name), ["Renamed"]);

    // The rewritten file has nothing left to drop: no second copy.
    updateScheduledTaskStore((store) => { store.tasks[0].name = "Again"; return true; });
    assert.equal(backups().length, 1);
  } finally {
    for (const name of backups()) rmSync(join(dir, name));
    if (before === null) rmSync(storeFile, { force: true });
    else writeFileSync(storeFile, before);
  }
});

test("an enabled scheduler starts even when the store cannot be read at boot", async (t) => {
  const storeFile = join(agentDir, "omp-web", "scheduled-tasks.json");
  mkdirSync(join(agentDir, "omp-web"), { recursive: true });
  const before = existsSync(storeFile) ? readFileSync(storeFile, "utf8") : null;
  const disabled = process.env.OMP_WEB_DISABLE_SCHEDULER;
  delete process.env.OMP_WEB_DISABLE_SCHEDULER;
  const warn = t.mock.method(console, "warn", () => {});
  writeFileSync(storeFile, "{\"version\":1,\"tasks\":[");
  try {
    await startScheduledTaskScheduler();
    // The list reports this instance's scheduler as on; it must be.
    assert.ok(globalThis.__ompScheduledTaskTimer);
    assert.match(warn.mock.calls.map((call) => call.arguments[0]).join("\n"), /Cannot parse/);
  } finally {
    clearInterval(globalThis.__ompScheduledTaskTimer);
    delete globalThis.__ompScheduledTaskTimer;
    delete globalThis.__ompScheduledTaskRunner;
    if (disabled === undefined) delete process.env.OMP_WEB_DISABLE_SCHEDULER;
    else process.env.OMP_WEB_DISABLE_SCHEDULER = disabled;
    if (before === null) rmSync(storeFile, { force: true });
    else writeFileSync(storeFile, before);
  }
});

test("routes validate input, persist tasks and map missing ids to 404", async () => {
  const json = (body) => new Request("http://localhost/api/scheduled-tasks", { method: "POST", body: JSON.stringify(body), headers: { "content-type": "application/json" } });
  const bad = await listRoute.POST(json({ ...input({ cwd: agentDir }), schedule: { kind: "daily", time: "25:00" } }));
  assert.equal(bad.status, 400);
  assert.deepEqual(Object.keys((await bad.json()).fields), ["schedule"]);

  const noDir = await listRoute.POST(json(input({ cwd: join(agentDir, "nope") })));
  assert.equal(noDir.status, 400);
  assert.equal((await noDir.json()).code, "directory_not_found");

  const created = await listRoute.POST(json(input({ cwd: agentDir })));
  assert.equal(created.status, 201);
  const { task } = await created.json();
  assert.equal(loadScheduledTaskStore().tasks[0].id, task.id);

  const listed = await (await listRoute.GET()).json();
  assert.equal(listed.tasks.length, 1);
  assert.equal(typeof listed.tasks[0].nextRunAt, "string");
  assert.equal(typeof listed.timeZone, "string");

  const params = { params: Promise.resolve({ id: task.id }) };
  const patch = await taskRoute.PATCH(new Request("http://localhost", { method: "PATCH", body: JSON.stringify({ enabled: false }) }), params);
  assert.equal((await patch.json()).task.enabled, false);
  assert.equal((await (await listRoute.GET()).json()).tasks[0].nextRunAt, null);

  assert.equal((await taskRoute.DELETE(new Request("http://localhost", { method: "DELETE" }), params)).status, 200);
  const gone = await taskRoute.PATCH(new Request("http://localhost", { method: "PATCH", body: JSON.stringify({ enabled: true }) }), params);
  assert.equal(gone.status, 404);
  assert.equal((await gone.json()).code, "scheduled_task_not_found");
});
