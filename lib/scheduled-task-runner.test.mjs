import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
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
const { createScheduledTaskRunner, ScheduledTaskRunningError } = await jiti.import("./scheduled-task-runner.ts");
const { emptyScheduledTaskStore } = await jiti.import("./scheduled-tasks.ts");
const { claimScheduledSlot, loadScheduledTaskStore } = await jiti.import("./scheduled-task-store.ts");
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

function harness({ start = local(2026, 10, 22, 12), claim = () => true, alive = () => false } = {}) {
  let store = emptyScheduledTaskStore();
  const h = {
    now: start,
    sessions: [],
    opened: [],
    createError: null,
    /** Error the next spawned session throws when it receives the prompt. */
    promptError: null,
    claims: [],
  };
  h.runner = createScheduledTaskRunner({
    now: () => h.now,
    load: () => structuredClone(store),
    update(fn) {
      const draft = structuredClone(store);
      const result = fn(draft);
      if (result !== false) store = draft;
      return result;
    },
    claimSlot: (taskId, slot) => { h.claims.push([taskId, slot.toISOString()]); return claim(taskId, slot); },
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
    log() {},
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

test("slot claims are exclusive on disk", () => {
  const slot = local(2026, 10, 23, 15, 30);
  assert.equal(claimScheduledSlot("st_disk", slot), true);
  assert.equal(claimScheduledSlot("st_disk", slot), false);
  assert.equal(claimScheduledSlot("st_disk", local(2026, 10, 30, 15, 30)), true);
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
