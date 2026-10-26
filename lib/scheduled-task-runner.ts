import { randomUUID } from "crypto";
import { existsSync } from "fs";
import { allowFileRoot } from "./file-access";
import { getRpcSession, resolveSpawnCwdResult, startRpcSession } from "./rpc-manager";
import { claimScheduledSlot, loadScheduledTaskStore, pruneScheduledSlotClaims, updateScheduledTaskStore } from "./scheduled-task-store";
import { invalidateSessionListCache, readSessionHeader, resolveSessionPath } from "./session-reader";
import {
  applyScheduledTaskPatch,
  createScheduledTask,
  dueScheduleSlot,
  nextScheduledRun,
  SCHEDULED_TASK_LIMITS,
  withScheduledTaskRun,
  type ScheduledTask,
  type ScheduledTaskInput,
  type ScheduledTaskListResponse,
  type ScheduledTaskRun,
  type ScheduledTaskRunTrigger,
  type ScheduledTaskStore,
} from "./scheduled-tasks";

// Runs scheduled tasks inside the omp-web server process. Each run is an
// ordinary omp session started through the same paths the UI uses, so it
// shows up live in the sidebar and can be opened, steered or continued.
// State that must survive Next.js hot reloads (in-flight runs, the timer)
// lives on globalThis, like the session registry in rpc-manager.

/** A run's session as the runner needs it (AgentSessionWrapper satisfies it). */
export interface ScheduledRunSession {
  readonly sessionId: string;
  isRunning(): boolean;
  send(command: Record<string, unknown>): Promise<unknown>;
  waitForTurnEnd(timeoutMs: number): Promise<void>;
  destroyAndWait(): Promise<void>;
}

export interface ScheduledTaskRunnerDeps {
  now(): Date;
  load(): ScheduledTaskStore;
  update<T>(update: (store: ScheduledTaskStore) => T | false): T | false;
  claimSlot(taskId: string, slot: Date): boolean;
  directoryExists(path: string): boolean;
  isProcessAlive(pid: number): boolean;
  /** Spawn a fresh session in `cwd` (no prompt sent). */
  createSession(cwd: string): Promise<ScheduledRunSession>;
  /** Attach to an existing session, or null when it no longer exists. */
  openSession(sessionId: string): Promise<ScheduledRunSession | null>;
  /** Format a run time for the session name, in the server's local time. */
  formatSessionTime(date: Date): string;
  log(message: string): void;
}

export class ScheduledTaskNotFoundError extends Error {
  code = "scheduled_task_not_found";
  constructor() {
    super("Scheduled task not found");
  }
}

export class ScheduledTaskLimitError extends Error {
  code = "scheduled_task_limit";
  constructor() {
    super(`At most ${SCHEDULED_TASK_LIMITS.maxTasks} scheduled tasks can exist`);
  }
}

export class ScheduledTaskRunningError extends Error {
  code = "scheduled_task_running";
  constructor() {
    super("This task is already running");
  }
}

/** Upper bound for one run; the session keeps running past it, only the
 *  run record stops waiting and is marked failed. */
export const SCHEDULED_RUN_TIMEOUT_MS = 6 * 60 * 60_000;

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export interface StartedScheduledRun {
  /** The record once the prompt was accepted, or the skip/failure record. */
  run: ScheduledTaskRun;
  /** Settles with the final record when the turn ends. */
  done: Promise<ScheduledTaskRun>;
}

export interface ScheduledTaskRunner {
  /** Fire every slot that is due now; returns the runs it started. */
  tick(): Promise<ScheduledTaskRun[]>;
  list(schedulerEnabled: boolean): ScheduledTaskListResponse;
  create(input: ScheduledTaskInput): ScheduledTask;
  update(id: string, patch: Partial<ScheduledTaskInput>): ScheduledTask;
  remove(id: string): void;
  runNow(id: string): Promise<ScheduledTaskRun>;
  /** Mark runs whose owning process is gone (or forgot them) as interrupted. */
  reconcileRuns(): void;
  startRun(task: ScheduledTask, trigger: ScheduledTaskRunTrigger, scheduledFor: Date | null): Promise<StartedScheduledRun>;
  isActive(taskId: string): boolean;
}

export function createScheduledTaskRunner(deps: ScheduledTaskRunnerDeps): ScheduledTaskRunner {
  /** taskId → id of its in-flight run in this process. */
  const active = new Map<string, string>();

  const saveRun = (run: ScheduledTaskRun) => {
    deps.update((store) => {
      if (!store.tasks.some((task) => task.id === run.taskId)) return false;
      store.runs = withScheduledTaskRun(store.runs, run);
      return true;
    });
  };

  const setLastSession = (taskId: string, sessionId: string) => {
    deps.update((store) => {
      const task = store.tasks.find((entry) => entry.id === taskId);
      if (!task || task.lastSessionId === sessionId) return false;
      task.lastSessionId = sessionId;
      return true;
    });
  };

  const finish = (run: ScheduledTaskRun, status: ScheduledTaskRun["status"], error: string | null) => {
    Object.assign(run, { status, error, finishedAt: deps.now().toISOString(), pid: null });
    saveRun(run);
  };

  /**
   * Start one run. Resolves once the prompt was accepted (or the run was
   * skipped or failed to start) with the run record; `done` settles when the
   * turn ends. Never throws for run-time problems: they become the record.
   */
  async function startRun(task: ScheduledTask, trigger: ScheduledTaskRunTrigger, scheduledFor: Date | null): Promise<StartedScheduledRun> {
    const run: ScheduledTaskRun = {
      id: `run_${randomUUID()}`,
      taskId: task.id,
      trigger,
      scheduledFor: scheduledFor?.toISOString() ?? null,
      startedAt: deps.now().toISOString(),
      finishedAt: null,
      sessionId: null,
      status: "running",
      error: null,
      pid: process.pid,
    };
    if (active.has(task.id)) {
      finish(run, "skipped", "The previous run of this task was still running.");
      return { run, done: Promise.resolve(run) };
    }
    active.set(task.id, run.id);
    saveRun(run);

    let session: ScheduledRunSession | null = null;
    let created = false;
    try {
      if (!deps.directoryExists(task.cwd)) throw new Error(`Workspace directory does not exist: ${task.cwd}`);
      if (task.sessionMode === "continue" && task.lastSessionId) {
        session = await deps.openSession(task.lastSessionId);
        if (session?.isRunning()) {
          active.delete(task.id);
          run.sessionId = session.sessionId;
          finish(run, "skipped", "The continued session was busy.");
          return { run, done: Promise.resolve(run) };
        }
      }
      if (!session) {
        session = await deps.createSession(task.cwd);
        created = true;
        await session.send({ type: "set_session_name", name: `${task.name} · ${deps.formatSessionTime(deps.now())}` });
      }
      if (task.model) await session.send({ type: "set_model", provider: task.model.provider, modelId: task.model.modelId });
      if (task.thinkingLevel) await session.send({ type: "set_thinking_level", level: task.thinkingLevel });
      run.sessionId = session.sessionId;
      saveRun(run);
      setLastSession(task.id, session.sessionId);
      // Register before sending so a fast turn cannot end unobserved.
      const turnEnd = session.waitForTurnEnd(SCHEDULED_RUN_TIMEOUT_MS);
      turnEnd.catch(() => {});
      await session.send({ type: "prompt", message: task.prompt });
      const live = session;
      const done = turnEnd.then(
        () => finish(run, "completed", null),
        (error: unknown) => finish(run, "failed", errorMessage(error)),
      ).then(() => {
        // A non-owner write moves the session to a new id; follow it.
        if (live.sessionId && live.sessionId !== run.sessionId) {
          run.sessionId = live.sessionId;
          saveRun(run);
          setLastSession(task.id, live.sessionId);
        }
        return run;
      }).finally(() => {
        if (active.get(task.id) === run.id) active.delete(task.id);
      });
      return { run, done };
    } catch (error) {
      active.delete(task.id);
      // A session spawned for a prompt that never ran would idle as an
      // orphaned child; a continued session belongs to the user and stays.
      if (created && session) await session.destroyAndWait().catch(() => {});
      finish(run, "failed", errorMessage(error));
      deps.log(`scheduled task ${task.id} failed to start: ${run.error}`);
      return { run, done: Promise.resolve(run) };
    }
  }

  /** Fire every slot that is due now. Returns the runs it started. */
  async function tick(): Promise<ScheduledTaskRun[]> {
    const now = deps.now();
    const started: ScheduledTaskRun[] = [];
    for (const task of deps.load().tasks) {
      const due = dueScheduleSlot(task, now);
      if (!due) continue;
      const claimed = deps.claimSlot(task.id, due.slot);
      // Advance the watermark whether or not this instance won the claim.
      const current = deps.update((store) => {
        const entry = store.tasks.find((candidate) => candidate.id === task.id);
        if (!entry || new Date(entry.lastSlotAt).getTime() >= due.slot.getTime()) return false;
        entry.lastSlotAt = due.slot.toISOString();
        return entry;
      });
      if (!claimed || current === false) continue;
      deps.log(`running scheduled task ${task.id} (${due.trigger}, slot ${due.slot.toISOString()})`);
      started.push((await startRun(current, due.trigger, due.slot)).run);
    }
    return started;
  }

  /** Mark runs whose owning process is gone (or forgot them) as interrupted. */
  function reconcileRuns(): void {
    deps.update((store) => {
      let changed = false;
      for (const run of store.runs) {
        if (run.status !== "running") continue;
        const orphaned = run.pid === null
          || (run.pid === process.pid ? active.get(run.taskId) !== run.id : !deps.isProcessAlive(run.pid));
        if (!orphaned) continue;
        Object.assign(run, { status: "interrupted", finishedAt: run.finishedAt ?? deps.now().toISOString(), pid: null });
        changed = true;
      }
      return changed;
    });
  }

  function list(schedulerEnabled: boolean): ScheduledTaskListResponse {
    reconcileRuns();
    const store = deps.load();
    const now = deps.now();
    const runningTasks = new Set(store.runs.filter((run) => run.status === "running").map((run) => run.taskId));
    return {
      tasks: store.tasks.map((task) => ({
        ...task,
        nextRunAt: nextScheduledRun(task, now)?.toISOString() ?? null,
        running: runningTasks.has(task.id),
      })),
      runs: store.runs,
      timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      schedulerEnabled,
    };
  }

  function create(input: ScheduledTaskInput): ScheduledTask {
    const task = deps.update((store) => {
      if (store.tasks.length >= SCHEDULED_TASK_LIMITS.maxTasks) throw new ScheduledTaskLimitError();
      const created = createScheduledTask(input, `st_${randomUUID()}`, deps.now());
      store.tasks.push(created);
      return created;
    });
    if (task === false) throw new Error("Scheduled task store rejected the write");
    return task;
  }

  function update(id: string, patch: Partial<ScheduledTaskInput>): ScheduledTask {
    const task = deps.update((store) => {
      const index = store.tasks.findIndex((entry) => entry.id === id);
      if (index < 0) throw new ScheduledTaskNotFoundError();
      store.tasks[index] = applyScheduledTaskPatch(store.tasks[index], patch, deps.now());
      return store.tasks[index];
    });
    if (task === false) throw new Error("Scheduled task store rejected the write");
    return task;
  }

  /** Delete a task and its history. A run in flight keeps going as a session. */
  function remove(id: string): void {
    deps.update((store) => {
      if (!store.tasks.some((task) => task.id === id)) throw new ScheduledTaskNotFoundError();
      store.tasks = store.tasks.filter((task) => task.id !== id);
      store.runs = store.runs.filter((run) => run.taskId !== id);
      return true;
    });
  }

  async function runNow(id: string): Promise<ScheduledTaskRun> {
    const task = deps.load().tasks.find((entry) => entry.id === id);
    if (!task) throw new ScheduledTaskNotFoundError();
    if (active.has(id)) throw new ScheduledTaskRunningError();
    return (await startRun(task, "manual", null)).run;
  }

  return { tick, list, create, update, remove, runNow, reconcileRuns, startRun, isActive: (taskId) => active.has(taskId) };
}

// ── Process singleton ────────────────────────────────────────────────────

const TICK_MS = 30_000;

declare global {
  var __ompScheduledTaskRunner: ScheduledTaskRunner | undefined;
  var __ompScheduledTaskTimer: NodeJS.Timeout | undefined;
}

/** The scheduler is on unless OMP_WEB_DISABLE_SCHEDULER=1 (manual runs and
 *  editing keep working either way). */
export function isSchedulerEnabled(): boolean {
  return process.env.OMP_WEB_DISABLE_SCHEDULER !== "1";
}

function defaultDeps(): ScheduledTaskRunnerDeps {
  return {
    now: () => new Date(),
    load: loadScheduledTaskStore,
    update: updateScheduledTaskStore,
    claimSlot: claimScheduledSlot,
    directoryExists: existsSync,
    isProcessAlive: (pid) => {
      try {
        process.kill(pid, 0);
        return true;
      } catch (error) {
        return (error as NodeJS.ErrnoException).code === "EPERM";
      }
    },
    async createSession(cwd) {
      // A unique key: startRpcSession coalesces callers that share one.
      const { session } = await startRpcSession(`__scheduled__${randomUUID()}`, "", cwd, undefined, undefined);
      allowFileRoot(cwd);
      invalidateSessionListCache();
      return session;
    },
    async openSession(sessionId) {
      const live = getRpcSession(sessionId);
      if (live?.isAlive()) return live;
      const filePath = await resolveSessionPath(sessionId);
      if (!filePath) return null;
      const header = readSessionHeader(filePath);
      const { cwd } = resolveSpawnCwdResult(header?.cwd);
      return (await startRpcSession(sessionId, filePath, cwd, undefined, undefined, header?.cwd)).session;
    },
    formatSessionTime: (date) => {
      const pad = (value: number) => String(value).padStart(2, "0");
      return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
    },
    log: (message) => console.log(`[omp-web] ${message}`),
  };
}

/** The process-wide runner (created on first use). */
export function getScheduledTaskRunner(): ScheduledTaskRunner {
  globalThis.__ompScheduledTaskRunner ??= createScheduledTaskRunner(defaultDeps());
  return globalThis.__ompScheduledTaskRunner;
}

/** Start the periodic tick (idempotent). The first tick runs immediately so a
 *  slot missed while omp-web was down catches up right after startup. */
export async function startScheduledTaskScheduler(): Promise<void> {
  if (!isSchedulerEnabled() || globalThis.__ompScheduledTaskTimer) return;
  const runner = getScheduledTaskRunner();
  pruneScheduledSlotClaims(new Date());
  runner.reconcileRuns();
  let ticking = false;
  const tick = async () => {
    if (ticking) return;
    ticking = true;
    try {
      await runner.tick();
    } catch (error) {
      console.warn(`[omp-web] scheduled task tick failed: ${errorMessage(error)}`);
    } finally {
      ticking = false;
    }
  };
  globalThis.__ompScheduledTaskTimer = setInterval(() => void tick(), TICK_MS);
  globalThis.__ompScheduledTaskTimer.unref?.();
  await tick();
}
