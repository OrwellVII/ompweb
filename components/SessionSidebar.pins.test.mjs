import "../tests/setup-dom.mjs";
import assert from "node:assert/strict";
import test, { afterEach } from "node:test";
import React from "react";
import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react/pure.js";
import userEvent from "@testing-library/user-event";
import { createJiti } from "jiti";

// The live-event stream needs a worker; this test drives the list by fetches only.
globalThis.Worker ??= class { onmessage = null; onerror = null; postMessage() {} };
const jiti = createJiti(import.meta.url, { jsx: { runtime: "automatic" }, tryNative: false, tsconfigPaths: true });
const { SessionSidebar } = await jiti.import("@/components/SessionSidebar");

const originalFetch = globalThis.fetch;
afterEach(() => {
  cleanup();
  globalThis.fetch = originalFetch;
  localStorage.clear();
});

const noop = () => {};
const session = {
  path: "/work/proj/s-x.jsonl",
  id: "s-x",
  cwd: "/work/proj",
  name: "Pinned elsewhere",
  created: "2026-01-01T00:00:00.000Z",
  modified: "2026-01-01T00:00:00.000Z",
  messageCount: 1,
  firstMessage: "hello",
};

test("a failed pin write refetches the list without its ETag so the server's pins win", async () => {
  // The server's pin list; the session list ETag follows it like the real route.
  let serverPins = ["s-x"];
  const etag = () => `"pins-${serverPins.join(",")}"`;
  const listRequests = [];
  let failPatch;
  globalThis.fetch = async (input, init = {}) => {
    const url = String(input);
    if (url === "/api/sessions") {
      const ifNoneMatch = init.headers?.["If-None-Match"] ?? null;
      listRequests.push(ifNoneMatch);
      if (ifNoneMatch === etag()) return new Response(null, { status: 304 });
      return new Response(JSON.stringify({ sessions: [session], runningSessionIds: [], runningSessions: [], pinnedSessionIds: serverPins }), { status: 200, headers: { ETag: etag() } });
    }
    if (url === "/api/sessions/s-x" && init.method === "PATCH") {
      return new Promise((resolve) => { failPatch = () => resolve(new Response("{}", { status: 500 })); });
    }
    if (url === "/api/projects") return new Response(JSON.stringify({ projects: [] }), { status: 200 });
    return new Response("{}", { status: 404 });
  };

  render(React.createElement(SessionSidebar, { selectedSessionId: null, onSelectSession: noop, addProjectOpen: false, setAddProjectOpen: noop, onOpenArchive: noop }));
  await waitFor(() => assert.ok(document.querySelector(".sidebar-pinned-sessions")?.textContent.includes("Pinned elsewhere")));

  // Unpin here; the write is still in flight when another device unpins the
  // same session and a refresh commits that list (and its new ETag).
  const user = userEvent.setup();
  const group = document.querySelector(".sidebar-pinned-sessions");
  await user.click(within(group).getByRole("button", { name: "Project actions" }));
  await user.click(await screen.findByRole("menuitem", { name: "Unpin" }));
  assert.equal(document.querySelector(".sidebar-pinned-sessions"), null, "the unpin shows at once");
  serverPins = [];
  // The browser reporting the network is back triggers the sidebar's refresh.
  await act(async () => { window.dispatchEvent(new window.Event("online")); });
  await waitFor(() => assert.equal(listRequests.length, 2));

  // This device's write fails: the local rollback re-pins the session, but the
  // server no longer pins it, so the refetch must not be answered with a 304.
  await act(async () => { failPatch(); });
  await waitFor(() => assert.equal(listRequests.length, 3));
  assert.equal(listRequests[2], null, "the refetch after a failed pin write is unconditional");
  await waitFor(() => assert.equal(document.querySelector(".sidebar-pinned-sessions"), null, "the server's pins win over the rollback"));
});
