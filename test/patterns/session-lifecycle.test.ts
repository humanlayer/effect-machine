// @effect-diagnostics strictEffectProvide:off - tests are entry points
import { Clock, Effect, Schema, SubscriptionRef } from "effect";
import { TestClock } from "effect/testing";

import { ActorSystemDefault, assertPath, Event, Machine, State } from "../../src/index.js";
import { describe, expect, it, yieldFibers } from "effect-bun-test";

/**
 * Session lifecycle pattern tests based on bite session.machine.ts
 * Tests: initial state calculation, maintenance interrupt, session timeout
 */
describe("Session Lifecycle Pattern", () => {
  const UserRole = Schema.Literals(["guest", "user", "admin"]);
  type UserRole = typeof UserRole.Type;

  const SessionState = State({
    Guest: {},
    Active: { userId: Schema.String, role: UserRole, lastActivity: Schema.Number },
    Maintenance: { message: Schema.String, previousState: Schema.Literals(["Guest", "Active"]) },
    SessionExpired: {},
    LoggedOut: {},
  });
  type SessionState = typeof SessionState.Type;

  const SessionEvent = Event({
    Login: { userId: Schema.String, role: UserRole },
    Activity: {},
    MaintenanceStarted: { message: Schema.String },
    MaintenanceEnded: {},
    SessionTimeout: {},
    Logout: {},
  });

  // Helper to compute initial state based on token
  const makeSessionMachine = (token: string | null) => {
    // Initial state computed inline - no need for .always()
    const initial =
      token === null
        ? SessionState.Guest
        : SessionState.Active({ userId: "from-token", role: "user", lastActivity: Date.now() });

    return Machine.make({
      state: SessionState,
      event: SessionEvent,
      initial,
    })
      .on(SessionState.Guest, SessionEvent.Login, ({ event }) =>
        SessionState.Active({ userId: event.userId, role: event.role, lastActivity: Date.now() }),
      )
      .on(
        [SessionState.Active, SessionState.Guest],
        SessionEvent.MaintenanceStarted,
        ({ state, event }) =>
          SessionState.Maintenance({
            message: event.message,
            previousState: state._tag,
          }),
      )
      .on(SessionState.Active, SessionEvent.SessionTimeout, () => SessionState.SessionExpired)
      .task(SessionState.Active, () => Effect.sleep("30 minutes"), {
        onSuccess: () => SessionEvent.SessionTimeout,
      })
      .on(SessionState.Maintenance, SessionEvent.MaintenanceEnded, ({ state }) =>
        state.previousState === "Active"
          ? SessionState.Active({ userId: "restored", role: "user", lastActivity: Date.now() })
          : SessionState.Guest,
      )
      .on(SessionState.Active, SessionEvent.Logout, () => SessionState.LoggedOut)
      .final(SessionState.SessionExpired)
      .final(SessionState.LoggedOut);
  };

  it.live("null token starts as Guest", () =>
    Effect.gen(function* () {
      const machine = makeSessionMachine(null);
      const result = yield* assertPath(machine, [], ["Guest"]);
      expect(result.finalState._tag).toBe("Guest");
    }),
  );

  it.live("valid token starts as Active", () =>
    Effect.gen(function* () {
      const machine = makeSessionMachine("valid-token");
      const result = yield* assertPath(machine, [], ["Active"]);
      expect(result.finalState._tag).toBe("Active");
    }),
  );

  it.live("guest can login to active session", () =>
    Effect.gen(function* () {
      const machine = makeSessionMachine(null);
      const result = yield* assertPath(
        machine,
        [SessionEvent.Login({ userId: "user-123", role: "user" })],
        ["Guest", "Active"],
      );
      expect(result.finalState._tag).toBe("Active");
    }),
  );

  it.live("maintenance mode interrupts active session", () => {
    const machine = Machine.make({
      state: SessionState,
      event: SessionEvent,
      initial: SessionState.Active({ userId: "user-1", role: "user", lastActivity: Date.now() }),
    })
      .on(SessionState.Active, SessionEvent.MaintenanceStarted, ({ event }) =>
        SessionState.Maintenance({ message: event.message, previousState: "Active" }),
      )
      .on(SessionState.Maintenance, SessionEvent.MaintenanceEnded, ({ state }) =>
        state.previousState === "Active"
          ? SessionState.Active({ userId: "restored", role: "user", lastActivity: Date.now() })
          : SessionState.Guest,
      );

    return assertPath(
      machine,
      [
        SessionEvent.MaintenanceStarted({ message: "System upgrade" }),
        SessionEvent.MaintenanceEnded,
      ],
      ["Active", "Maintenance", "Active"],
    );
  });

  it.scoped("session timeout after inactivity", () =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const activeMachine = Machine.make({
        state: SessionState,
        event: SessionEvent,
        initial: SessionState.Active({
          userId: "user-1",
          role: "user",
          lastActivity: now,
        }),
      })
        .on(SessionState.Active, SessionEvent.SessionTimeout, () => SessionState.SessionExpired)
        .task(SessionState.Active, () => Effect.sleep("30 minutes"), {
          onSuccess: () => SessionEvent.SessionTimeout,
        })
        .final(SessionState.SessionExpired);

      const actor = yield* Machine.spawn(activeMachine, { id: "session" });
      yield* actor.start;

      let state = yield* SubscriptionRef.get(actor.state);
      expect(state._tag).toBe("Active");

      // Activity within timeout window
      yield* TestClock.adjust("15 minutes");
      yield* actor.send(SessionEvent.Activity);
      yield* yieldFibers;

      state = yield* SubscriptionRef.get(actor.state);
      expect(state._tag).toBe("Active");

      // Activity does NOT reset timer (internal transition)
      // So after 15 more minutes (30 total from start), should timeout
      yield* TestClock.adjust("15 minutes");
      yield* yieldFibers;

      state = yield* SubscriptionRef.get(actor.state);
      expect(state._tag).toBe("SessionExpired");
    }).pipe(Effect.provide(ActorSystemDefault)),
  );

  it.scoped("activity with reenter resets timeout", () =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const activeMachine = Machine.make({
        state: SessionState,
        event: SessionEvent,
        initial: SessionState.Active({
          userId: "user-1",
          role: "user",
          lastActivity: now,
        }),
      })
        .task(SessionState.Active, () => Effect.sleep("30 minutes"), {
          onSuccess: () => SessionEvent.SessionTimeout,
        })
        .on(SessionState.Active, SessionEvent.SessionTimeout, () => SessionState.SessionExpired)
        // Use reenter to reenter the state, resetting the task timer
        .reenter(SessionState.Active, SessionEvent.Activity, ({ state }) =>
          SessionState.Active.with(state, { lastActivity: Date.now() }),
        )
        .final(SessionState.SessionExpired);

      const actor = yield* Machine.spawn(activeMachine, { id: "session" });
      yield* actor.start;

      // Activity after 20 minutes
      yield* TestClock.adjust("20 minutes");
      yield* actor.send(SessionEvent.Activity);
      yield* yieldFibers;

      let state = yield* SubscriptionRef.get(actor.state);
      expect(state._tag).toBe("Active");

      // 20 more minutes (40 total, but only 20 from activity)
      yield* TestClock.adjust("20 minutes");
      yield* yieldFibers;

      state = yield* SubscriptionRef.get(actor.state);
      expect(state._tag).toBe("Active"); // Timer was reset

      // 10 more minutes (30 from activity)
      yield* TestClock.adjust("10 minutes");
      yield* yieldFibers;

      state = yield* SubscriptionRef.get(actor.state);
      expect(state._tag).toBe("SessionExpired");
    }).pipe(Effect.provide(ActorSystemDefault)),
  );

  it.live("logout from active session", () => {
    const activeMachine = Machine.make({
      state: SessionState,
      event: SessionEvent,
      initial: SessionState.Active({ userId: "user-1", role: "user", lastActivity: Date.now() }),
    })
      .on(SessionState.Active, SessionEvent.Logout, () => SessionState.LoggedOut)
      .final(SessionState.LoggedOut);

    return assertPath(activeMachine, [SessionEvent.Logout], ["Active", "LoggedOut"]);
  });
});
