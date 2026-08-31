// @effect-diagnostics strictEffectProvide:off - tests are entry points
import { Effect, Schema, SubscriptionRef } from "effect";
import { TestClock } from "effect/testing";

import { ActorSystemDefault, ActorSystemService, Event, Machine, State } from "../src/index.js";
import { describe, expect, it, yieldFibers } from "effect-bun-test";

describe("Same-state Transitions", () => {
  const FormState = State({
    Form: { name: Schema.String, count: Schema.Number },
    Submitted: {},
  });
  type FormState = typeof FormState.Type;

  const FormEvent = Event({
    SetName: { name: Schema.String },
    Submit: {},
  });

  it.scopedLive("default: same state tag skips exit/enter effects", () =>
    Effect.gen(function* () {
      const effects: string[] = [];

      const machine = Machine.make({
        state: FormState,
        event: FormEvent,
        initial: FormState.Form({ name: "", count: 0 }),
      })
        .on(FormState.Form, FormEvent.SetName, ({ state, event }) =>
          FormState.Form.with(state, { name: event.name, count: state.count + 1 }),
        )
        .on(FormState.Form, FormEvent.Submit, () => FormState.Submitted)
        .spawn(FormState.Form, () =>
          Effect.gen(function* () {
            effects.push("enter:Form");
            yield* Effect.addFinalizer(() => Effect.sync(() => effects.push("exit:Form")));
          }),
        );

      const system = yield* ActorSystemService;
      const actor = yield* system.spawn("form", machine);
      yield* yieldFibers; // Let spawn effect run

      // Initial enter should fire
      expect(effects).toEqual(["enter:Form"]);

      // Same state tag - no exit/enter
      const r1 = yield* actor.call(FormEvent.SetName({ name: "Alice" }));
      yield* yieldFibers;

      expect(r1.newState._tag).toBe("Form");
      expect((r1.newState as FormState & { _tag: "Form" }).name).toBe("Alice");
      expect(effects).toEqual(["enter:Form"]);

      // Another same-state transition
      yield* actor.call(FormEvent.SetName({ name: "Bob" }));
      yield* yieldFibers;

      expect(effects).toEqual(["enter:Form"]);

      // Different state tag - runs exit
      yield* actor.call(FormEvent.Submit);
      yield* yieldFibers;

      expect(effects).toEqual(["enter:Form", "exit:Form"]);
    }).pipe(Effect.provide(ActorSystemDefault)),
  );

  it.scopedLive("reenter runs exit/enter for same state tag", () =>
    Effect.gen(function* () {
      const effects: string[] = [];

      const machine = Machine.make({
        state: FormState,
        event: FormEvent,
        initial: FormState.Form({ name: "", count: 0 }),
      })
        .reenter(FormState.Form, FormEvent.SetName, ({ state, event }) =>
          FormState.Form.with(state, { name: event.name, count: state.count + 1 }),
        )
        .spawn(FormState.Form, () =>
          Effect.gen(function* () {
            effects.push("enter:Form");
            yield* Effect.addFinalizer(() => Effect.sync(() => effects.push("exit:Form")));
          }),
        );

      const system = yield* ActorSystemService;
      const actor = yield* system.spawn("form", machine);
      yield* yieldFibers; // Let spawn effect run

      // Initial enter
      expect(effects).toEqual(["enter:Form"]);

      // reenter runs exit/enter even for same state tag
      yield* actor.call(FormEvent.SetName({ name: "Alice" }));
      yield* yieldFibers;

      expect(effects).toEqual(["enter:Form", "exit:Form", "enter:Form"]);
    }).pipe(Effect.provide(ActorSystemDefault)),
  );
});

describe("Reenter Transitions", () => {
  const PollState = State({
    Polling: { attempts: Schema.Number },
    Done: {},
  });
  type PollState = typeof PollState.Type;

  const PollEvent = Event({
    Poll: {},
    Reset: {},
    Finish: {},
  });

  it.scopedLive("reenter runs exit/enter for same state tag", () =>
    Effect.gen(function* () {
      const effects: string[] = [];

      const machine = Machine.make({
        state: PollState,
        event: PollEvent,
        initial: PollState.Polling({ attempts: 0 }),
      })
        .on(PollState.Polling, PollEvent.Finish, () => PollState.Done)
        .reenter(PollState.Polling, PollEvent.Reset, ({ state }) =>
          PollState.Polling.with(state, { attempts: state.attempts + 1 }),
        )
        .spawn(PollState.Polling, ({ state }) =>
          Effect.gen(function* () {
            // Log entry
            effects.push(`enter:Polling:${(state as PollState & { _tag: "Polling" }).attempts}`);
            // Log exit via finalizer
            yield* Effect.addFinalizer(() =>
              Effect.sync(() =>
                effects.push(`exit:Polling:${(state as PollState & { _tag: "Polling" }).attempts}`),
              ),
            );
          }),
        );

      const system = yield* ActorSystemService;
      const actor = yield* system.spawn("poller", machine);
      yield* yieldFibers; // Let spawn effect run

      // Initial enter
      expect(effects).toEqual(["enter:Polling:0"]);

      // reenter runs exit/enter
      const r1 = yield* actor.call(PollEvent.Reset);
      yield* yieldFibers;

      expect(r1.newState._tag).toBe("Polling");
      expect((r1.newState as PollState & { _tag: "Polling" }).attempts).toBe(1);
      expect(effects).toEqual(["enter:Polling:0", "exit:Polling:0", "enter:Polling:1"]);

      // Another reenter transition
      yield* actor.call(PollEvent.Reset);
      yield* yieldFibers;

      expect(effects).toEqual([
        "enter:Polling:0",
        "exit:Polling:0",
        "enter:Polling:1",
        "exit:Polling:1",
        "enter:Polling:2",
      ]);
    }).pipe(Effect.provide(ActorSystemDefault)),
  );

  it.scoped("reenter restarts task timer", () =>
    Effect.gen(function* () {
      const machine = Machine.make({
        state: PollState,
        event: PollEvent,
        initial: PollState.Polling({ attempts: 0 }),
      })
        .on(PollState.Polling, PollEvent.Poll, () => PollState.Done)
        .reenter(PollState.Polling, PollEvent.Reset, ({ state }) =>
          PollState.Polling.with(state, { attempts: state.attempts + 1 }),
        )
        .task(PollState.Polling, () => Effect.sleep("5 seconds"), {
          onSuccess: () => PollEvent.Poll,
        });

      const actor = yield* Machine.spawn(machine, { id: "poller" });
      yield* actor.start;

      // Advance 3 seconds
      yield* TestClock.adjust("3 seconds");
      yield* yieldFibers;

      let state = yield* SubscriptionRef.get(actor.state);
      expect(state._tag).toBe("Polling");

      // Reset - should restart the 5 second timer
      yield* actor.send(PollEvent.Reset);
      yield* yieldFibers;

      // Advance another 3 seconds (6 total from start, but 3 from reset)
      yield* TestClock.adjust("3 seconds");
      yield* yieldFibers;

      state = yield* SubscriptionRef.get(actor.state);
      expect(state._tag).toBe("Polling"); // Timer not done yet

      // Advance 2 more seconds (5 total from reset)
      yield* TestClock.adjust("2 seconds");
      yield* yieldFibers;

      state = yield* SubscriptionRef.get(actor.state);
      expect(state._tag).toBe("Done"); // Timer fired
    }).pipe(Effect.provide(ActorSystemDefault)),
  );
});
