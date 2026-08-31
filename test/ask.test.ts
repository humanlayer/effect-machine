// @effect-diagnostics strictEffectProvide:off - tests are entry points
import { Cause, Effect, Exit, Schema } from "effect";

import { Machine, State, Event } from "../src/index.js";
import { describe, expect, it } from "effect-bun-test";

const TestState = State({
  Idle: {},
  Active: { count: Schema.Number },
  Done: {},
});

const TestEvent = Event({
  Start: {},
  Increment: {},
  GetCount: Event.reply({}, Schema.Number),
  MultiplyCount: Event.reply({ factor: Schema.Number }, Schema.Number),
  GetNothing: Event.reply({}, Schema.Undefined),
  Stop: {},
});

const createMachine = () =>
  Machine.make({
    state: TestState,
    event: TestEvent,
    initial: TestState.Idle,
  })
    .on(TestState.Idle, TestEvent.Start, () => TestState.Active({ count: 0 }))
    .on(TestState.Active, TestEvent.Increment, ({ state }) =>
      TestState.Active({ count: state.count + 1 }),
    )
    // Handler that returns Machine.reply — typed domain reply for ask
    .on(TestState.Active, TestEvent.GetCount, ({ state }) =>
      Machine.reply(TestState.Active({ count: state.count }), state.count),
    )
    .on(TestState.Active, TestEvent.MultiplyCount, ({ state, event }) =>
      Machine.reply(TestState.Active({ count: state.count }), state.count * event.factor),
    )
    // Handler that returns Machine.reply with undefined — explicit undefined reply
    .on(TestState.Active, TestEvent.GetNothing, ({ state }) =>
      Machine.reply(TestState.Active({ count: state.count }), undefined),
    )
    .on(TestState.Active, TestEvent.Stop, () => TestState.Done)
    .final(TestState.Done);

describe("ActorRef.ask", () => {
  it.scopedLive("returns domain reply value from handler", () =>
    Effect.gen(function* () {
      const machine = createMachine();
      const actor = yield* Machine.spawn(machine);
      yield* actor.start;

      yield* actor.call(TestEvent.Start);
      yield* actor.call(TestEvent.Increment);
      yield* actor.call(TestEvent.Increment);

      const count = yield* actor.ask(TestEvent.GetCount);
      expect(count).toBe(2);
    }),
  );

  it.scopedLive("fails with NoReplyError when no handler matches", () =>
    Effect.gen(function* () {
      const machine = createMachine();
      const actor = yield* Machine.spawn(machine);
      yield* actor.start;

      // In Idle state — no handler for GetCount, so ask fails with NoReplyError
      const result = yield* actor.ask(TestEvent.GetCount).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
    }),
  );

  it.scopedLive("fails with ActorStoppedError on stopped actor", () =>
    Effect.gen(function* () {
      const machine = createMachine();
      const actor = yield* Machine.spawn(machine);
      yield* actor.start;

      yield* actor.stop;

      const result = yield* actor.ask(TestEvent.GetCount).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
    }),
  );

  it.scopedLive("call still returns ProcessEventResult (unchanged)", () =>
    Effect.gen(function* () {
      const machine = createMachine();
      const actor = yield* Machine.spawn(machine);
      yield* actor.start;

      yield* actor.call(TestEvent.Start);
      yield* actor.call(TestEvent.Increment);
      yield* actor.call(TestEvent.Increment);

      // call on GetCount handler returns ProcessEventResult, not the reply value
      const result = yield* actor.call(TestEvent.GetCount);
      expect(result.transitioned).toBe(true);
      expect(result.newState._tag).toBe("Active");
      // The reply field is on the result for inspection but call returns the full result
      expect(result.reply).toBe(2);
    }),
  );

  it.scopedLive("ask works with multiple sequential calls", () =>
    Effect.gen(function* () {
      const machine = createMachine();
      const actor = yield* Machine.spawn(machine);
      yield* actor.start;

      yield* actor.call(TestEvent.Start);

      yield* actor.call(TestEvent.Increment);
      const count1 = yield* actor.ask(TestEvent.GetCount);
      expect(count1).toBe(1);

      yield* actor.call(TestEvent.Increment);
      const count2 = yield* actor.ask(TestEvent.GetCount);
      expect(count2).toBe(2);
    }),
  );

  it.scopedLive("reply-bearing events with payload accept plain constructor args", () =>
    Effect.gen(function* () {
      const machine = createMachine();
      const actor = yield* Machine.spawn(machine);
      yield* actor.start;

      yield* actor.call(TestEvent.Start);
      yield* actor.call(TestEvent.Increment);
      yield* actor.call(TestEvent.Increment);

      const result = yield* actor.ask(TestEvent.MultiplyCount({ factor: 3 }));
      expect(result).toBe(6);
    }),
  );

  it.scopedLive("reply: undefined is a valid reply, not NoReplyError", () =>
    Effect.gen(function* () {
      const machine = createMachine();
      const actor = yield* Machine.spawn(machine);
      yield* actor.start;

      yield* actor.call(TestEvent.Start);

      const result = yield* actor.ask(TestEvent.GetNothing);
      expect(result).toBeUndefined();
    }),
  );

  it.scopedLive("decodes a transforming reply schema exactly once", () =>
    Effect.gen(function* () {
      const TransformEvent = Event({
        GetCount: Event.reply({}, Schema.NumberFromString),
      });

      const machine = Machine.make({
        state: TestState,
        event: TransformEvent,
        initial: TestState.Active({ count: 7 }),
      }).on(TestState.Active, TransformEvent.GetCount, ({ state }) =>
        Machine.reply(state, state.count),
      );

      const actor = yield* Machine.spawn(machine);
      yield* actor.start;

      const count = yield* actor.ask(TransformEvent.GetCount);
      expect(count).toBe(7);
      const typedCount: number = count;
      expect(typedCount).toBe(7);
    }),
  );

  it.scopedLive("decodes a deferred transforming reply exactly once", () =>
    Effect.gen(function* () {
      const DeferredState = State({
        Idle: {},
        Replying: {},
      });
      const DeferredEvent = Event({
        GetCount: Event.reply({}, Schema.NumberFromString),
      });
      const machine = Machine.make({
        state: DeferredState,
        event: DeferredEvent,
        initial: DeferredState.Idle,
      })
        .on(DeferredState.Idle, DeferredEvent.GetCount, () =>
          Machine.deferReply(DeferredState.Replying),
        )
        .spawn(DeferredState.Replying, ({ self }) =>
          Effect.sleep("10 millis").pipe(Effect.andThen(self.reply(9)), Effect.asVoid),
        );

      const actor = yield* Machine.spawn(machine);
      yield* actor.start;

      const count = yield* actor.ask(DeferredEvent.GetCount).pipe(Effect.timeout("2 seconds"));
      expect(count).toBe(9);
      const typedCount: number = count;
      expect(typedCount).toBe(9);
    }),
  );

  it.scopedLive("defects a deferred ask when transforming reply encoding fails", () =>
    Effect.gen(function* () {
      const DeferredState = State({
        Idle: {},
        Replying: {},
      });
      const DeferredEvent = Event({
        GetCount: Event.reply({}, Schema.NumberFromString),
      });
      const machine = Machine.make({
        state: DeferredState,
        event: DeferredEvent,
        initial: DeferredState.Idle,
      })
        .on(DeferredState.Idle, DeferredEvent.GetCount, () =>
          Machine.deferReply(DeferredState.Replying),
        )
        .spawn(DeferredState.Replying, ({ self }) =>
          Effect.sleep("10 millis").pipe(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any -- intentional encoding mismatch
            Effect.andThen(self.reply("not-a-number" as any)),
            Effect.asVoid,
          ),
        );

      const actor = yield* Machine.spawn(machine);
      yield* actor.start;

      const exit = yield* actor
        .ask(DeferredEvent.GetCount)
        .pipe(Effect.timeout("2 seconds"), Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) {
        expect(Cause.hasDies(exit.cause)).toBe(true);
        expect(Cause.squash(exit.cause)).toHaveProperty("_tag", "SchemaError");
      }
    }),
  );

  it.scopedLive("reply schema mismatch is a defect (die)", () =>
    Effect.gen(function* () {
      // Build a machine where the handler lies about the reply type
      const BadEvent = Event({
        GetCount: Event.reply({}, Schema.Number),
      });

      const machine = Machine.make({
        state: TestState,
        event: BadEvent,
        initial: TestState.Idle,
      }).on(TestState.Idle, BadEvent.GetCount, () =>
        // eslint-disable-next-line @typescript-eslint/no-explicit-any -- intentional mismatch
        Machine.reply(TestState.Idle, "not-a-number" as any),
      );

      const actor = yield* Machine.spawn(machine);
      yield* actor.start;
      const exit = yield* actor.ask(BadEvent.GetCount).pipe(Effect.exit);
      // Decode failure surfaces as a defect (die), not a checked error
      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        expect(Cause.hasDies(exit.cause)).toBe(true);
      }
    }),
  );
});
