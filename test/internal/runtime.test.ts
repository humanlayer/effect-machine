// @effect-diagnostics strictEffectProvide:off - tests are entry points
import type { Scope } from "effect";
import { Cause, Effect, Exit } from "effect";
import { expect } from "bun:test";
import { describe, it } from "effect-bun-test";

import { Event, Machine, State } from "../../src/index.js";

const TestState = State({
  Idle: {},
});

const TestEvent = Event({
  Ping: {},
});

describe("actor start gate", () => {
  it.scopedLive("preserves the original defect for concurrent and repeated callers", () =>
    Effect.gen(function* () {
      const defect = new Error("startup exploded");
      const machine = Machine.make({
        state: TestState,
        event: TestEvent,
        initial: TestState.Idle,
      }).spawn(TestState.Idle, (): Effect.Effect<void, never, Scope.Scope> => {
        // Deliberately violate the handler contract before returning an Effect to exercise
        // startup-gate defect propagation rather than an asynchronously forked defect.
        throw defect;
      });
      const actor = yield* Machine.spawn(machine, { id: "start-gate" });

      const infallibleStart: Effect.Effect<void> = actor.start;
      const concurrent = yield* Effect.all(
        [
          infallibleStart.pipe(Effect.exit),
          infallibleStart.pipe(Effect.exit),
          infallibleStart.pipe(Effect.exit),
        ],
        { concurrency: "unbounded" },
      );
      const repeated = yield* infallibleStart.pipe(Effect.exit);

      let originalCause: Cause.Cause<never> | undefined;
      for (const exit of [...concurrent, repeated]) {
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          expect(Cause.squash(exit.cause)).toBe(defect);
          if (originalCause === undefined) {
            originalCause = exit.cause;
          } else {
            expect(exit.cause).toEqual(originalCause);
          }
        }
      }

      yield* actor.stop;
    }),
  );
});
