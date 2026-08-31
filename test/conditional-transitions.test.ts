// @effect-diagnostics strictEffectProvide:off - tests are entry points
import { Effect, Schema } from "effect";
import { describe, expect, test } from "bun:test";

import { Event, Machine, simulate, State } from "../src/index.js";

describe("Conditional Transitions (replaces choose combinator)", () => {
  test("first matching condition wins", async () => {
    const TestState = State({
      Idle: { value: Schema.Number },
      High: {},
      Medium: {},
      Low: {},
    });
    const TestEvent = Event({ Check: {} });

    const machine = Machine.make({
      state: TestState,
      event: TestEvent,
      initial: TestState.Idle({ value: 75 }),
    })
      .on(TestState.Idle, TestEvent.Check, ({ state }) => {
        if (state.value >= 70) return TestState.High;
        if (state.value >= 40) return TestState.Medium;
        return TestState.Low;
      })
      .final(TestState.High)
      .final(TestState.Medium)
      .final(TestState.Low);

    const result = await Effect.runPromise(simulate(machine, [TestEvent.Check]));
    expect(result.finalState._tag).toBe("High");
  });

  test("fallback branch catches all", async () => {
    const TestState = State({
      Idle: { value: Schema.Number },
      High: {},
      Low: {},
    });
    const TestEvent = Event({ Check: {} });

    const machine = Machine.make({
      state: TestState,
      event: TestEvent,
      initial: TestState.Idle({ value: 10 }),
    })
      .on(TestState.Idle, TestEvent.Check, ({ state }) =>
        state.value >= 70 ? TestState.High : TestState.Low,
      )
      .final(TestState.High)
      .final(TestState.Low);

    const result = await Effect.runPromise(simulate(machine, [TestEvent.Check]));
    expect(result.finalState._tag).toBe("Low");
  });

  test("runs effect in matching branch", async () => {
    const TestState = State({ Idle: {}, Done: {} });
    const TestEvent = Event({ Go: {} });
    const logs: string[] = [];

    const machine = Machine.make({
      state: TestState,
      event: TestEvent,
      initial: TestState.Idle,
    })
      .on(TestState.Idle, TestEvent.Go, () =>
        Effect.sync(() => {
          logs.push("effect ran");
          return TestState.Done;
        }),
      )
      .final(TestState.Done);

    await Effect.runPromise(simulate(machine, [TestEvent.Go]));
    expect(logs).toEqual(["effect ran"]);
  });
});
