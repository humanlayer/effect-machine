import { Effect, SubscriptionRef } from "effect";

import type { Machine, MachineRef } from "./machine.js";
import { materializeMachine } from "./machine.js";
import { AssertionError } from "./errors.js";
import type { SlotsDef, ProvideSlots } from "./slot.js";
import { executeTransition, shouldPostpone } from "./internal/transition.js";
import { stubSystem } from "./internal/utils.js";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type MachineInput<S, E, R, SD extends SlotsDef = Record<string, never>> =
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Machine<S, E, R, any, any, SD>;

const makeDummySelf = <E>(label: string): MachineRef<E> => {
  const dummySend = Effect.fn(label)((_event: E) => Effect.void);
  return {
    send: dummySend,
    cast: dummySend,
    spawn: () => Effect.die(`spawn not supported in ${label}`),
    reply: () => Effect.succeed(false),
  };
};

/**
 * Result of simulating events through a machine
 */
export interface SimulationResult<S> {
  readonly states: ReadonlyArray<S>;
  readonly finalState: S;
}

/**
 * Simulate a sequence of events through a machine without running an actor.
 * Useful for testing state transitions in isolation.
 * Does not run onEnter/spawn/background effects, but does run slots
 * within transition handlers.
 *
 * @example
 * ```ts
 * const result = yield* simulate(
 *   fetcherMachine,
 *   [
 *     Event.Fetch({ url: "https://example.com" }),
 *     Event._Done({ data: { foo: "bar" } })
 *   ]
 * )
 *
 * expect(result.finalState._tag).toBe("Success")
 * expect(result.states).toHaveLength(3) // Idle -> Loading -> Success
 * ```
 */
export const simulate = Effect.fn("effect-machine.simulate")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef = Record<string, never>,
>(
  input: MachineInput<S, E, R, SD>,
  events: ReadonlyArray<E>,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  options?: { slots?: ProvideSlots<SD, any> },
) {
  // SAFETY: materialization preserves this input machine's state, event, and slot domains.
  const machine = materializeMachine(input, options?.slots) as Machine<
    S,
    E,
    R,
    Record<string, never>,
    Record<string, never>,
    SD
  >;

  const dummySelf = makeDummySelf<E>("effect-machine.testing.simulate");

  let currentState = machine.initial;
  const states: S[] = [currentState];
  const hasPostponeRules = machine.postponeRules.length > 0;
  const postponed: E[] = [];

  for (const event of events) {
    // Check postpone rules
    if (hasPostponeRules && shouldPostpone(machine, currentState._tag, event._tag)) {
      postponed.push(event);
      continue;
    }

    const result = yield* executeTransition(
      machine,
      currentState,
      event,
      dummySelf,
      stubSystem,
      "simulation",
    );

    if (!result.transitioned) {
      continue;
    }

    const prevTag = currentState._tag;
    currentState = result.newState;
    states.push(currentState);

    // Stop if final state
    if (machine.finalStates.has(currentState._tag)) {
      break;
    }

    // Drain postponed events after state tag change — loop until stable
    let drainTag = prevTag;
    while (currentState._tag !== drainTag && postponed.length > 0) {
      drainTag = currentState._tag;
      const drained = postponed.splice(0);
      for (const postponedEvent of drained) {
        if (shouldPostpone(machine, currentState._tag, postponedEvent._tag)) {
          postponed.push(postponedEvent);
          continue;
        }
        const drainResult = yield* executeTransition(
          machine,
          currentState,
          postponedEvent,
          dummySelf,
          stubSystem,
          "simulation",
        );
        if (drainResult.transitioned) {
          currentState = drainResult.newState;
          states.push(currentState);
          if (machine.finalStates.has(currentState._tag)) {
            break;
          }
        }
      }
    }
  }

  return { states, finalState: currentState };
});

// AssertionError is exported from errors.ts
export { AssertionError } from "./errors.js";

/**
 * Assert that a machine can reach a specific state given a sequence of events
 */
export const assertReaches = Effect.fn("effect-machine.assertReaches")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef = Record<string, never>,
>(
  input: MachineInput<S, E, R, SD>,
  events: ReadonlyArray<E>,
  expectedTag: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  options?: { slots?: ProvideSlots<SD, any> },
) {
  const result = yield* simulate(input, events, options);
  if (result.finalState._tag !== expectedTag) {
    return yield* new AssertionError({
      message:
        `Expected final state "${expectedTag}" but got "${result.finalState._tag}". ` +
        `States visited: ${result.states.map((s) => s._tag).join(" -> ")}`,
    });
  }
  return result.finalState;
});

/**
 * Assert that a machine follows a specific path of state tags
 *
 * @example
 * ```ts
 * yield* assertPath(
 *   machine,
 *   [Event.Start(), Event.Increment(), Event.Stop()],
 *   ["Idle", "Counting", "Counting", "Done"]
 * )
 * ```
 */
export const assertPath = Effect.fn("effect-machine.assertPath")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef = Record<string, never>,
>(
  input: MachineInput<S, E, R, SD>,
  events: ReadonlyArray<E>,
  expectedPath: ReadonlyArray<string>,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  options?: { slots?: ProvideSlots<SD, any> },
) {
  const result = yield* simulate(input, events, options);
  const actualPath = result.states.map((s) => s._tag);

  if (actualPath.length !== expectedPath.length) {
    return yield* new AssertionError({
      message:
        `Path length mismatch. Expected ${expectedPath.length} states but got ${actualPath.length}.\n` +
        `Expected: ${expectedPath.join(" -> ")}\n` +
        `Actual:   ${actualPath.join(" -> ")}`,
    });
  }

  for (let i = 0; i < expectedPath.length; i++) {
    if (actualPath[i] !== expectedPath[i]) {
      return yield* new AssertionError({
        message:
          `Path mismatch at position ${i}. Expected "${expectedPath[i]}" but got "${actualPath[i]}".\n` +
          `Expected: ${expectedPath.join(" -> ")}\n` +
          `Actual:   ${actualPath.join(" -> ")}`,
      });
    }
  }

  return result;
});

/**
 * Assert that a machine never reaches a specific state given a sequence of events
 *
 * @example
 * ```ts
 * // Verify error handling doesn't reach crash state
 * yield* assertNeverReaches(
 *   machine,
 *   [Event.Error(), Event.Retry(), Event.Success()],
 *   "Crashed"
 * )
 * ```
 */
export const assertNeverReaches = Effect.fn("effect-machine.assertNeverReaches")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef = Record<string, never>,
>(
  input: MachineInput<S, E, R, SD>,
  events: ReadonlyArray<E>,
  forbiddenTag: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  options?: { slots?: ProvideSlots<SD, any> },
) {
  const result = yield* simulate(input, events, options);

  const visitedIndex = result.states.findIndex((s) => s._tag === forbiddenTag);
  if (visitedIndex !== -1) {
    return yield* new AssertionError({
      message:
        `Machine reached forbidden state "${forbiddenTag}" at position ${visitedIndex}.\n` +
        `States visited: ${result.states.map((s) => s._tag).join(" -> ")}`,
    });
  }

  return result;
});

/**
 * Create a controllable test harness for a machine
 */
export interface TestHarness<S, E, R> {
  readonly state: SubscriptionRef.SubscriptionRef<S>;
  readonly send: (event: E) => Effect.Effect<S, never, R>;
  readonly getState: Effect.Effect<S>;
}

/**
 * Options for creating a test harness
 */
export interface TestHarnessOptions<S, E, SD extends SlotsDef = Record<string, never>> {
  /**
   * Called after each transition with the previous state, event, and new state.
   * Useful for logging or spying on transitions.
   */
  readonly onTransition?: (from: S, event: E, to: S) => void;
  /** Slot handler implementations. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly slots?: ProvideSlots<SD, any>;
}

/**
 * Create a test harness for step-by-step testing.
 * Does not run onEnter/spawn/background effects, but does run slots
 * within transition handlers.
 *
 * @example Basic usage
 * ```ts
 * const harness = yield* createTestHarness(machine)
 * yield* harness.send(Event.Start())
 * const state = yield* harness.getState
 * ```
 *
 * @example With transition observer
 * ```ts
 * const transitions: Array<{ from: string; event: string; to: string }> = []
 * const harness = yield* createTestHarness(machine, {
 *   onTransition: (from, event, to) =>
 *     transitions.push({ from: from._tag, event: event._tag, to: to._tag })
 * })
 * ```
 */
export const createTestHarness = Effect.fn("effect-machine.createTestHarness")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef = Record<string, never>,
>(input: MachineInput<S, E, R, SD>, options?: TestHarnessOptions<S, E, SD>) {
  // SAFETY: materialization preserves this input machine's state, event, and slot domains.
  const machine = materializeMachine(input, options?.slots) as Machine<
    S,
    E,
    R,
    Record<string, never>,
    Record<string, never>,
    SD
  >;

  const dummySelf = makeDummySelf<E>("effect-machine.testing.harness");

  const stateRef = yield* SubscriptionRef.make(machine.initial);
  const hasPostponeRules = machine.postponeRules.length > 0;
  const postponed: E[] = [];

  const send = Effect.fn("effect-machine.testHarness.send")(function* (event: E) {
    const currentState = yield* SubscriptionRef.get(stateRef);

    // Check postpone rules
    if (hasPostponeRules && shouldPostpone(machine, currentState._tag, event._tag)) {
      postponed.push(event);
      return currentState;
    }

    const result = yield* executeTransition(
      machine,
      currentState,
      event,
      dummySelf,
      stubSystem,
      "test-harness",
    );

    if (!result.transitioned) {
      return currentState;
    }

    const prevTag = currentState._tag;
    const newState = result.newState;
    yield* SubscriptionRef.set(stateRef, newState);

    // Call transition observer
    if (options?.onTransition !== undefined) {
      options.onTransition(currentState, event, newState);
    }

    // Drain postponed after state tag change — loop until stable
    let drainTag = prevTag;
    let currentTag = newState._tag;
    while (currentTag !== drainTag && postponed.length > 0) {
      drainTag = currentTag;
      const drained = postponed.splice(0);
      for (const postponedEvent of drained) {
        const state = yield* SubscriptionRef.get(stateRef);
        if (shouldPostpone(machine, state._tag, postponedEvent._tag)) {
          postponed.push(postponedEvent);
          continue;
        }
        const drainResult = yield* executeTransition(
          machine,
          state,
          postponedEvent,
          dummySelf,
          stubSystem,
          "test-harness",
        );
        if (drainResult.transitioned) {
          yield* SubscriptionRef.set(stateRef, drainResult.newState);
          currentTag = drainResult.newState._tag;
          if (options?.onTransition !== undefined) {
            options.onTransition(state, postponedEvent, drainResult.newState);
          }
        }
      }
    }

    return newState;
  });

  return {
    state: stateRef,
    send,
    getState: SubscriptionRef.get(stateRef),
  };
});
