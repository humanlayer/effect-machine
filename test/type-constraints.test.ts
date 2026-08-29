// @effect-diagnostics missingEffectContext:off
// @effect-diagnostics missingEffectError:off
// @effect-diagnostics unnecessaryEffectGen:off
// @effect-diagnostics deterministicKeys:off
/**
 * Type-level tests for handler constraints.
 *
 * These tests verify that handlers:
 * 1. Keep transition handlers pure while allowing state effects to require services
 * 2. Cannot produce errors
 * 3. Must return machine-scoped state schema
 *
 * All "bad" tests use @ts-expect-error on the handler return expression.
 */
import { Effect, Schema, Context } from "effect";
import { Machine, State, Event, Slot } from "../src/index.js";
import type { ProvideSlots } from "../src/slot.js";

const MyState = State({
  Idle: {},
  Loading: { url: Schema.String },
  Done: {},
});

const MyEvent = Event({
  Start: {},
  Complete: {},
});

// Test 1: Handler cannot require arbitrary services
class MyService extends Context.Service<MyService, { foo: string }>()("@test/MyService") {}

const _test1 = Machine.make({
  state: MyState,
  event: MyEvent,
  initial: MyState.Idle,
  // @ts-expect-error - Handler cannot require arbitrary services (MyService not in R=never)
}).on(MyState.Idle, MyEvent.Start, () =>
  Effect.gen(function* () {
    const svc = yield* MyService;
    return MyState.Loading({ url: svc.foo });
  }),
);

// Test 2: Handler cannot return wrong state
const WrongState = State({
  Other: {},
});

const _test2 = Machine.make({
  state: MyState,
  event: MyEvent,
  initial: MyState.Idle,
  // @ts-expect-error - Handler must return state from machine's schema
}).on(MyState.Idle, MyEvent.Start, () => WrongState.Other);

// Test 3: Handler cannot produce errors
class MyError extends Schema.TaggedError<MyError>()("MyError", {}) {}

const _test3 = Machine.make({
  state: MyState,
  event: MyEvent,
  initial: MyState.Idle,
  // @ts-expect-error - Handler cannot produce errors (MyError not assignable to never)
}).on(MyState.Idle, MyEvent.Start, () =>
  Effect.gen(function* () {
    return yield* new MyError({});
  }),
);

// Test 4: spawn handler CAN use Scope (for finalizers) - should compile
const _test4 = Machine.make({
  state: MyState,
  event: MyEvent,
  initial: MyState.Idle,
})
  .on(MyState.Idle, MyEvent.Start, () => MyState.Loading({ url: "/" }))
  .spawn(MyState.Loading, () => Effect.addFinalizer(() => Effect.log("cleanup")));

// Test 5: spawn handler can require a service, which propagates to Machine.spawn
const _test5 = Machine.make({
  state: MyState,
  event: MyEvent,
  initial: MyState.Idle,
})
  .on(MyState.Idle, MyEvent.Start, () => MyState.Loading({ url: "/" }))
  .spawn(MyState.Loading, () => MyService.pipe(Effect.asVoid));

const _test5RequiresService: Effect.Effect<unknown, never, MyService> = Machine.spawn(_test5);

// Test 6: task handler can require a service, which also propagates to Machine.spawn
const _test6 = Machine.make({
  state: MyState,
  event: MyEvent,
  initial: MyState.Loading({ url: "/" }),
}).task(MyState.Loading, () => MyService, {
  onSuccess: () => MyEvent.Complete,
});

const _test6RequiresService: Effect.Effect<unknown, never, MyService> = Machine.spawn(_test6);

// ============================================================================
// Reply Schema Type Constraints
// ============================================================================

const ReplyEvent = Event({
  GetCount: Event.reply({}, Schema.Number),
  GetName: Event.reply({}, Schema.String),
  Fire: {},
});

const ReplyState = State({
  Active: { count: Schema.Number },
  Done: {},
});

// Test 7: Handler for reply-bearing event MUST return Machine.reply()
const _test7 = Machine.make({
  state: ReplyState,
  event: ReplyEvent,
  initial: ReplyState.Active({ count: 0 }),
}).on(ReplyState.Active, ReplyEvent.GetCount, ({ state }) =>
  Machine.reply(ReplyState.Active({ count: state.count }), state.count),
);

// Test 8: Handler for reply-bearing event CANNOT return plain state
const _test8 = Machine.make({
  state: ReplyState,
  event: ReplyEvent,
  initial: ReplyState.Active({ count: 0 }),
  // @ts-expect-error - reply-bearing event requires Machine.reply(), not plain state
}).on(ReplyState.Active, ReplyEvent.GetCount, () => ReplyState.Active({ count: 0 }));

// Test 9: Handler for non-reply event CANNOT return Machine.reply()
const _test9 = Machine.make({
  state: ReplyState,
  event: ReplyEvent,
  initial: ReplyState.Active({ count: 0 }),
  // @ts-expect-error - non-reply event handler cannot return Machine.reply()
}).on(ReplyState.Active, ReplyEvent.Fire, () => Machine.reply(ReplyState.Done, 42));

// Test 10: Machine.reply() type must match schema
const _testReplyValue = Machine.make({
  state: ReplyState,
  event: ReplyEvent,
  initial: ReplyState.Active({ count: 0 }),
  // @ts-expect-error - reply type string doesn't match Schema.Number
}).on(ReplyState.Active, ReplyEvent.GetCount, ({ state }) =>
  Machine.reply(ReplyState.Active({ count: state.count }), "not a number"),
);

// Test 10b: reply-bearing constructors accept plain payload fields, not hidden reply metadata
const PayloadReplyEvent = Event({
  GetById: Event.reply({ id: Schema.String }, Schema.Number),
});
const _test9bPayload: Parameters<typeof PayloadReplyEvent.GetById>[0] = { id: "task-1" };
const _test9b = PayloadReplyEvent.GetById(_test9bPayload);
const _test9bId: string = _test9b.id;

// ============================================================================
// Slot Type Safety Regression Tests
// ============================================================================

const MySlots = Slot.define({
  canRetry: Slot.fn({ max: Schema.Number }, Schema.Boolean),
  fetchData: Slot.fn({ url: Schema.String }),
  computeValue: Slot.fn({ input: Schema.Number }, Schema.Number),
});
type MySlotsDef = typeof MySlots.definitions;

// Test 10: Slots are accessible via `slots` in handler context
const _test10 = Machine.make({
  state: MyState,
  event: MyEvent,
  slots: MySlots,
  initial: MyState.Idle,
}).on(MyState.Idle, MyEvent.Start, ({ slots }) =>
  Effect.gen(function* () {
    const canRetry = yield* slots.canRetry({ max: 3 });
    if (canRetry) {
      yield* slots.fetchData({ url: "/" });
    }
    return MyState.Loading({ url: "/" });
  }),
);

// Test 11: Slot call with wrong param type is rejected
const _test11 = Machine.make({
  state: MyState,
  event: MyEvent,
  slots: MySlots,
  initial: MyState.Idle,
}).on(MyState.Idle, MyEvent.Start, ({ slots }) =>
  Effect.gen(function* () {
    // @ts-expect-error - max should be number, not string
    yield* slots.canRetry({ max: "not a number" });
    return MyState.Loading({ url: "/" });
  }),
);

// Test 12: Slot return type is enforced
const _test12 = Machine.make({
  state: MyState,
  event: MyEvent,
  slots: MySlots,
  initial: MyState.Idle,
}).on(MyState.Idle, MyEvent.Start, ({ slots }) =>
  Effect.gen(function* () {
    // computeValue returns number, assigning to string should fail
    // @ts-expect-error - computeValue returns number, not string
    const _v: string = yield* slots.computeValue({ input: 42 });
    return MyState.Loading({ url: "/" });
  }),
);

// Test 13: ProvideSlots requires all slots to be implemented
// @ts-expect-error - missing 'computeValue' property
const _test13: ProvideSlots<MySlotsDef> = {
  canRetry: ({ max }) => max > 0,
  fetchData: ({ url }) => Effect.log(url),
};

// Test 14: ProvideSlots rejects wrong handler param types
const _test14: ProvideSlots<MySlotsDef> = {
  // @ts-expect-error - max should be number, handler expects string
  canRetry: ({ max }: { max: string }) => max.length > 0,
  fetchData: ({ url }) => Effect.log(url),
  computeValue: ({ input }) => input * 2,
};

// Test 15: ProvideSlots accepts valid implementations (should compile)
const _test15: ProvideSlots<MySlotsDef> = {
  canRetry: ({ max }) => max > 0,
  fetchData: ({ url }) => Effect.log(url),
  computeValue: ({ input }) => input * 2,
};

// Test 16: Machine without slots — handler context has empty slots
const _test16 = Machine.make({
  state: MyState,
  event: MyEvent,
  initial: MyState.Idle,
}).on(MyState.Idle, MyEvent.Start, ({ slots }) => {
  // @ts-expect-error - no slots defined, canRetry doesn't exist
  const _x = slots.canRetry;
  return MyState.Loading({ url: "/" });
});

// This file should compile with all @ts-expect-error comments being valid
export {};
