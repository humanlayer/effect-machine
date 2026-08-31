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
import { Machine, State, Event } from "../src/index.js";
import type { ActorHandle } from "../src/index.js";

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

type EffectRequirements<Value> =
  Value extends Effect.Effect<infer _A, infer _E, infer R> ? R : never;
type Assert<Condition extends true> = Condition;

const _test5Spawn = Machine.spawn(_test5);
type _Test5RequiresService = Assert<
  MyService extends EffectRequirements<typeof _test5Spawn> ? true : false
>;

// Test 6: task handler can require a service, which also propagates to Machine.spawn
const _test6 = Machine.make({
  state: MyState,
  event: MyEvent,
  initial: MyState.Loading({ url: "/" }),
}).task(MyState.Loading, () => MyService, {
  onSuccess: () => MyEvent.Complete,
});

const _test6Spawn = Machine.spawn(_test6);
type _Test6RequiresService = Assert<
  MyService extends EffectRequirements<typeof _test6Spawn> ? true : false
>;

// Test 6b: requirement-growing methods do not mutate the type of an earlier alias
const _test6bBase = Machine.make({
  state: MyState,
  event: MyEvent,
  initial: MyState.Idle,
});
const _test6bWithService = _test6bBase.background(() => MyService.pipe(Effect.asVoid));
const _test6bBaseSpawn = Machine.spawn(_test6bBase);
const _test6bWithServiceSpawn = Machine.spawn(_test6bWithService);
type _Test6bBaseStillServiceFree = Assert<
  MyService extends EffectRequirements<typeof _test6bBaseSpawn> ? false : true
>;
type _Test6bRequiresService = Assert<
  MyService extends EffectRequirements<typeof _test6bWithServiceSpawn> ? true : false
>;

// Heterogeneous lookup is intentionally eventless; exact spawn results remain ActorRef.
const _actorHandleCannotSend = (handle: ActorHandle) => {
  // @ts-expect-error - ActorHandle has no event type witness
  handle.send(MyEvent.Start);
};

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

// Test 7b: onAny supports reply-bearing events with the same contract as on
const _test7b = Machine.make({
  state: ReplyState,
  event: ReplyEvent,
  initial: ReplyState.Active({ count: 0 }),
}).onAny(ReplyEvent.GetCount, ({ state }) =>
  Machine.reply(state, state._tag === "Active" ? state.count : 0),
);

// Test 7c: onAny requires Machine.reply() for reply-bearing events
const _test7c = Machine.make({
  state: ReplyState,
  event: ReplyEvent,
  initial: ReplyState.Active({ count: 0 }),
  // @ts-expect-error - reply-bearing onAny handler requires Machine.reply()
}).onAny(ReplyEvent.GetCount, ({ state }) => state);

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

// This file should compile with all @ts-expect-error comments being valid
export {};
