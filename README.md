# @humanlayer/effect-machine

Type-safe state machines and actors for [Effect](https://effect.website).

Complex workflows usually fail the same way: one `status` field, a few side booleans, and effects scattered across callbacks. `@humanlayer/effect-machine` gives you one typed model for state, events, and transitions, then runs it as a real actor.

Use it when a feature has:

- multiple valid and invalid states
- async work tied to state entry
- retries, timeouts, cancellation, or backpressure
- logic you want to reuse in-process, in tests, and in distributed systems

## Install

```bash
bun add @humanlayer/effect-machine effect
```

`effect` is a peer dependency. This package is validated against the latest Effect v4 beta.

## Core Pattern

States and events are schemas. Types, validation, and serialization come from one place.

```ts
import { Cause, Context, Effect, Layer, Schema } from "effect";
import { Event, Machine, State } from "@humanlayer/effect-machine";

class Payments extends Context.Service<
  Payments,
  {
    readonly charge: (
      cartId: string,
      totalCents: number,
    ) => Effect.Effect<{ readonly receiptId: string }, Error>;
  }
>()("@app/Payments") {}

const CheckoutState = State({
  ReviewingCart: { cartId: Schema.String, totalCents: Schema.Number },
  ChargingCard: { cartId: Schema.String, totalCents: Schema.Number },
  Confirmed: { cartId: Schema.String, receiptId: Schema.String },
  Failed: { cartId: Schema.String, reason: Schema.String },
});

const CheckoutEvent = Event({
  Submit: {},
  Charged: { receiptId: Schema.String },
  Declined: { reason: Schema.String },
  Cancel: {},
});

const checkoutMachine = Machine.make({
  state: CheckoutState,
  event: CheckoutEvent,
  initial: CheckoutState.ReviewingCart({ cartId: "cart_123", totalCents: 4200 }),
})
  .on(CheckoutState.ReviewingCart, CheckoutEvent.Submit, ({ state }) =>
    CheckoutState.ChargingCard.with(state),
  )
  .task(
    CheckoutState.ChargingCard,
    ({ state }) =>
      Effect.gen(function* () {
        const payments = yield* Payments;
        return yield* payments.charge(state.cartId, state.totalCents);
      }),
    {
      onSuccess: (result) => CheckoutEvent.Charged({ receiptId: result.receiptId }),
      onFailure: (cause) => CheckoutEvent.Declined({ reason: Cause.pretty(cause) }),
    },
  )
  .on(CheckoutState.ChargingCard, CheckoutEvent.Charged, ({ state, event }) =>
    CheckoutState.Confirmed.with(state, { receiptId: event.receiptId }),
  )
  .on(CheckoutState.ChargingCard, CheckoutEvent.Declined, ({ state, event }) =>
    CheckoutState.Failed.with(state, { reason: event.reason }),
  )
  .onAny(CheckoutEvent.Cancel, ({ state }) =>
    CheckoutState.Failed.with(state, { reason: "cancelled" }),
  )
  .final(CheckoutState.Confirmed)
  .final(CheckoutState.Failed);
```

A few things to notice:

- Empty variants are values: `State.Idle`. Non-empty variants are constructors: `State.Loading({ url })`.
- `State.with(source, overrides)` carries overlapping fields forward without manual copying.
- `.onAny(...)` is a fallback; a specific `.on(...)` wins.
- `.task(...)` runs work on state entry, sends mapped completion events, and cancels work on state exit.

## Transitions And Effects

The fluent builder keeps state behavior beside the transitions that make it relevant:

- `.on([State.Draft, State.Review], Event.Cancel, handler)` registers one transition for multiple states; `.from(state, scope => ...)` groups transitions by source state.
- `.reenter(...)` runs state lifecycle again when a transition keeps the same state tag. Ordinary same-state transitions do not restart state effects or timers.
- `.spawn(state, handler)` forks state-scoped work that is interrupted on state exit. `.background(handler)` runs for the actor lifetime.
- `.timeout(state, { duration, event })` starts a state-scoped timer; leaving the state cancels it. Both `duration` and `event` can derive from the entered state.
- `.postpone(state, event)` buffers matching events and drains them in FIFO order after the next state-tag change.

Use `self.send(...)` from a state effect to feed work back into the machine. State effects can use Effect services and can be asynchronous; transition handlers stay pure.

Methods that can add Effect requirements—`.spawn(...)`, `.task(...)`, `.timeout(...)`, and `.background(...)`—are copy-on-write. Always use their returned machine. This keeps an earlier alias truthful and unchanged:

```ts
const base = Machine.make({ state, event, initial });
const withWorker = base.spawn(State.Running, worker);

base.spawnEffects.length; // 0
withWorker.spawnEffects.length; // 1
```

State-effect contexts expose an honest lifecycle event union: initial effects receive `$init`, while effects started after a state transition receive `$enter`.

## Services And Layers

New machines use Effect's service system for dependencies, not actor-local slot maps. Define a dependency with `Context.Service` (the Effect v4 replacement for `ServiceMap.Service`), access it with `yield*` inside a state effect, and provide an implementation with a `Layer` at the program boundary.

Requirements from `.task()`, `.spawn()`, and `.background()` are inferred by the machine and flow through `Machine.spawn`, `system.spawn`, and `EntityMachine.layer`. Transition handlers remain pure: they cannot require services or fail. Move I/O into a state effect and communicate its outcome with an event.

```ts
const PaymentsLive = Layer.succeed(Payments, {
  charge: (cartId, totalCents) => Effect.succeed({ receiptId: `rcpt_${cartId}_${totalCents}` }),
});

const program = Effect.gen(function* () {
  const actor = yield* Machine.spawn(checkoutMachine);
  yield* actor.start;
  yield* actor.send(CheckoutEvent.Submit);
  return yield* actor.awaitFinal;
}).pipe(Effect.provide(PaymentsLive));
```

This also makes testing conventional Effect code: provide a test layer around the actor program. `simulate` and `createTestHarness` do not run state effects, so they do not require their services.

### Migrating From Slots

`Slot`, `Machine.make({ slots })`, handler `({ slots })`, and `{ slots }` spawn options remain as deprecated compatibility APIs. Use them only while migrating an existing machine; they are not the DI mechanism for new code.

| Legacy slot pattern                     | Effect service replacement                                               |
| --------------------------------------- | ------------------------------------------------------------------------ |
| `Slot.define({ charge: Slot.fn(...) })` | `class Payments extends Context.Service<...>()("@app/Payments") {}`      |
| `Machine.make({ ..., slots })`          | Read the service in `.task(...)`, `.spawn(...)`, or `.background(...)`   |
| `Machine.spawn(machine, { slots })`     | `Machine.spawn(machine).pipe(Effect.provide(PaymentsLive))`              |
| `system.spawn(id, machine, { slots })`  | Provide `PaymentsLive` around the program that calls `system.spawn(...)` |

Legacy slot handlers must still be supplied explicitly at every execution boundary that uses them, such as `Machine.spawn`, `system.spawn`, `simulate`, `createTestHarness`, and `Machine.replay`. Their dependencies are not inferred through the machine type, so migrate them to Effect services when possible.

## Request And Reply

Declare a reply schema on an event to make it valid for `actor.ask(...)`. Its transition returns `Machine.reply(nextState, value)`, so the reply type is inferred from the schema.

```ts
const ReceiptEvent = Event({
  GetReceipt: Event.reply({}, Schema.String),
  Cancel: {},
});

const machine = Machine.make({
  state: CheckoutState,
  event: ReceiptEvent,
  initial: CheckoutState.Confirmed({ cartId: "cart_123", receiptId: "rcpt_123" }),
})
  .on(CheckoutState.Confirmed, ReceiptEvent.GetReceipt, ({ state }) =>
    Machine.reply(state, state.receiptId),
  )
  .onAny(ReceiptEvent.Cancel, ({ state }) =>
    CheckoutState.Failed.with(state, { reason: "cancelled" }),
  );

const receiptId = yield * actor.ask(ReceiptEvent.GetReceipt);
```

`ask` fails with `NoReplyError` when a handler does not reply and `ActorStoppedError` when the actor stops first. For a reply produced later by state work, return `Machine.deferReply(state)` from the transition and call `self.reply(value)` from a `.spawn(...)` handler.

## Running Actors

`Machine.spawn` allocates an actor but does not start it. Call `actor.start` to fork the event loop, background effects, and spawn effects. Events sent before `start()` are queued.

Key actor operations:

- `start` forks the event loop and entry effects
- `send(event)` queues and returns immediately
- `call(event)` returns full transition info
- `ask(event)` returns a typed domain reply from `Event.reply(...)`
- `waitFor(...)` and `awaitFinal` coordinate with state changes
- `stop` interrupts now; `drain` processes remaining queued events first
- `snapshot`, `matches`, and `can` inspect the current actor state
- `changes` and `transitions` expose state and transition streams; `subscribe` provides a synchronous listener

Use `Machine.scoped` when a local actor should stop with an Effect scope. The scope bridge is explicit, so unrelated scopes never stop an actor by accident.

```ts
const program = Effect.scoped(
  Machine.scoped(
    Effect.gen(function* () {
      const actor = yield* Machine.spawn(checkoutMachine);
      yield* actor.start;
      yield* actor.send(CheckoutEvent.Submit);
      return yield* actor.awaitFinal;
    }),
  ),
);
```

For named actors or shared lookup, use an actor system. `system.spawn` auto-starts the actor:

```ts
import { ActorSystemDefault, ActorSystemService } from "@humanlayer/effect-machine";

const program = Effect.gen(function* () {
  const system = yield* ActorSystemService;
  const actor = yield* system.spawn("checkout-123", checkoutMachine);
  yield* actor.send(CheckoutEvent.Submit);
}).pipe(Effect.provide(ActorSystemDefault), Effect.provide(PaymentsLive));
```

`ActorSystemService` also exposes `get(id)`, `stop(id)`, a snapshot `actors` map, an event `Stream`, and `subscribe(...)` for synchronous `ActorSpawned`, `ActorRestarted`, and `ActorStopped` notifications. Typed `spawn` returns a full `ActorRef<State, Event>`. Heterogeneous lookups, maps, child collections, and system events expose `ActorHandle`, which supports lifecycle and read-only observation but cannot accept an event without a type witness.

## Recovery, Durability, And Supervision

Local actors can resolve an initial state during `start()` and persist committed transitions with lifecycle hooks. `hydrate` takes precedence over recovery, which is useful when a caller already has authoritative state.

```ts
import { Option } from "effect";
import { Supervision } from "@humanlayer/effect-machine";

const actor =
  yield *
  Machine.spawn(checkoutMachine, {
    lifecycle: {
      recovery: {
        resolve: ({ actorId }) =>
          storage.load(actorId).pipe(Effect.map(Option.fromNullable), Effect.orDie),
      },
      durability: {
        save: ({ actorId, nextState, event }) =>
          storage.save(actorId, nextState, event).pipe(Effect.orDie),
        shouldSave: (nextState, previousState) => nextState._tag !== previousState._tag,
      },
    },
    supervision: Supervision.restart({ maxRestarts: 3, within: "1 minute" }),
  });
yield * actor.start;
```

Recovery receives the actor ID, generation, and machine initial state, and returns `Option<State>`. Durability runs after a transition commits and receives both states, the event, actor ID, and generation. A supervised defect restarts from recovered state when available, otherwise from `machine.initial`; explicit `stop`, `drain`, and final states are terminal. Use `actor.awaitExit` to observe a terminal `Final`, `Stopped`, or `Defect` exit.

## Child Actors

State effects can create children with `self.spawn(id, machine)`. Children created by `.spawn(...)` are state-scoped and automatically stop when their parent leaves that state.

```ts
const parentMachine = Machine.make({
  state: ParentState,
  event: ParentEvent,
  initial: ParentState.Idle,
})
  .on(ParentState.Idle, ParentEvent.Start, () => ParentState.Running)
  .spawn(ParentState.Running, ({ self }) =>
    Effect.gen(function* () {
      const child = yield* self.spawn("worker-1", workerMachine).pipe(Effect.orDie);
      yield* child.send(WorkerEvent.Start);
    }),
  );
```

Use `.background(...)` when a child should live for the whole actor lifetime. Each actor exposes its direct children through `actor.children` and can look up actors through `actor.system`.

## Inspection

Provide the optional `InspectorService` to observe actor spawn, received events, transitions, tasks, effects, defects, and stops. Built-ins include `consoleInspector`, `collectingInspector`, and `tracingInspector`.

```ts
import { consoleInspector, InspectorService } from "@humanlayer/effect-machine";

const program = Effect.gen(function* () {
  const actor = yield* Machine.spawn(checkoutMachine);
  yield* actor.start;
  yield* actor.send(CheckoutEvent.Submit);
}).pipe(Effect.provideService(InspectorService, consoleInspector()));
```

## Testing

Test state transitions without starting an actor:

```ts
import { simulate } from "@humanlayer/effect-machine";

const result =
  yield *
  simulate(checkoutMachine, [
    CheckoutEvent.Submit,
    CheckoutEvent.Charged({ receiptId: "rcpt_123" }),
  ]);

expect(result.states.map((state) => state._tag)).toEqual([
  "ReviewingCart",
  "ChargingCard",
  "Confirmed",
]);
```

`simulate` and `createTestHarness` run transition logic but do not run `.task()`, `.spawn()`, or `.background()` effects.

For focused assertions, `assertPath`, `assertReaches`, and `assertNeverReaches` are built on the same simulation model. Test state effects with a real actor instead.

## Cluster

Run the same machine behind `@effect/cluster`:

```ts
import { EntityMachine, toEntity } from "@humanlayer/effect-machine/cluster";

const CheckoutEntity = toEntity(checkoutMachine, { type: "Checkout" });

const CheckoutEntityLayer = EntityMachine.layer(CheckoutEntity, {
  initializeState: (entityId) => CheckoutState.ReviewingCart({ cartId: entityId, totalCents: 0 }),
  persistence: { strategy: "journal" },
});
```

`toEntity` requires a machine made with `Machine.make({ state, event, initial })`, then returns a machine-owned entity with canonical `Send`, `Ask`, `GetState`, and `WatchState` RPCs. `EntityMachine.layer(entity, options?)` uses the machine carried by that entity, preventing protocol/machine mismatches. `makeEntityActorRef(entity, client, entityId)` wraps the protocol with typed `send`, `ask`, `snapshot`, `watch`, and `waitFor`; remote Ask values are decoded with the event's reply schema and client transport errors remain in each operation's error channel.

Persistence is opt-in and resolves `PersistenceAdapter` from the entity layer's services:

- **Snapshot** is the default. It saves on each state change unless `snapshotSchedule` controls the cadence, then restores on reactivation.
- **Journal** appends every `Send` and `Ask` event inline, replays events after the latest snapshot, and saves a snapshot when the entity deactivates.

Entity options also include `maxIdleTime`, `mailboxCapacity`, `defectRetryPolicy`, and `disableFatalDefects`, which are forwarded to `@effect/cluster`.

## License

MIT
