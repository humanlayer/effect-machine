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

The builder also supports `.spawn(...)` for long-lived entry effects, `.background(...)` for actor-lifetime effects, `.timeout(...)`, `.postpone(...)`, and `.reenter(...)`.

## Services And Layers

State effects use normal Effect services. Requirements from `.task()`, `.spawn()`, and `.background()` are inferred by the machine and required when it runs. Provide implementations with an Effect `Layer`; no per-actor slot map is needed.

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

Transition handlers remain pure and cannot require services or fail. Use a state effect to perform I/O, then map its result to an event with `.task()`. For continuously running work that can send many events, use `.spawn()` and `ctx.self.send(...)`.

## Running Actors

`Machine.spawn` allocates an actor but does not start it. Call `actor.start` to fork the event loop, background effects, and spawn effects. Events sent before `start()` are queued.

Key actor operations:

- `start` forks the event loop and entry effects
- `send(event)` queues and returns immediately
- `call(event)` returns full transition info
- `ask(event)` returns a typed domain reply from `Event.reply(...)`
- `waitFor(...)` and `awaitFinal` coordinate with state changes
- `stop` interrupts now; `drain` processes remaining queued events first

For named actors or shared lookup, use an actor system. `system.spawn` auto-starts the actor:

```ts
import { ActorSystemDefault, ActorSystemService } from "@humanlayer/effect-machine";

const program = Effect.gen(function* () {
  const system = yield* ActorSystemService;
  const actor = yield* system.spawn("checkout-123", checkoutMachine);
  yield* actor.send(CheckoutEvent.Submit);
}).pipe(Effect.provide(ActorSystemDefault), Effect.provide(PaymentsLive));
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

## Cluster

Run the same machine behind `@effect/cluster`:

```ts
import { EntityMachine, toEntity } from "@humanlayer/effect-machine/cluster";

const CheckoutEntity = toEntity(checkoutMachine, { type: "Checkout" });

const CheckoutEntityLayer = EntityMachine.layer(CheckoutEntity, checkoutMachine, {
  initializeState: (entityId) => CheckoutState.ReviewingCart({ cartId: entityId, totalCents: 0 }),
  persistence: { strategy: "journal" },
});
```

Persistence strategies:

- **Snapshot** saves state periodically and restores it on reactivation.
- **Journal** appends each RPC event and replays it on reactivation.

## License

MIT
