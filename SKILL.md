# @humanlayer/effect-machine Skill

Quick reference for AI agents working with @humanlayer/effect-machine.

## What It Is

Type-safe state machines for Effect. Schema-first API.

## Core Pattern

```ts
import { Machine, State, Event } from "@humanlayer/effect-machine";

// 1. Define schemas
const MyState = State({
  Idle: {},
  Loading: { url: Schema.String },
  Done: { data: Schema.Unknown },
});

const MyEvent = Event({
  Start: { url: Schema.String },
  Complete: { data: Schema.Unknown },
});

// 2. Build machine
const machine = Machine.make({
  state: MyState,
  event: MyEvent,
  initial: MyState.Idle,
})
  .on(MyState.Idle, MyEvent.Start, ({ event }) => MyState.Loading({ url: event.url }))
  .on(MyState.Loading, MyEvent.Complete, ({ event }) => MyState.Done({ data: event.data }))
  .final(MyState.Done);
```

## Key Methods

| Method                                 | Purpose                                 |
| -------------------------------------- | --------------------------------------- |
| `.on(state, event, handler)`           | Add transition                          |
| `.on([stateA, stateB], event, h)`      | Multi-state transition                  |
| `.onAny(event, handler)`               | Wildcard (any state, specific .on wins) |
| `.reenter(state, event, handler)`      | Force lifecycle on same-state           |
| `.spawn(state, handler)`               | State-scoped effect (auto-cancelled)    |
| `.timeout(state, { duration, event })` | State timeout (gen_statem)              |
| `.postpone(state, event/events)`       | Postpone event in state (gen_statem)    |
| `.background(handler)`                 | Machine-lifetime effect                 |
| `.final(state)`                        | Mark final state                        |

## State.with()

Construct state from existing source:

```ts
// Per-variant: preserve fields, override specific ones
State.Active.with(state, { count: state.count + 1 });

// Cross-state: picks only target fields
State.Shipped.with(processingState, { trackingId: "TRACK-123" });

// Empty variant
State.Idle.with(anyState); // -> { _tag: "Idle" }

// Union-level: dispatches to correct variant based on _tag
// Preserves specific variant subtype — no switch needed
const updated = MyState.with(state, { queue: newQueue });
```

## Services And Layers

```ts
import { Context, Effect, Layer } from "effect";

class Fetcher extends Context.Service<
  Fetcher,
  { readonly fetch: (url: string) => Effect.Effect<string> }
>()("@app/Fetcher") {}

const machine = Machine.make({ state, event, initial })
  .on(State.X, Event.Start, () => State.Loading)
  .task(
    State.Loading,
    () =>
      Effect.gen(function* () {
        const fetcher = yield* Fetcher;
        return yield* fetcher.fetch("/api");
      }),
    { onSuccess: (data) => Event.Loaded({ data }) },
  );

const FetcherLive = Layer.succeed(Fetcher, {
  fetch: (url) => Http.get(url),
});

const actor = yield * Machine.spawn(machine).pipe(Effect.provide(FetcherLive));
```

Requirements from `.task()`, `.spawn()`, and `.background()` are inferred by the machine. Transition handlers remain pure; map I/O results back into the machine with events.

## Running Actors

**Simple (no registry):**

```ts
const program = Effect.gen(function* () {
  const actor = yield* Machine.spawn(machine);
  yield* actor.send(Event.Start({ url: "/api" }));
  const state = yield* actor.waitFor(MyState.Done);
});

Effect.runPromise(Effect.scoped(program));
```

**With registry/persistence:**

```ts
const program = Effect.gen(function* () {
  const system = yield* ActorSystemService;
  const actor = yield* system.spawn("id", machine);
  // ...
});

Effect.runPromise(Effect.scoped(program.pipe(Effect.provide(ActorSystemDefault))));
```

## ActorRef API

| Method                           | Description                                 |
| -------------------------------- | ------------------------------------------- |
| `actor.send(event)`              | Fire-and-forget (queue event)               |
| `actor.cast(event)`              | Alias for send (OTP gen_server:cast)        |
| `actor.call(event)`              | Request-reply, returns `ProcessEventResult` |
| `actor.ask(event)`               | Typed reply (event must have `Event.reply`) |
| `actor.waitFor(State.X)`         | Wait for state (constructor or fn)          |
| `actor.sendAndWait(ev, State.X)` | Send + wait for state                       |
| `actor.awaitFinal`               | Wait for final state                        |
| `actor.watch(other)`             | Completes when other actor stops            |
| `actor.drain`                    | Process remaining queue, then stop          |
| `actor.snapshot`                 | Get current state                           |
| `actor.sync.send(event)`         | Sync fire-and-forget (for UI)               |
| `actor.sync.stop()`              | Sync stop                                   |
| `actor.sync.snapshot()`          | Sync get state                              |
| `actor.sync.matches(tag)`        | Sync check state tag                        |
| `actor.sync.can(event)`          | Sync can handle event?                      |
| `actor.subscribe(fn)`            | Sync callback, returns unsubscribe          |
| `actor.system`                   | Access the actor's `ActorSystem`            |
| `actor.children`                 | Child actors (`ReadonlyMap`)                |

## ask / reply

Events declare reply schemas via `Event.reply()`. Handlers use `Machine.reply()`:

```ts
const MyEvent = Event({
  GetCount: Event.reply({}, Schema.Number),  // askable
  Reset: {},                                  // not askable
});

.on(State.Active, Event.GetCount, ({ state }) =>
  Machine.reply(state, state.count),
)

const count = yield* actor.ask(Event.GetCount);  // number — type inferred from schema
// actor.ask(Event.Reset) — compile error (no reply schema)
```

Deferred replies via `Machine.deferReply()` — spawn handler settles later via `self.reply(value)`.

Fails with `NoReplyError` if handler doesn't reply, `ActorStoppedError` on stop.

## Timeout & Postpone

```ts
// State timeout — timer auto-cancelled on state exit
machine.timeout(State.Loading, {
  duration: Duration.seconds(30),
  event: Event.Timeout,
});

// Event postpone — buffered, drained on next state change
machine.postpone(State.Connecting, [Event.Data, Event.Cmd]);
```

## ProcessEventResult

Returned by `actor.call(event)`:

```ts
interface ProcessEventResult<S> {
  newState: S;
  previousState: S;
  transitioned: boolean;
  lifecycleRan: boolean;
  isFinal: boolean;
  hasReply: boolean;
  reply?: unknown;
  postponed: boolean;
}
```

## System Observation

```ts
// Sync callback — ActorSpawned / ActorStopped events
const unsub = system.subscribe((event) => console.log(event._tag, event.id));

// Sync snapshot of all registered actors
const actors: ReadonlyMap<string, ActorRef> = system.actors;

// Async stream (late subscribers miss prior events)
system.events.pipe(Stream.take(10), Stream.runCollect);
```

## Testing

```ts
// Simulate (no spawn effects)
const result = yield * simulate(machine, [Event.Start, Event.Complete]);
expect(result.finalState._tag).toBe("Done");

// Assert path
yield * assertPath(machine, events, ["Idle", "Loading", "Done"]);

// Real actor — call-based testing
const actor = yield * Machine.spawn(machine);
const result = yield * actor.call(Event.Start);
expect(result.transitioned).toBe(true);
expect(result.newState._tag).toBe("Loading");
```

## Critical Gotchas

1. **Empty structs are values**: `State.Idle` not `State.Idle()`
2. **yield after send**: `yield* Effect.yieldNow` to process events
3. **simulate skips spawn**: Use real actors for spawn effect tests
4. **Same-state skips lifecycle**: Use `.reenter()` to force
5. **Never throw in Effect.gen**: Use `yield* Effect.fail()`
6. **`.onAny()` is fallback**: Specific `.on()` always takes priority
7. **Provide services with layers**: `Machine.spawn(machine).pipe(Effect.provide(MyLayer))`
8. **call vs send**: `send`/`cast` = fire-and-forget, `call` = request-reply, `ask` = typed reply
9. **Sync helpers**: Use `actor.sync.*` (not top-level `sendSync`/`snapshotSync`)
10. **ActorStoppedError**: Pending `call`/`ask` Deferreds settled on stop

## Cluster / Entity Machines

Wire machines to `@effect/cluster` for distributed actors:

```ts
import { toEntity, EntityMachine, PersistenceAdapter } from "@humanlayer/effect-machine/cluster";

const OrderEntity = toEntity(orderMachine, { type: "Order" });

const OrderEntityLayer = EntityMachine.layer(OrderEntity, orderMachine, {
  initializeState: (entityId) => OrderState.Pending({ orderId: entityId }),
  persistence: { strategy: "journal" }, // or "snapshot" (default)
});
```

| Export                                        | Purpose                                                             |
| --------------------------------------------- | ------------------------------------------------------------------- |
| `toEntity(machine, { type })`                 | Generate `Entity` definition with Send/Ask/GetState/WatchState RPCs |
| `EntityMachine.layer(entity, machine, opts?)` | Wire machine to cluster Entity layer                                |
| `makeEntityActorRef(client, id)`              | Typed client wrapper (send/ask/snapshot/watch/waitFor)              |
| `PersistenceAdapter`                          | Service tag for storage backend                                     |
| `makeInMemoryPersistenceAdapter`              | In-memory adapter for testing                                       |

**Persistence strategies:**

- **snapshot**: background scheduler + deactivation finalizer. No journal.
- **journal**: inline event append on each RPC, replay on reactivation. Deactivation snapshot as fallback.

**EntityMachineOptions:** `initializeState`, `hooks`, `maxIdleTime`, `mailboxCapacity`, `disableFatalDefects`, `defectRetryPolicy`, `persistence`

## Files

| File                            | Purpose                                |
| ------------------------------- | -------------------------------------- |
| `machine.ts`                    | Machine builder                        |
| `schema.ts`                     | State/Event + with                     |
| `actor.ts`                      | ActorSystem, event loop                |
| `testing.ts`                    | simulate, harness                      |
| `internal/runtime.ts`           | Shared runtime kernel (entity-machine) |
| `cluster/entity-machine.ts`     | Entity-machine adapter + persistence   |
| `cluster/persistence.ts`        | Adapter interface, types, service tag  |
| `cluster/adapters/in-memory.ts` | In-memory persistence adapter          |
| `cluster/entity-actor-ref.ts`   | Typed entity client wrapper            |
| `cluster/to-entity.ts`          | Entity definition generator            |
