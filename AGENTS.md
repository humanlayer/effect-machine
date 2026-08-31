# @humanlayer/effect-machine

Type-safe state machines for Effect.

## Commands

```bash
bun run gate          # typecheck + lint + test + build
bun test              # Run tests
bun run typecheck     # tsgo --noEmit, patched by @effect/tsgo
bun run lint          # type-aware oxlint; Effect diagnostics run through tsgo plugin
bun run fmt           # oxfmt
```

## Conventions

- Files: kebab-case (`actor.ts`, `persistent-actor.ts`)
- States/Events: schema-first with `State({...})` / `Event({...})` - they ARE schemas
- Empty structs: plain values - `State.Idle` (not callable)
- Non-empty: `State.Loading({ url })` - constructor requiring args
- Machine creation: `Machine.make({ state, event, initial })` - types inferred
- Exports: all public API via `src/index.ts`
- Namespace pattern: `import { Machine } from "@humanlayer/effect-machine"` then `Machine.make`, etc.
- Services use `class X extends Context.Service<X, Shape>()("key") {}`; keep `serviceNotAsClass` enabled.
- `effect` stays peer-only at runtime; keep a concrete Effect version in dev deps for validation.

## Fluent Builder

```ts
const machine = Machine.make({ state, event, initial })
  .on(State.Idle, Event.Start, () => State.Running)
  .on([State.Draft, State.Review], Event.Cancel, () => State.Cancelled) // multi-state
  .onAny(Event.Reset, () => State.Idle) // wildcard (any state)
  .spawn(State.Running, ({ self }) => poll().pipe(Effect.andThen(self.send(Event.Polled))))
  .timeout(State.Loading, { duration: Duration.seconds(30), event: Event.Timeout })
  .postpone(State.Connecting, Event.Data)
  .final(State.Done);
```

- Transition/final/postpone methods mutate `this`; requirement-growing `.spawn()`, `.task()`, `.timeout()`, and `.background()` methods are copy-on-write
- Builder chain ends naturally — no terminal method needed
- `.onAny()` fires when no specific `.on()` matches for that event

## .task()

Async work that emits an event on completion:

```ts
// Explicit onSuccess mapping
.task(State.Loading, ({ state }) => fetchData(state.url), {
  onSuccess: (data) => Event.Loaded({ data }),
  onFailure: (cause) => Event.Failed({ error: Cause.pretty(cause) }),
})

// Shorthand — when task returns Event directly, onSuccess can be omitted
.task(State.Loading, ({ state }) => fetchData(state.url).pipe(Effect.map(d => Event.Loaded({ data: d }))), {
  onFailure: (cause) => Event.Failed({ error: Cause.pretty(cause) }),
})

// Multi-state
.task([State.Loading, State.Retrying], ({ state }) => fetchData(state.url), { onSuccess: ... })
```

## State.with()

Construct state from existing source. Per-variant and union-level:

```ts
// Per-variant: target-specific, works cross-state
State.Active.with(state, { count: state.count + 1 });
State.Shipped.with(processingState, { trackingId: "TRACK-123" });
State.Idle.with(anyState); // -> { _tag: "Idle" }

// Union-level: dispatches by _tag, preserves specific variant subtype
const updated = MyState.with(state, { queue: newQueue });
// If state is Streaming, returns Streaming (not union type)
// Partial keys not in target variant are silently dropped
```

## Services And Layers

State effects use normal Effect services. Requirements from `.task()`, `.spawn()`, and `.background()` are inferred by the machine and supplied when it runs. Transition handlers remain pure.

```ts
class ReplicationOperations extends Context.Service<
  ReplicationOperations,
  { readonly open: () => Effect.Effect<void> }
>()("@app/ReplicationOperations") {}

const machine = Machine.make({ state, event, initial })
  .on(State.Idle, Event.Start, () => State.Connecting)
  .task(
    State.Connecting,
    () =>
      Effect.gen(function* () {
        const operations = yield* ReplicationOperations;
        yield* operations.open();
      }),
    { onSuccess: () => Event.Connected },
  );

const OperationsLive = Layer.succeed(ReplicationOperations, { open: () => Effect.void });
const actor = yield * Machine.spawn(machine).pipe(Effect.provide(OperationsLive));
yield * actor.start;
```

## Running Machines

**Simple (no registry):**

`Machine.spawn` returns an **unstarted** actor. Call `actor.start` to fork the event loop.

```ts
const actor = yield * Machine.spawn(machine);
yield * actor.start; // fork event loop, background effects, spawn effects
yield * actor.stop; // caller responsible

// Scope-aware — use Machine.scoped to bridge ActorScope from Scope:
yield *
  Effect.scoped(
    Machine.scoped(
      Effect.gen(function* () {
        const actor = yield* Machine.spawn(machine);
        yield* actor.start;
        // actor.stop called automatically when scope closes
      }),
    ),
  );
```

**With registry:**

`system.spawn` auto-starts — no `actor.start` needed.

```ts
const system = yield * ActorSystemService;
const actor = yield * system.spawn("my-id", machine);
```

**ActorScope:** `Machine.spawn` and `system.spawn` detect `ActorScope` via `Effect.serviceOption` — if present, attach stop finalizer; if absent, skip. Use `Machine.scoped(effect)` to bridge `Scope.Scope` → `ActorScope`. This is explicit opt-in — ambient `Scope.Scope` does NOT trigger auto-cleanup (prevents bugs where unrelated scopes tear down actors).

## Recovery + Durability

Lifecycle hooks for persistence. Replace the old `PersistConfig`.

```ts
const actor =
  yield *
  Machine.spawn(machine, {
    lifecycle: {
      recovery: {
        resolve: ({ actorId, generation, machineInitial }) =>
          storage.get(actorId).pipe(Effect.map(Option.fromNullable)),
      },
      durability: {
        save: ({ actorId, nextState, event }) => storage.set(actorId, nextState),
        shouldSave: (state, prev) => state._tag !== prev._tag,
      },
    },
  });
yield * actor.start;
```

- **Recovery** — resolves initial state per generation. Runs during `actor.start`. Returns `Option<S>`: `Some` overrides initial, `None` uses `machine.initial`. `generation` is 0 for cold start, 1+ for supervision restarts.
- **Durability** — saves state after committed transitions. Receives `DurabilityCommit` with actorId, generation, previousState, nextState, event. `shouldSave` is a sync predicate to skip uninteresting transitions.
- **`hydrate` overrides recovery** — `Machine.spawn(machine, { hydrate: state })` skips `recovery.resolve` entirely.

## Supervision

Actors can automatically restart on defect with `Supervision.restart()`:

```ts
import { Supervision } from "@humanlayer/effect-machine";

const actor =
  yield *
  Machine.spawn(machine, {
    supervision: Supervision.restart({ maxRestarts: 3, within: "1 minute" }),
  });
yield * actor.start;

// Via system (auto-starts)
const actor =
  yield *
  system.spawn("id", machine, {
    supervision: Supervision.restart(),
  });

// Observe exit reason
const exit = yield * actor.awaitExit; // ActorExit<S>
const exit = yield * actor.watch(other); // ActorExit<unknown>
```

- **Restart from `machine.initial`** — always clean slate, never last-state
- **Actor ID survives** — same identity across restarts
- **Pending requests fail** — `call`/`ask`/`sendWait` behind crash get `ActorStoppedError`
- **Children die** — both scopes close; children come back only if restart re-runs spawn/background
- **`stop`/`drain` are terminal** — no restart
- **Final state = no restart** — `awaitExit` resolves with `ActorExit.Final`
- **Budget** — `Schedule` controls timing/count; exhaustion = terminal `ActorExit.Defect`
- **Classifier** — `shouldRestart` optionally skips restart for specific defect types
- Entity-machine: cluster-supervised via `defectRetryPolicy`, NOT local supervision

## Child Actors

Spawn children from `.spawn()`/`.background()` handlers via `self.spawn(id, childMachine)`:

```ts
machine.spawn(State.Active, ({ self }) =>
  Effect.gen(function* () {
    const child = yield* self.spawn("worker-1", workerMachine).pipe(Effect.orDie);
    yield* child.send(WorkerEvent.Start);
    // child auto-stopped when parent exits Active state
  }),
);
```

- Children spawned in `.spawn()` handlers are **state-scoped** — auto-stopped on state exit
- Children spawned in `.background()` handlers live for machine lifetime
- `self.spawn` returns `Effect<ActorRef, DuplicateActorError, R>` — use `Effect.orDie` in handlers
- Every `ActorRef` has `actor.system` for child access: `actor.system.get("worker-1")`

## ActorRef API

```ts
actor.send(event); // fire-and-forget
actor.cast(event); // alias for send
actor.call(event); // request-reply, returns ProcessEventResult
actor.ask(event); // typed reply (event must have Event.reply())
actor.waitFor(State.X); // wait for state (constructor or predicate)
actor.sendAndWait(ev, X); // send + wait for state
actor.awaitFinal; // wait for final state
actor.watch(other); // completes when other actor stops
actor.drain; // process remaining queue, then stop
actor.subscribe(fn); // sync callback, returns unsubscribe
actor.system; // ActorSystem
actor.children; // ReadonlyMap<string, ActorRef>

// Sync helpers (for UI hooks)
actor.sync.send(event);
actor.sync.stop();
actor.sync.snapshot();
actor.sync.matches(tag);
actor.sync.can(event);
```

## ask / reply

Events declare reply schemas via `Event.reply()`. Handlers use `Machine.reply()`:

```ts
const MyEvent = Event({
  GetCount: Event.reply({}, Schema.Number),
  Reset: {},
});

.on(State.Active, Event.GetCount, ({ state }) =>
  Machine.reply(state, state.count),
)

const count = yield* actor.ask(Event.GetCount);  // number
```

## Handler Type Constraints

| Method                 | Allowed R | Why                           |
| ---------------------- | --------- | ----------------------------- | ---------------------------------- |
| `.on()` / `.reenter()` | `never`   | Pure transitions, no services |
| `.task()` / `.spawn()` | `Scope    | R`                            | State-scoped work can use services |
| `.background()`        | `Scope    | R`                            | Lifetime work can use services     |

- Transition handlers cannot require services or produce errors.
- State effects map failures to events with `.task()` or handle them in `.spawn()`.
- State-effect service requirements propagate to `Machine.spawn` and `system.spawn`.

## Gotchas

- `Machine.spawn` returns an **unstarted** actor — must call `yield* actor.start`. `system.spawn` auto-starts.
- Never `throw` in Effect.gen — use `yield* Effect.fail()`
- `yield* Effect.yieldNow` after `send()` to let effects run
- `simulate()`/`createTestHarness()` don't run spawn effects
- Same-state transitions skip spawn/finalizers — use `.reenter()` to force
- Empty structs: `State.Idle` not `State.Idle()`
- `.onAny()` only fires when no specific `.on()` matches
- `self.spawn` errors with `DuplicateActorError` — wrap with `Effect.orDie`
- Provide state-effect services with `Effect.provide(MyLayer)` around actor creation.
- Sync helpers live on `actor.sync.*`
- Pending `call`/`ask` Deferreds settled with `ActorStoppedError` on stop
- `ask()` only accepts events with `Event.reply()` — non-reply events are a type error
- Reply decode failures (schema mismatch) are defects

## Cluster / Entity Machines

Wire machines to `@effect/cluster` for distributed actors:

```ts
import { toEntity, EntityMachine } from "@humanlayer/effect-machine/cluster";

const OrderEntity = toEntity(orderMachine, { type: "Order" });
const OrderEntityLayer = EntityMachine.layer(OrderEntity, {
  initializeState: (entityId) => OrderState.Pending({ orderId: entityId }),
  persistence: { strategy: "journal" },
});
```

- `toEntity` generates a machine-owned Entity with Send/Ask/GetState/WatchState RPCs
- `EntityMachine.layer` wires the Entity's machine to cluster via the shared runtime kernel
- `EntityActorRef`: `makeEntityActorRef(entity, client, id)` decodes Ask replies and preserves client errors

### Entity Persistence

Opt-in via `EntityMachineOptions.persistence`:

- **Snapshot strategy** (default): background scheduler + deactivation finalizer
- **Journal strategy**: inline event append on each RPC, replay on reactivation
- `PersistenceAdapter` service tag resolved from context
- Journal append failures defect entity — cluster retry restarts from snapshot
- Hydration: snapshot → journal replay → `initializeState` → `machine.initial`

### Cluster Gotchas

- Entity tests use `Entity.makeTestClient` + `ShardingConfig.layer` + `Effect.scoped`
- `EntityMachine.layer` accepts the `MachineEntity` returned by `toEntity`; the entity owns its machine and protocol
- Entity RPCs use `.tag` field (not `._tag`) to distinguish request types
- WatchState test skipped due to effect beta Queue bug

## Documentation

- `SKILL.md` — AI agent quick reference
