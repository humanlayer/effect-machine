---
name: humanlayer-effect-machine
description: Type-safe state machines for Effect. Use when building state machines with @humanlayer/effect-machine — defining states/events, transition handlers, service-backed state effects, timeouts, postpone, actors, typed ask/reply, testing, recovery/durability lifecycle. Triggers on @humanlayer/effect-machine imports, Machine.make, Machine.spawn, actor.start, Machine.replay, Machine.reply, Event.reply, State/Event definitions, ActorRef usage, Recovery, Durability, Lifecycle.
---

## Navigation

```
What are you building?
├─ Defining states/events           → §Schema-First
├─ Writing transition handlers      → §Transitions
├─ Adding side effects              → §Effects
├─ Testing machines                 → §Testing
├─ Running actors                   → §Actors
├─ Typed ask/reply                  → §Ask / Reply
├─ Recovery/durability              → §Lifecycle
├─ Timeouts / postpone              → §Timeouts, §Postpone
└─ Providing Effect services        → §Services And Layers
```

## Schema-First

States and events ARE schemas. `State({})` and `Event({})` produce tagged unions with constructors.

```ts
import { Schema } from "effect";
import { State, Event } from "@humanlayer/effect-machine";

const S = State({
  Idle: {}, // empty → plain value: S.Idle
  Loading: { url: Schema.String }, // non-empty → constructor: S.Loading({ url })
});

const E = Event({
  Start: { url: Schema.String },
  Done: { data: Schema.Unknown },
  GetCount: Event.reply({}, Schema.Number), // reply-bearing event
  GetInfo: Event.reply({ id: Schema.String }, Schema.String), // payload + reply
});
```

**with** — construct from existing state, picks overlapping fields:

```ts
S.Active.with(state); // pick target fields from source
S.Active.with(state, { count: n + 1 }); // pick + override
```

**Type guards / matching:**

```ts
S.$is("Loading")(value)                  // boolean type guard
S.$match(value, { Loading: (s) => ..., _: () => ... })  // pattern match
```

## Transitions

```ts
const machine = Machine.make({ state: S, event: E, initial: S.Idle })
  // Single state → event → handler
  .on(S.Idle, E.Start, ({ event }) => S.Loading({ url: event.url }))

  // Multi-state source
  .on([S.Loading, S.Retrying], E.Done, ({ event }) => S.Active({ data: event.data }))

  // Wildcard — any state (specific .on wins)
  .onAny(E.Cancel, () => S.Cancelled)

  // Reenter same state (re-triggers spawn effects + timeouts)
  .reenter(S.Active, E.Refresh, ({ state }) => S.Active.with(state))

  // Mark final states (actor stops, postpone buffer settles)
  .final(S.Done)
  .final(S.Cancelled);
```

**Handler return types:**

```ts
// Pure: return new state
({ state, event }) => S.Next({ ... })

// Effectful: return Effect<State>
({ state }) => Effect.gen(function* () { ... return S.Next({ ... }) })

// With reply (for actor.ask — event must use Event.reply()):
({ state }) => Machine.reply(S.Same.with(state), state.count)
```

## Effects

### spawn — state-scoped, auto-cancelled on exit

```ts
machine.spawn(S.Loading, ({ state, self }) =>
  Effect.gen(function* () {
    const data = yield* fetchData(state.url);
    yield* self.send(E.Done({ data }));
  }),
);
```

### task — spawn + auto-route success/failure

```ts
machine.task(S.Loading, ({ state }) => fetchData(state.url), {
  onSuccess: (data) => E.Done({ data }),
  onFailure: () => E.Error,
});
```

### background — machine-lifetime (not state-scoped)

```ts
machine.background(({ self }) =>
  Stream.fromSchedule(Schedule.spaced("10 seconds")).pipe(
    Stream.runForEach(() => self.send(E.Heartbeat)),
  ),
);
```

## Services And Layers

State effects use normal Effect services. Requirements from `.task()`, `.spawn()`, and `.background()` are inferred by the machine and provided when it starts:

```ts
import { Context, Effect, Layer } from "effect";

class Loader extends Context.Service<
  Loader,
  { readonly load: (url: string) => Effect.Effect<string> }
>()("@app/Loader") {}

const machine = Machine.make({ state: S, event: E, initial: S.Idle })
  .on(S.Idle, E.Start, ({ event }) => S.Loading({ url: event.url }))
  .task(
    S.Loading,
    ({ state }) =>
      Effect.gen(function* () {
        const loader = yield* Loader;
        return yield* loader.load(state.url);
      }),
    { onSuccess: (data) => E.Done({ data }) },
  );

const LoaderLive = Layer.succeed(Loader, { load: (url) => Http.get(url) });
const actor = yield * Machine.spawn(machine).pipe(Effect.provide(LoaderLive));
yield * actor.start;
```

Transition handlers remain pure. Run I/O in state effects and map the result to an event.

## Ask / Reply

Events declare reply schemas via `Event.reply(fields, schema)`. Handlers must use `Machine.reply()`:

```ts
const E = Event({
  GetCount: Event.reply({}, Schema.Number), // askable
  Reset: {}, // not askable
});

// Handler — Machine.reply() required for reply-bearing events
machine.on(S.Active, E.GetCount, ({ state }) => Machine.reply(S.Active.with(state), state.count));

// Caller — return type inferred from schema
const count = yield * actor.ask(E.GetCount); // number
// actor.ask(E.Reset) — TYPE ERROR (no reply schema)
```

**Rules:**

- `Event.reply({}, schema)` — empty payload + reply; `Event.reply({ id: Schema.String }, schema)` — payload + reply
- Handler for reply-bearing event MUST return `Machine.reply(state, value)` — plain state return is a type error
- Handler for non-reply event CANNOT return `Machine.reply()` — type error
- `.onAny()` handlers cannot provide replies — use specific `.on()` for reply events
- Reply decode mismatch (handler returns wrong type) = defect at runtime
- Cluster: `Ask` RPC propagates replies through entity boundary

## Timeouts

gen_statem-style. Timer starts on state entry, cancels on exit:

```ts
machine.timeout(S.Loading, {
  duration: Duration.seconds(30),
  event: E.Timeout,
});

// Dynamic duration from state
machine.timeout(S.Retrying, {
  duration: (state) => Duration.seconds(state.backoff),
  event: E.GiveUp,
});
```

`.reenter()` restarts the timer with fresh state values.

## Postpone

gen_statem-style. Buffered events drain FIFO on state change, looping until stable:

```ts
machine.postpone(S.Connecting, E.Data).postpone(S.Connecting, [E.Data, E.Command]);
```

Multi-stage: if a drained event causes another state change, postponed events re-drain.

## Actors

### Machine.spawn — standalone, unstarted actor

`Machine.spawn` returns a **cold** actor. Call `actor.start` to fork the event loop. Events sent before `start()` are queued.

```ts
const actor = yield * Machine.spawn(machine);
yield * actor.start;

// With options
const actor = yield * Machine.spawn(machine, { id: "my-id", hydrate: savedState });
yield * actor.start;
```

Auto-cleans up if `Scope` is present. Otherwise call `actor.stop` manually.

### ActorRef API

| Method                 | Description                                                         |
| ---------------------- | ------------------------------------------------------------------- |
| `start`                | Fork event loop + effects (required after `Machine.spawn`)          |
| `send(event)`          | Fire-and-forget                                                     |
| `cast(event)`          | Alias for send                                                      |
| `call(event)`          | Request-reply → `ProcessEventResult`                                |
| `ask(event)`           | Typed reply (event must have `Event.reply()` schema)                |
| `snapshot`             | Current state                                                       |
| `changes`              | `Stream<State>` (SubscriptionRef-backed)                            |
| `transitions`          | `Stream<{ fromState, toState, event }>` (PubSub-backed edge stream) |
| `waitFor(S.X)`         | Wait for state                                                      |
| `sendAndWait(ev, S.X)` | Send + wait                                                         |
| `awaitFinal`           | Wait for final state                                                |
| `sync.*`               | Sync variants for non-Effect boundaries                             |

### ActorSystem — registry + lifecycle (auto-starts)

`system.spawn` auto-starts — no `actor.start` needed.

```ts
const system = yield * ActorSystemService;
const actor = yield * system.spawn("id", machine); // auto-started
const maybe = yield * system.get("id"); // Option<ActorRef>
yield * system.stop("id"); // boolean
system.actors; // ReadonlyMap snapshot
system.events; // Stream<SystemEvent>
```

### Child actors

```ts
machine.spawn(S.Active, ({ self }) =>
  Effect.gen(function* () {
    const child = yield* self.spawn("worker", workerMachine).pipe(Effect.orDie);
    yield* child.send(WorkerEvent.Start);
    // auto-stopped when parent exits Active
  }),
);
```

## Lifecycle

Recovery + Durability hooks for persistence. Passed via `lifecycle` option on `Machine.spawn` / `system.spawn`.

```ts
const actor =
  yield *
  Machine.spawn(machine, {
    lifecycle: {
      recovery: {
        // Runs during actor.start. Return Some to override initial state, None for cold start.
        resolve: ({ actorId, generation, machineInitial }) =>
          storage.get(actorId).pipe(Effect.map(Option.fromNullable)),
      },
      durability: {
        // Runs after each committed transition
        save: ({ actorId, generation, previousState, nextState, event }) =>
          storage.set(actorId, nextState),
        // Optional sync filter — skip uninteresting transitions
        shouldSave: (state, prev) => state._tag !== prev._tag,
      },
    },
  });
yield * actor.start;
```

| Interface          | When it runs                                     | Receives                                                   |
| ------------------ | ------------------------------------------------ | ---------------------------------------------------------- |
| `Recovery<S>`      | During `actor.start` (and supervision restart)   | `{ actorId, generation, machineInitial }`                  |
| `Durability<S, E>` | After each state commit, before reply settlement | `{ actorId, generation, previousState, nextState, event }` |

- `generation` — 0 = cold start, 1+ = supervision restart
- `hydrate` overrides recovery — `Machine.spawn(machine, { hydrate: state })` skips `resolve` entirely
- `Lifecycle<S, E>` = `{ recovery?, durability? }` — both optional

### Replay + Hydrate

```ts
// Restore from snapshot
const actor = yield * Machine.spawn(machine, { hydrate: loadedState });
yield * actor.start;

// Restore from event log
const state = yield * Machine.replay(machine, events);
const actor = yield * Machine.spawn(machine, { hydrate: state });
yield * actor.start;

// Restore from snapshot + tail events
const state = yield * Machine.replay(machine, tailEvents, { from: snapshot });
const actor = yield * Machine.spawn(machine, { hydrate: state });
yield * actor.start;
```

**Machine.replay semantics:**

- Folds events through transition handlers (pure or effectful)
- `self.send`/`self.spawn` are no-ops (stubbed)
- Spawn effects, background effects, timeouts do NOT run
- Postpone rules respected (loop until stable)
- Final state stops replay
- Unhandled events silently skipped

## Testing

```ts
import { simulate, assertPath, assertReaches, createTestHarness } from "@humanlayer/effect-machine";

// Simulate — run events, get all states
const { states, finalState } = yield * simulate(machine, [E.Start, E.Done]);

// Assertions
yield * assertPath(machine, events, ["Idle", "Loading", "Done"]);
yield * assertReaches(machine, events, "Done");
yield * assertNeverReaches(machine, events, "Error");

// Test harness — step-by-step
const harness = yield * createTestHarness(machine);
yield * harness.send(E.Start);
expect(harness.state._tag).toBe("Loading");
```

Both `simulate` and `createTestHarness` accept `Machine` directly.

## Gotchas

- **`Machine.spawn` returns unstarted actor** — must call `yield* actor.start`. `system.spawn` auto-starts.
- **Provide service implementations with layers** — `Machine.spawn(machine).pipe(Effect.provide(MyLayer))`
- **Empty state = value, non-empty = constructor** — `S.Idle` vs `S.Loading({ url })`
- **Spawn effects re-run on hydrate** — `Machine.spawn({ hydrate })` re-runs spawn effects for the hydrated state (timers, scoped resources)
- **`hydrate` overrides recovery** — `resolve()` is never called when `hydrate` is set
- **`transitions` is observational** — PubSub-backed, late subscribers miss edges. Not a durability guarantee.
- **Effectful handlers in replay** — replay runs handlers but stubs `self`/`system`. Side effects through `self.send` are no-ops.
- **`ask()` requires reply schema** — only events with `Event.reply()` accepted; non-reply events are type errors
- **Reply decode failure = defect** — if handler returns wrong type, actor dies (broken handler, not business logic)
