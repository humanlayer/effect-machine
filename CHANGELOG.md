# @humanlayer/effect-machine

## 0.18.0

### Minor Changes

- [`37485f0`](https://github.com/humanlayer/effect-machine/commit/37485f064a30063fc43ec24b63744b384c9a52d3) Thanks [@K-Mistele](https://github.com/K-Mistele)! - Publish the Humanlayer-scoped package, target the latest Effect v4 beta, remove the Effect v3 compatibility exports, and infer Effect service requirements from state effects so applications provide behavior with layers rather than actor-local slot maps.

### Patch Changes

- [#2](https://github.com/humanlayer/effect-machine/pull/2) [`bb1ce43`](https://github.com/humanlayer/effect-machine/commit/bb1ce430bce35bc44db5d1545f66d59d865548ec) Thanks [@K-Mistele](https://github.com/K-Mistele)! - Preserve Effect service dependencies supplied while allocating a local actor, so state effects run correctly when `actor.start` is called later.

## 0.17.1

### Patch Changes

- [`99fcb3f`](https://github.com/cevr/effect-machine/commit/99fcb3f7b2770c51b66843f9973fe6380ace812e) Thanks [@cevr](https://github.com/cevr)! - Update the Effect v4 toolchain to `effect@4.0.0-beta.64` and run tsdown through Bun so CI and local builds share Bun's package resolution.

## 0.17.0

### Minor Changes

- [`5e344f7`](https://github.com/cevr/effect-machine/commit/5e344f76b0e3b06bd28d71297b53e02739dc0a5b) Thanks [@cevr](https://github.com/cevr)! - Rename `derive` to `with` on state/event schemas. Union-level `with` accepts shared fields without casts on generic type parameters. Add `.schema` (branded) accessor to `MachineStateSchema`. Remove `.plain` — use constructors in tests instead of raw object literals. Add `Slot.of(slotsSchema, provided)` to convert `ProvideSlots` into `SlotCalls` without consumer-side casts.

### Patch Changes

- [`79a9311`](https://github.com/cevr/effect-machine/commit/79a9311e0370eaa4dbecc2d149ffa0be76039bf8) Thanks [@cevr](https://github.com/cevr)! - Modernize the repository toolchain around `@effect/tsgo`, the latest Effect beta, and type-aware oxlint.

  `effect` is now peer-only at runtime, v4 service tags use the `Context.Service` class form with `serviceNotAsClass` enabled, and the v3 mirror is validated through the same tsgo gate.

## 0.16.0

### Minor Changes

- [`d17665e`](https://github.com/cevr/effect-machine/commit/d17665e726ac10b82ac470d58539643e3624592d) Thanks [@cevr](https://github.com/cevr)! - Replace ambient Scope detection with explicit ActorScope service
  - `Machine.spawn` and `system.spawn` no longer attach cleanup finalizers to ambient `Scope.Scope`. This fixes a bug where unrelated scopes would unexpectedly tear down actors.
  - New `ActorScope` service tag — when present in context, actors attach stop finalizers to it.
  - New `Machine.scoped(effect)` helper bridges `Scope.Scope` → `ActorScope` for opt-in auto-cleanup.
  - Backport `call` improvements to v3: warning log on stopped actor, complete `ProcessEventResult` fields with `satisfies` for type safety.
  - Fix `bun test` tsconfig resolution and add `test:all` / v3 tests to gate.

- [`ec8ccd3`](https://github.com/cevr/effect-machine/commit/ec8ccd3560df0f1e7f8c2ea91086373341072c93) Thanks [@cevr](https://github.com/cevr)! - Upgrade to effect 4.0.0-beta.47, require tsgo for type checking

  **Breaking:** Minimum peer dependency is now `effect@>=4.0.0-beta.47`. The `ServiceMap` module was removed upstream — all `ServiceMap.Service` usages are now `Context.Service`.
  - Rename `ServiceMap.Service` → `Context.Service` throughout
  - Rename `Effect.services()` → `Effect.context()`
  - Switch type checker from `tsc` to `tsgo` (native Go compiler via `@typescript/native-preview`)
  - Switch Effect LSP from tsconfig plugin patch to `effect-language-service diagnostics` CLI
  - Simplify tsconfig.json for TypeScript 6 defaults

## 0.15.2

### Patch Changes

- [`0c81fd3`](https://github.com/cevr/effect-machine/commit/0c81fd3a079dc8ce34a2077c0eeaf96382d95bdb) Thanks [@cevr](https://github.com/cevr)! - fix(v3): backport reply metadata stripping from event constructor payloads

  `Event.reply(...)` constructor payload typing no longer leaks reply schema metadata into user payload arguments.

## 0.15.1

### Patch Changes

- [`612e686`](https://github.com/cevr/effect-machine/commit/612e6863cdde4c806a2c91dbc70eb34598020359) Thanks [@cevr](https://github.com/cevr)! - Fix `Event.reply(...)` constructor payload typing so reply schema metadata does not leak into user payload arguments.

  Add regressions for:
  - payload-bearing reply event constructors accepting plain payload objects
  - `ask()` with payload-bearing reply events

## 0.15.0

### Minor Changes

- [`d518ed6`](https://github.com/cevr/effect-machine/commit/d518ed648095e8916d7a935a3fe0d8b8427ffd7a) Thanks [@cevr](https://github.com/cevr)! - Cold spawn + Recovery/Durability lifecycle API (v3 backport).

  **Breaking changes:**
  - `Machine.spawn` now returns an **unstarted** actor. Call `yield* actor.start` to fork the event loop, background effects, and spawn effects. Events sent before `start()` are queued.
  - `system.spawn` auto-starts — no change needed for registry-based spawns.
  - `PersistConfig<S>` is removed. Use `Lifecycle<S, E>` instead.

  **New APIs:**
  - `ActorRef.start` — idempotent Effect that starts the actor
  - `Recovery<S>` — resolves initial state per generation during `actor.start`
  - `RecoveryContext<S>` — `{ actorId, generation, machineInitial }`
  - `Durability<S, E>` — saves state after committed transitions
  - `DurabilityCommit<S, E>` — `{ actorId, generation, previousState, nextState, event }`
  - `Lifecycle<S, E>` — `{ recovery?, durability? }`

  **Migration from `PersistConfig`:**

  ```ts
  // Before (PersistConfig)
  Machine.spawn(machine, {
    persist: {
      load: () => storage.get(key),
      save: (state) => storage.set(key, state),
      shouldSave: (state, prev) => state._tag !== prev._tag,
      onRestore: (state, { initial }) => validate(state),
    },
  });

  // After (Lifecycle)
  const actor =
    yield *
    Machine.spawn(machine, {
      lifecycle: {
        recovery: {
          // Replaces load() + onRestore() — single callback
          resolve: ({ actorId, generation, machineInitial }) =>
            storage.get(key).pipe(
              Effect.map(Option.fromNullable),
              // Do any validation/migration here
            ),
        },
        durability: {
          // Receives full commit context, not just the new state
          save: ({ actorId, generation, previousState, nextState, event }) =>
            storage.set(key, nextState),
          shouldSave: (state, prev) => state._tag !== prev._tag,
        },
      },
    });
  yield * actor.start; // NEW: explicit start required
  ```

  **Key differences from `PersistConfig`:**
  - `Recovery.resolve` merges `load()` + `onRestore()` into one callback
  - `Recovery.resolve` receives `RecoveryContext` with `actorId`, `generation` (0 = cold start, 1+ = supervision restart), and `machineInitial`
  - `Durability.save` receives `DurabilityCommit` with full transition context (previous state, next state, event, generation)
  - Recovery runs during `actor.start`, not during allocation
  - `hydrate` option overrides recovery entirely (resolve is never called)

## 0.14.0

### Minor Changes

- [`ff7fd8f`](https://github.com/cevr/effect-machine/commit/ff7fd8f6fc3505db568b432e5425f45f3474a554) Thanks [@cevr](https://github.com/cevr)! - Unified slot system redesign + local persistence + slot schemas.

  **Breaking changes (pre-1.0):**
  - `Slot.Guards` / `Slot.Effects` replaced by `Slot.define` + `Slot.fn`
  - `guards:` / `effects:` on `Machine.make` replaced by single `slots:` field
  - Slot handlers take only params — no ctx parameter. Use `yield* machine.Context` for machine state.
  - `HandlerContext.guards` / `HandlerContext.effects` replaced by `HandlerContext.slots`
  - `StateHandlerContext.effects` replaced by `StateHandlerContext.slots`
  - Removed: `SlotContext`, `GuardsDef`, `EffectsDef`, `GuardSlot`, `EffectSlot`, `GuardHandlers`, `EffectHandlers`, `HasGuardKeys`, `HasEffectKeys`
  - `SlotProvisionError.slotType` is now `"slot"` only (was `"guard" | "effect" | "slot"`)
  - `Machine.spawn` `slots` option is now `ProvideSlots<SD>` (type-checked, was `Record<string, any>`)

  **New APIs:**
  - `Slot.fn(fields, returnSchema?)` — define a slot with typed params and arbitrary return type
  - `Slot.define({ ... })` — create a slots schema from slot definitions
  - `SlotFnDef.inputSchema` / `outputSchema` — materialized schemas for runtime validation and serialization
  - `SlotsSchema.requestSchema` / `resultSchema` / `invocationSchema` — wire-format schemas for RPC and persistence
  - `slotValidation` option on `Machine.make` — runtime input/output validation (default: true)
  - `SlotCodecError` — tagged error for validation failures (raised as defect)
  - `PersistConfig<S>` — local persistence for `Machine.spawn`:
    - `load()` → hydrate from storage
    - `save(state)` → save after transitions
    - `shouldSave?(state, prev)` → filter saves
    - `onRestore?(state, { initial })` → recovery decision hook
  - `ActorSystem.spawn` now accepts `slots` and `persist` options
  - Multi-state `.spawn()` and `.task()` overloads (array of states)
  - `.task()` shorthand — omit `onSuccess` when task returns Event directly

  **Internal:**
  - `resolveActorSystem()` and `runSupervisionLoop()` extracted from `createActor`
  - `Queue.clear` replaces manual poll loop for shutdown drain
  - Plain-object return bug fixed in slot resolve (uses `Effect.isEffect`)
  - `materializeMachine` threads `_slotValidation` through copies

### Patch Changes

- [`8e8a9ce`](https://github.com/cevr/effect-machine/commit/8e8a9cefce69431e24db9c8718e53bc203b08440) Thanks [@cevr](https://github.com/cevr)! - - feat: union-level `derive` on State and Event schemas — dispatches by `_tag`, preserves specific variant subtype
  - fix: `derive` partial keys not in target variant are now silently dropped
  - fix: `.task()` `onSuccess` is now optional — omit when task returns Event directly

## 0.13.0

### Minor Changes

- [`74e3fea`](https://github.com/cevr/effect-machine/commit/74e3fea361eb4d90e759d06479b825418cc59357) Thanks [@cevr](https://github.com/cevr)! - Add actor supervision with automatic restart on defect.

  **New APIs:**
  - `Supervision.restart({ maxRestarts, within, backoff })` — Schedule-based restart policy
  - `Supervision.none` — no supervision (default, crashes are terminal)
  - `actor.awaitExit` — resolves with `ActorExit<S>` when actor terminally stops
  - `actor.watch(other)` — now returns `ActorExit<unknown>` (breaking, pre-1.0)

  **New types:**
  - `ActorExit<S>` — `Final { state }` | `Stopped` | `Defect { cause, phase }`
  - `DefectPhase` — `"transition"` | `"spawn"` | `"background"` | `"initial-spawn"`
  - `Supervision.Policy` — Schedule-based restart policy interface

  **Usage:**

  ```ts
  import { Machine, Supervision } from "effect-machine";

  const actor =
    yield *
    Machine.spawn(machine, {
      supervision: Supervision.restart({ maxRestarts: 3, within: "1 minute" }),
    });

  const exit = yield * actor.awaitExit; // ActorExit<S>
  ```

  **Breaking changes (pre-1.0):**
  - `watch()` returns `Effect<ActorExit<unknown>>` instead of `Effect<void>`
  - `SystemEvent.ActorStopped` gains `exit: ActorExit<unknown>` field
  - New `SystemEvent.ActorRestarted` variant

  **Internal:**
  - Runtime kernel split: cell-owned resources, actorScope, exitDeferred
  - Background/spawn/transition defect detection with DefectPhase tagging
  - Generation owner fiber for actorScope lifecycle
  - `Effect.runForkWith` for proper service propagation (v4)
  - `globalXInEffect` diagnostics enabled in tsconfig

## 0.12.0

### Minor Changes

- [`f26b21f`](https://github.com/cevr/effect-machine/commit/f26b21f3b3d47973f57bf7ab7aef3a97d96d7d5b) Thanks [@cevr](https://github.com/cevr)! - feat(cluster): entity persistence with snapshot and journal strategies

  Add opt-in state persistence for entity-machines across deactivation/reactivation:
  - **Snapshot strategy**: periodic background saves + deactivation finalizer. Simple, fast.
  - **Journal strategy**: inline event append on each Send/Ask RPC, replay on reactivation. Full audit trail.
  - **PersistenceAdapter** service tag with `saveSnapshot`, `loadSnapshot`, `appendEvents` (CAS), `loadEvents`
  - **InMemoryPersistenceAdapter** for testing/development
  - **PersistenceKey** = `{ entityType, entityId }` prevents cross-type collisions
  - Journal append failures defect the entity (cluster retry restarts from last snapshot)
  - Snapshot scheduler only in snapshot-only mode (prevents state/version tear in journal mode)
  - v3 backport included

  Also includes the cluster overhaul (runtime kernel, EntityActorRef, WatchState, self.reply, self.spawn).

- [`33d8a87`](https://github.com/cevr/effect-machine/commit/33d8a87f13617c771fc5b96966435a63965cd258) Thanks [@cevr](https://github.com/cevr)! - feat: add typed reply schemas for ask()
  - `Event.reply(fields, schema)` — declare reply-bearing events with schema validation
  - `Machine.reply(state, value)` — branded helper replacing duck-typed `{ state, reply }`
  - `actor.ask(event)` — infers return type from event's reply schema; non-reply events are type errors
  - Runtime validation: reply values decoded through schema; decode failure = defect
  - Entity-machine: `Ask` RPC propagates replies through cluster boundary
  - Backported to v3

## 0.11.0

### Minor Changes

- [`6bdee0c`](https://github.com/cevr/effect-machine/commit/6bdee0c8ed38ea644f6197b6a26e729673f3ac36) Thanks [@cevr](https://github.com/cevr)! - Delete monolithic persistence subsystem, add composable primitives.

  **Added:**
  - `Machine.replay(built, events, { from? })` — fold events through transition handlers to compute state. Respects postpone rules and final-state cutoff. Runs effectful handlers with stubbed self/system.
  - `actor.transitions` — PubSub-backed stream of `{ fromState, toState, event }` on every successful transition. Observational, not a durability guarantee.

  **Removed:**
  - `PersistenceAdapter`, `PersistenceAdapterTag`, `PersistenceError`, `VersionConflictError`
  - `PersistentMachine`, `PersistentActorRef`, `PersistenceConfig`
  - `createPersistentActor`, `restorePersistentActor`, `isPersistentMachine`
  - `InMemoryPersistenceAdapter`, `makeInMemoryPersistenceAdapter`
  - `Machine.persist()`, `BuiltMachine.persist()`
  - `ActorSystem.restore`, `ActorSystem.restoreMany`, `ActorSystem.restoreAll`, `ActorSystem.listPersisted`

  **Migration:** Compose persistence from primitives:
  - Snapshot: `actor.changes` → save to your store
  - Event journal: `actor.transitions` → append events
  - Restore from snapshot: `Machine.spawn(machine, { hydrate: loadedState })`
  - Restore from events: `Machine.replay(machine, events)` → `Machine.spawn(machine, { hydrate: state })`

### Patch Changes

- [`3ff2dfb`](https://github.com/cevr/effect-machine/commit/3ff2dfb421e17818fcadf1195e5e943d10ed9448) Thanks [@cevr](https://github.com/cevr)! - Fix multi-stage postpone drain in live actor event loop. Previously, postponed events were drained in a single pass — if a drained event caused a state change that made other postponed events runnable, they waited until the next mailbox event. Now loops until stable, matching simulate() and replay() behavior.

## 0.10.0

### Minor Changes

- [`921e063`](https://github.com/cevr/effect-machine/commit/921e0630cd24c51d7e189c760db2df57a2a6f239) Thanks [@cevr](https://github.com/cevr)! - OTP-inspired API redesign:
  - rename dispatch→call, add cast alias for send
  - extract sync helpers to actor.sync.\* namespace
  - add ask() for typed domain replies from handlers
  - add .timeout() for gen_statem-style state timeouts
  - add .postpone() for gen_statem-style event postpone
  - fix reply settlement (ActorStoppedError on stop/interrupt)

  Breaking: removed top-level sync methods (sendSync, stopSync, etc.), removed dispatchPromise.

- [`eee2ff4`](https://github.com/cevr/effect-machine/commit/eee2ff413c855a90e432e068ddf21efe0dc8fe68) Thanks [@cevr](https://github.com/cevr)! - Backport all v4 features to Effect v3 variant + restructure into `v3/` directory

  **Restructure:**
  - `src-v3/` → `v3/src/`, `tsconfig.v3.json` → `v3/tsconfig.json`, `tsdown.v3.config.ts` → `v3/tsdown.config.ts`
  - Added `v3/test/` with full test suite (248 tests)
  - Package exports unchanged: `effect-machine/v3`, `effect-machine/v3/cluster`

  **Features backported from v4:**
  - `call()` — serialized request-reply (OTP gen_server:call)
  - `cast()` — fire-and-forget alias for send (OTP gen_server:cast)
  - `ask()` — typed domain reply from handler's `{ state, reply }` return
  - `ActorRef.sync` namespace — replaces flat `sendSync`/`stopSync`/`snapshotSync`/`matchesSync`/`canSync`
  - `.timeout()` builder — gen_statem-style state timeouts
  - `.postpone()` builder — gen_statem-style event postpone with drain-until-stable
  - `hasReply` structural flag on `ProcessEventResult`
  - `ActorStoppedError` / `NoReplyError` error types
  - `makeInspectorEffect` / `combineInspectors` / `tracingInspector` inspection helpers
  - `actorId` threaded through all handler contexts

  **Bug fix:**
  - `State.derive()` now guards against `_tag` override in partial argument

## 0.9.0

### Minor Changes

- [`6e3497b`](https://github.com/cevr/effect-machine/commit/6e3497bfadf0e354bd6f6284d940df8a72c27e9e) Thanks [@cevr](https://github.com/cevr)! - Add `dispatch` and `dispatchPromise` to ActorRef — synchronous event processing with transition receipts.

  **`dispatch(event)`** — Effect-based. Sends event through the queue (preserving serialization) and returns `ProcessEventResult<State>` with `{ transitioned, previousState, newState, lifecycleRan, isFinal }`. OTP `gen_server:call` equivalent.

  **`dispatchPromise(event)`** — Promise-based. Same semantics as `dispatch` for use at non-Effect boundaries (React event handlers, framework hooks, tests).

  Also exports `ProcessEventResult` from the public API.

## 0.8.0

### Minor Changes

- [`675f461`](https://github.com/cevr/effect-machine/commit/675f461b259639e1c2251d9a13cd334ae17881b4) Thanks [@cevr](https://github.com/cevr)! - Add effectful inspectors, scoped `from(...)` transitions, and named task inspection events.

### Patch Changes

- [`ac8f691`](https://github.com/cevr/effect-machine/commit/ac8f69125cacf9eb3ffe0dd63f2c71af36f306d9) Thanks [@cevr](https://github.com/cevr)! - Update Effect dependency to 4.0.0-beta.35

## 0.7.2

### Patch Changes

- [`f6c5bbe`](https://github.com/cevr/effect-machine/commit/f6c5bbee39bc118544f8cd88599a23655545c74f) Thanks [@cevr](https://github.com/cevr)! - Update Effect dependency to 4.0.0-beta.26

## 0.7.1

### Patch Changes

- [`fa54c61`](https://github.com/cevr/effect-machine/commit/fa54c61db824bdd9bf77976269844fa83d73e96b) Thanks [@cevr](https://github.com/cevr)! - Update Effect dependency to 4.0.0-beta.21
  - Replace `Effect.makeSemaphore` with `Semaphore.make` (removed in beta.6)

## 0.7.0

### Minor Changes

- [`34c85b8`](https://github.com/cevr/effect-machine/commit/34c85b8191728264177f1520f2893f746d74b8e8) Thanks [@cevr](https://github.com/cevr)! - Migrate to Effect v4 (4.0.0-beta.5)
  - Default export now targets Effect v4 — v3 users should import from `effect-machine/v3`
  - Migrate src/ and test suite to v4 APIs (Effect, Schema, SubscriptionRef, ServiceMap)
  - MachineStateSchema/MachineEventSchema now use Schema.Codec for encode/decode compat
  - 213 tests passing across 19 files

## 0.6.0

### Minor Changes

- [`9d5bd6f`](https://github.com/cevr/effect-machine/commit/9d5bd6fa81b3421124732d75fca210b2bb04c57c) Thanks [@cevr](https://github.com/cevr)! - Add actor.children to expose child actors spawned via self.spawn
  - `actor.children` returns `ReadonlyMap<string, ActorRef>` of children spawned via `self.spawn`
  - State-scoped children auto-removed from map on state exit
  - Works for both regular and persistent actors

- [`9d5bd6f`](https://github.com/cevr/effect-machine/commit/9d5bd6fa81b3421124732d75fca210b2bb04c57c) Thanks [@cevr](https://github.com/cevr)! - Add observable ActorSystem with event stream and sync observation
  - `system.subscribe(fn)` — sync callback for `ActorSpawned` / `ActorStopped` events, returns unsubscribe
  - `system.actors` — sync snapshot of all registered actors (`ReadonlyMap`)
  - `system.events` — async `Stream<SystemEvent>` via PubSub (each subscriber gets own queue)
  - Works with both explicit (`ActorSystemDefault`) and implicit (`Machine.spawn`) systems
  - No events emitted during system teardown
  - Double-stop prevention: `system.stop` + scope finalizer won't emit duplicate `ActorStopped`

## 0.5.0

### Minor Changes

- [`28835c6`](https://github.com/cevr/effect-machine/commit/28835c6afa09c440093db48444adaa1ca6782aa2) Thanks [@cevr](https://github.com/cevr)! - Add observable ActorSystem and wire up actor.children for persistent actors

  **ActorSystem observation:**
  - `system.subscribe(fn)` — sync callback for `ActorSpawned` / `ActorStopped` events, returns unsubscribe
  - `system.actors` — sync snapshot of all registered actors (`ReadonlyMap`)
  - `system.events` — async `Stream<SystemEvent>` via PubSub (each subscriber gets own queue)
  - Works with both explicit (`ActorSystemDefault`) and implicit (`Machine.spawn`) systems
  - No events emitted during system teardown
  - Double-stop prevention: `system.stop` + scope finalizer won't emit duplicate `ActorStopped`

  **actor.children:**
  - Wire up `childrenMap` in persistent actors so `actor.children` reflects `self.spawn` children
  - Children auto-removed from map on scope close (state-scoped cleanup)

## 0.4.0

### Minor Changes

- [`bd3953e`](https://github.com/cevr/effect-machine/commit/bd3953eaf0f2069ec6368a9a4c74451a9ab707fc) Thanks [@cevr](https://github.com/cevr)! - Add child actor support: `self.spawn()` from handlers, `actor.system` on every ActorRef, implicit system creation for `Machine.spawn`, and automatic lifecycle coupling to state scope

## 0.3.2

### Patch Changes

- [`a16c44e`](https://github.com/cevr/effect-machine/commit/a16c44e3e0da49712d5600092ac6eee6485d5270) Thanks [@cevr](https://github.com/cevr)! - Add tsdown build step. Library now ships pre-built ESM with .d.ts declarations instead of raw TypeScript source.

## 0.3.1

### Patch Changes

- Add `stopSync` to `ActorRef` — fire-and-forget stop for sync contexts (framework cleanup hooks, event handlers).

## 0.3.0

### Minor Changes

- [`0154ac9`](https://github.com/cevr/effect-machine/commit/0154ac910aed5b8ac456ba7194b6be25f8d640d4) Thanks [@cevr](https://github.com/cevr)! - feat: DX overhaul — multi-state `.on()`, `State.derive()`, `.onAny()`, `waitFor` deadlock fix, `sendSync`, `waitFor(State.X)`, `.build()` / `BuiltMachine`
  - **Multi-state `.on()`/`.reenter()`**: Accept arrays of states — `.on([State.A, State.B], Event.X, handler)`
  - **`State.derive()`**: Construct new state from source — `State.B.derive(stateA, { extra: val })` picks overlapping fields + applies overrides
  - **`.onAny()` wildcard transitions**: Handle event from any state — `.onAny(Event.Cancel, () => State.Cancelled)`. Specific `.on()` takes priority.
  - **`waitFor` deadlock fix**: Rewrote to use sync listeners + `Deferred` instead of `SubscriptionRef.changes` stream, preventing semaphore deadlock on synchronous transitions
  - **`sendSync`**: Fire-and-forget sync send for framework integration (React/Solid hooks)
  - **`waitFor(State.X)`**: Accept state constructor/value instead of predicate — `actor.waitFor(State.Active)` and `actor.sendAndWait(event, State.Done)`
  - **`.build()` / `BuiltMachine`**: Terminal builder method — `.provide()` renamed to `.build()`, returns `BuiltMachine`. `.validate()` removed. `Machine.spawn` and `ActorSystem.spawn` accept `BuiltMachine`. No-slot machines: `.build()` with no args.

- [`365da12`](https://github.com/cevr/effect-machine/commit/365da129e6875ebd9dc2b68f8b0873aab90f42cf) Thanks [@cevr](https://github.com/cevr)! - feat: remove `Scope.Scope` from `Machine.spawn` and `system.spawn` signatures
  - **Scope-optional spawn**: `Machine.spawn` and `ActorSystem.spawn` no longer require `Scope.Scope` in `R`. Both detect scope via `Effect.serviceOption` — if present, attach cleanup finalizer; if absent, skip.
  - **Daemon forks**: Event loop, background effects, and persistence fibers use `Effect.forkDaemon` — detached from parent scope, cleaned up by `actor.stop`.
  - **System-level cleanup**: `ActorSystem` layer teardown stops all registered actors automatically. `ActorSystemDefault` is now `Layer.scoped`.
  - **Breaking**: Callers that relied on `Scope.Scope` appearing in the `R` type of spawn may need type adjustments. `Effect.scoped` wrappers around spawn are no longer required but still work (scope detection finds them).

## 0.2.4

### Patch Changes

- [`d29f2ff`](https://github.com/cevr/effect-machine/commit/d29f2ff77af15f021884227042032cf5b46e9219) Thanks [@cevr](https://github.com/cevr)! - feat: makeInspector accepts Schema constructors as type params

  `makeInspector<typeof MyState, typeof MyEvent>(cb)` now auto-extracts `.Type` from schema constructors via `ResolveType`. No need for `typeof MyState.Type` anymore.

## 0.2.3

### Patch Changes

- [`85a2854`](https://github.com/cevr/effect-machine/commit/85a285401e9643710dd965bc376112bfaa7bdf33) Thanks [@cevr](https://github.com/cevr)! - fix: use forkScoped for event loop fiber to prevent premature interruption

  Changed `Effect.fork` to `Effect.forkScoped` for the actor event loop fiber. Previously, the event loop was attached to the calling fiber's scope, meaning it would be interrupted when a transient caller completed. Now the event loop's lifetime is tied to the provided `Scope.Scope` service, allowing callers to control the actor's lifecycle explicitly.

## 0.2.2

### Patch Changes

- [`2bcb0bb`](https://github.com/cevr/effect-machine/commit/2bcb0bbbb070c8f35cc9253aee3206140b8b35ae) Thanks [@cevr](https://github.com/cevr)! - Add `AnyInspectionEvent` type alias and default generic params on `makeInspector`/`consoleInspector`/`collectingInspector` so untyped inspectors work without explicit casts.

- [`c50c3ce`](https://github.com/cevr/effect-machine/commit/c50c3ce64207e8dea38d8d40a31de56e3a5549a7) Thanks [@cevr](https://github.com/cevr)! - Fix `waitFor` race condition where a fast state transition between `get` and `changes` subscription could cause `waitFor` to hang forever. Now uses `stateRef.changes` directly which emits the current value atomically as its first element.

## 0.2.1

### Patch Changes

- [`639ee87`](https://github.com/cevr/effect-machine/commit/639ee8731f91a23b2c6be4a68a2f7b0a7cee8bee) Thanks [@cevr](https://github.com/cevr)! - Fix PersistenceAdapterTag key for deterministic key diagnostics and align language-service config.

## 0.2.0

### Minor Changes

- [`f4a8c8f`](https://github.com/cevr/effect-machine/commit/f4a8c8f56bcc2d680d1d97c86d99e5c6a42dd2f5) Thanks [@cevr](https://github.com/cevr)! - - send after stop is a no-op for actors and persistent actors
  - async persistence worker for journaling/metadata to keep event loop fast
  - snapshot schedule completion no longer stops event loop
  - serialize spawn/restore to avoid duplicate side-effects under concurrency

- [`a77f9fa`](https://github.com/cevr/effect-machine/commit/a77f9fa117be89231b1a16db62c9793caa035091) Thanks [@cevr](https://github.com/cevr)! - Add Machine.task, ActorRef wait/await helpers, and error inspection events.

- [`3172971`](https://github.com/cevr/effect-machine/commit/31729717c653969aa158dd887248912c817cbe50) Thanks [@cevr](https://github.com/cevr)! - Add Effect.fn tracing across actor, persistence, cluster, and testing flows. Fix persistent actor lifecycle (spawn/background effects, snapshot scheduler), restore-from-events behavior, duplicate spawn handling, and slot preflight errors. Update persistence docs; PersistentActorRef now carries environment R and replayTo runs in R.
