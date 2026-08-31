---
"@humanlayer/effect-machine": minor
---

Remove the deprecated Slot API in favor of Effect services supplied through Layers to `.task()`, `.spawn()`, and `.background()`.

This release also strengthens the package's type and runtime boundaries:

- Requirement-growing builder methods are copy-on-write, keeping earlier machine aliases unchanged and truthfully typed.
- Heterogeneous actor registries expose an eventless `ActorHandle`; exact typed spawn results remain `ActorRef<State, Event>`.
- Transition and state-effect registries retain their state/event correlations without chained assertions.
- State-effect contexts expose explicit `$init` and `$enter` lifecycle events.
- Entity machines own their RPC protocol; remote Ask replies are decoded with event-specific schemas and client errors remain typed.
- Persistence writes encode state and events through machine codecs, while loaded records remain `unknown` until full schema decoding.
- Local and entity Ask paths support transforming reply codecs without duplicate decoding or stranded deferred replies.
- Repeated and concurrent `actor.start` callers observe the original startup failure cause.
- Source enforces unsafe, chained, widening, and unnecessary type-assertion rules. Tests remain exempt from unsafe and unnecessary assertion checks.

Intentional API changes:

- Remove `Slot`, slot schemas/types/errors, `Machine.make({ slots })`, `ctx.slots`, and slot provision options.
- `toEntity(machine)` returns a machine-owned `MachineEntity`; call `EntityMachine.layer(entity, options?)`.
- Call `makeEntityActorRef(entity, client, entityId)` so the wrapper can decode replies and preserve client errors.
- `system.get`, `system.actors`, system events, and `actor.children` expose `ActorHandle`.
- Transition and spawn-effect introspection expose guarded `matches` / `run` operations instead of erased handlers.
- Legacy compatible constructors without a static tag can use `Machine.tagged(tag, constructor)`.
