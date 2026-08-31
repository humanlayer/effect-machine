// @effect-diagnostics strictEffectProvide:off - tests are entry points
import { Effect, Option } from "effect";

import { ActorSystemDefault, ActorSystemService, Machine, State, Event } from "../src/index.js";
import { describe, expect, it, yieldFibers } from "effect-bun-test";

// ============================================================================
// Parent machine
// ============================================================================

const ParentState = State({
  Idle: {},
  Active: {},
  Done: {},
});

const ParentEvent = Event({
  Activate: {},
  Deactivate: {},
  Stop: {},
});

// ============================================================================
// Child machine
// ============================================================================

const ChildState = State({
  Running: {},
  Stopped: {},
});

const ChildEvent = Event({
  Stop: {},
});

const childMachine = Machine.make({
  state: ChildState,
  event: ChildEvent,
  initial: ChildState.Running,
})
  .on(ChildState.Running, ChildEvent.Stop, () => ChildState.Stopped)
  .final(ChildState.Stopped);

// ============================================================================
// Grandchild machine (for nesting tests)
// ============================================================================

const GrandchildState = State({
  Alive: {},
  Dead: {},
});

const GrandchildEvent = Event({
  Kill: {},
});

const grandchildMachine = Machine.make({
  state: GrandchildState,
  event: GrandchildEvent,
  initial: GrandchildState.Alive,
})
  .on(GrandchildState.Alive, GrandchildEvent.Kill, () => GrandchildState.Dead)
  .final(GrandchildState.Dead);

// ============================================================================
// Tests
// ============================================================================

describe("Child Actor Support", () => {
  describe("self.spawn in .spawn() handler", () => {
    it.scopedLive("spawns child visible via parent.system.get", () =>
      Effect.gen(function* () {
        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .on(ParentState.Active, ParentEvent.Stop, () => ParentState.Done)
          .spawn(ParentState.Active, ({ self }) =>
            self.spawn("child-1", childMachine).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ParentState.Done);

        const parent = yield* Machine.spawn(parentMachine);
        yield* parent.start;
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;

        // Child should be visible via parent's system
        const maybeChild = yield* parent.system.get("child-1");
        expect(Option.isSome(maybeChild)).toBe(true);

        const child = maybeChild.pipe(Option.getOrThrow);
        const childState = yield* child.snapshot;
        expect(childState._tag).toBe("Running");

        yield* parent.stop;
      }),
    );

    it.scopedLive("child auto-stopped on state exit", () =>
      Effect.gen(function* () {
        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .on(ParentState.Active, ParentEvent.Deactivate, () => ParentState.Idle)
          .on(ParentState.Active, ParentEvent.Stop, () => ParentState.Done)
          .spawn(ParentState.Active, ({ self }) =>
            self.spawn("child-1", childMachine).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ParentState.Done);

        const parent = yield* Machine.spawn(parentMachine);
        yield* parent.start;
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;

        // Child should exist
        const childBefore = yield* parent.system.get("child-1");
        expect(Option.isSome(childBefore)).toBe(true);

        // Transition out of Active state
        yield* parent.send(ParentEvent.Deactivate);
        yield* Effect.yieldNow;
        yield* yieldFibers;

        // Give the state scope close time to propagate
        yield* Effect.sleep("50 millis");

        // Verify the parent state changed
        const parentState = yield* parent.snapshot;
        expect(parentState._tag).toBe("Idle");

        yield* parent.stop;
      }),
    );

    it.scopedLive("child auto-stopped on parent.stop", () =>
      Effect.gen(function* () {
        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .on(ParentState.Active, ParentEvent.Stop, () => ParentState.Done)
          .spawn(ParentState.Active, ({ self }) =>
            self.spawn("child-1", childMachine).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ParentState.Done);

        const parent = yield* Machine.spawn(parentMachine);
        yield* parent.start;
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;

        // Child should exist
        const childBefore = yield* parent.system.get("child-1");
        expect(Option.isSome(childBefore)).toBe(true);

        // Stop parent — should clean up implicit system and all children
        yield* parent.stop;
      }),
    );

    it.scopedLive("multiple children cleaned up on state transition", () =>
      Effect.gen(function* () {
        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .on(ParentState.Active, ParentEvent.Deactivate, () => ParentState.Idle)
          .spawn(ParentState.Active, ({ self }) =>
            Effect.all([
              self.spawn("child-a", childMachine),
              self.spawn("child-b", childMachine),
              self.spawn("child-c", childMachine),
            ]).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ParentState.Done);

        const parent = yield* Machine.spawn(parentMachine);
        yield* parent.start;
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;

        // All children should exist
        for (const id of ["child-a", "child-b", "child-c"]) {
          const child = yield* parent.system.get(id);
          expect(Option.isSome(child)).toBe(true);
        }

        // Transition out of Active
        yield* parent.send(ParentEvent.Deactivate);
        yield* Effect.yieldNow;
        yield* yieldFibers;
        yield* Effect.sleep("50 millis");

        yield* parent.stop;
      }),
    );
  });

  describe("Machine.spawn implicit system", () => {
    it.scopedLive("actor.system exists on Machine.spawn", () =>
      Effect.gen(function* () {
        const simpleMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .final(ParentState.Done);

        const actor = yield* Machine.spawn(simpleMachine);
        yield* actor.start;
        // actor.system should exist (implicit system)
        expect(actor.system).toBeDefined();
        expect(actor.system.get).toBeDefined();
        expect(actor.system.spawn).toBeDefined();

        yield* actor.stop;
      }),
    );

    it.scopedLive("system.get works on implicit system", () =>
      Effect.gen(function* () {
        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .spawn(ParentState.Active, ({ self }) =>
            self.spawn("worker", childMachine).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ParentState.Done);

        const parent = yield* Machine.spawn(parentMachine);
        yield* parent.start;
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;

        const worker = yield* parent.system.get("worker");
        expect(Option.isSome(worker)).toBe(true);

        yield* parent.stop;
      }),
    );
  });

  describe("system.spawn inherits system", () => {
    it.scopedLive("parent and child share system", () =>
      Effect.gen(function* () {
        const system = yield* ActorSystemService;

        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .spawn(ParentState.Active, ({ self }) =>
            self.spawn("child-via-self", childMachine).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ParentState.Done);

        const parent = yield* system.spawn("parent", parentMachine);
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;

        // parent.system should be the same system
        expect(parent.system).toBe(system);

        // child should be visible via the shared system
        const child = yield* system.get("child-via-self");
        expect(Option.isSome(child)).toBe(true);

        yield* parent.stop;
      }).pipe(Effect.provide(ActorSystemDefault)),
    );
  });

  describe("child of child (grandchild)", () => {
    it.scopedLive("all share one system", () =>
      Effect.gen(function* () {
        // Child machine that spawns grandchild
        const ChildWithGrandchildState = State({
          Idle: {},
          Active: {},
          Done: {},
        });

        const ChildWithGrandchildEvent = Event({
          Activate: {},
          Stop: {},
        });

        const childWithGrandchild = Machine.make({
          state: ChildWithGrandchildState,
          event: ChildWithGrandchildEvent,
          initial: ChildWithGrandchildState.Idle,
        })
          .on(
            ChildWithGrandchildState.Idle,
            ChildWithGrandchildEvent.Activate,
            () => ChildWithGrandchildState.Active,
          )
          .spawn(ChildWithGrandchildState.Active, ({ self }) =>
            self.spawn("grandchild", grandchildMachine).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ChildWithGrandchildState.Done);

        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .spawn(ParentState.Active, ({ self }) =>
            Effect.gen(function* () {
              const child = yield* self.spawn("child", childWithGrandchild).pipe(Effect.orDie);
              yield* child.send(ChildWithGrandchildEvent.Activate);
            }),
          )
          .final(ParentState.Done);

        const system = yield* ActorSystemService;
        const parent = yield* system.spawn("parent", parentMachine);
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;
        yield* Effect.sleep("50 millis");

        // All actors should be in the same system
        const child = yield* system.get("child");
        expect(Option.isSome(child)).toBe(true);

        const grandchild = yield* system.get("grandchild");
        expect(Option.isSome(grandchild)).toBe(true);

        yield* parent.stop;
      }).pipe(Effect.provide(ActorSystemDefault)),
    );
  });

  describe("state effect self.spawn", () => {
    it.scopedLive("spawn handler can spawn children", () =>
      Effect.gen(function* () {
        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .spawn(ParentState.Active, ({ self }) =>
            self.spawn("state-child", childMachine).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ParentState.Done);

        const parent = yield* Machine.spawn(parentMachine);
        yield* parent.start;
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;
        yield* Effect.sleep("50 millis");

        const child = yield* parent.system.get("state-child");
        expect(Option.isSome(child)).toBe(true);

        yield* parent.stop;
      }),
    );
  });

  describe("actor.children", () => {
    it.scopedLive("contains spawned child after self.spawn", () =>
      Effect.gen(function* () {
        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .spawn(ParentState.Active, ({ self }) =>
            self.spawn("child-1", childMachine).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ParentState.Done);

        const parent = yield* Machine.spawn(parentMachine);
        yield* parent.start;
        expect(parent.children.size).toBe(0);

        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;

        expect(parent.children.size).toBe(1);
        expect(parent.children.has("child-1")).toBe(true);

        yield* parent.stop;
      }),
    );

    it.scopedLive("children cleared after state exit (state-scoped)", () =>
      Effect.gen(function* () {
        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .on(ParentState.Active, ParentEvent.Deactivate, () => ParentState.Idle)
          .spawn(ParentState.Active, ({ self }) =>
            self.spawn("child-1", childMachine).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ParentState.Done);

        const parent = yield* Machine.spawn(parentMachine);
        yield* parent.start;
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;

        expect(parent.children.size).toBe(1);

        // Transition out of Active — state scope closes, child removed
        yield* parent.send(ParentEvent.Deactivate);
        yield* Effect.yieldNow;
        yield* yieldFibers;
        yield* Effect.sleep("50 millis");

        expect(parent.children.size).toBe(0);

        yield* parent.stop;
      }),
    );

    it.scopedLive("dynamic children: spawn N workers, iterate", () =>
      Effect.gen(function* () {
        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .spawn(ParentState.Active, ({ self }) =>
            Effect.all([
              self.spawn("worker-0", childMachine),
              self.spawn("worker-1", childMachine),
              self.spawn("worker-2", childMachine),
            ]).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ParentState.Done);

        const parent = yield* Machine.spawn(parentMachine);
        yield* parent.start;
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;

        expect(parent.children.size).toBe(3);
        const ids = [...parent.children.keys()].sort();
        expect(ids).toEqual(["worker-0", "worker-1", "worker-2"]);

        yield* parent.stop;
      }),
    );

    it.scopedLive("background children live for machine lifetime", () =>
      Effect.gen(function* () {
        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .on(ParentState.Active, ParentEvent.Deactivate, () => ParentState.Idle)
          .background(({ self }) =>
            self.spawn("bg-child", childMachine).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ParentState.Done);

        const parent = yield* Machine.spawn(parentMachine);
        yield* parent.start;
        yield* Effect.yieldNow;
        yield* yieldFibers;

        // Background child should be in children
        expect(parent.children.size).toBe(1);
        expect(parent.children.has("bg-child")).toBe(true);

        // State transitions shouldn't remove background children
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;
        yield* parent.send(ParentEvent.Deactivate);
        yield* Effect.yieldNow;
        yield* yieldFibers;
        yield* Effect.sleep("50 millis");

        // Background child still there
        expect(parent.children.has("bg-child")).toBe(true);

        yield* parent.stop;
      }),
    );

    it.scopedLive("grandchild in child.children, not parent.children", () =>
      Effect.gen(function* () {
        const ChildWithGrandchildState = State({
          Idle: {},
          Active: {},
          Done: {},
        });

        const ChildWithGrandchildEvent = Event({
          Activate: {},
          Stop: {},
        });

        const childWithGrandchild = Machine.make({
          state: ChildWithGrandchildState,
          event: ChildWithGrandchildEvent,
          initial: ChildWithGrandchildState.Idle,
        })
          .on(
            ChildWithGrandchildState.Idle,
            ChildWithGrandchildEvent.Activate,
            () => ChildWithGrandchildState.Active,
          )
          .spawn(ChildWithGrandchildState.Active, ({ self }) =>
            self.spawn("grandchild", grandchildMachine).pipe(Effect.asVoid, Effect.orDie),
          )
          .final(ChildWithGrandchildState.Done);

        const parentMachine = Machine.make({
          state: ParentState,
          event: ParentEvent,
          initial: ParentState.Idle,
        })
          .on(ParentState.Idle, ParentEvent.Activate, () => ParentState.Active)
          .spawn(ParentState.Active, ({ self }) =>
            Effect.gen(function* () {
              const child = yield* self.spawn("child", childWithGrandchild).pipe(Effect.orDie);
              yield* child.send(ChildWithGrandchildEvent.Activate);
            }),
          )
          .final(ParentState.Done);

        const system = yield* ActorSystemService;
        const parent = yield* system.spawn("parent", parentMachine);
        yield* parent.send(ParentEvent.Activate);
        yield* Effect.yieldNow;
        yield* yieldFibers;
        yield* Effect.sleep("50 millis");

        // Parent has child, but not grandchild
        expect(parent.children.size).toBe(1);
        expect(parent.children.has("child")).toBe(true);
        expect(parent.children.has("grandchild")).toBe(false);

        // Child has grandchild
        const child = parent.children.get("child")!;
        expect(child.children.has("grandchild")).toBe(true);

        yield* parent.stop;
      }).pipe(Effect.provide(ActorSystemDefault)),
    );
  });
});
