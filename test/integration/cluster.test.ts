// @effect-diagnostics strictEffectProvide:off
// @effect-diagnostics missingEffectContext:off
// @effect-diagnostics missingEffectError:off
// @effect-diagnostics anyUnknownInErrorContext:off
/**
 * Cluster Integration Tests
 *
 * Tests the integration between effect-machine and @effect/cluster using:
 * - MachineSchema for schema-first definitions (single source of truth)
 * - toEntity for generating Entity definitions
 * - EntityMachine.layer for wiring machine to cluster
 */
import { Entity, ShardingConfig } from "effect/unstable/cluster";
import { Rpc } from "effect/unstable/rpc";
import { Duration, Effect, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { describe, expect, test } from "bun:test";

import {
  ActorSystemDefault,
  ActorSystemService,
  Machine,
  simulate,
  State,
  Event,
} from "../../src/index.js";
import { toEntity, EntityMachine, makeEntityActorRef } from "../../src/cluster/index.js";

// =============================================================================
// Schema-first definitions using MachineSchema
// =============================================================================

// State and Event defined once - schema IS the source of truth
const OrderState = State({
  Pending: { orderId: Schema.String },
  Processing: { orderId: Schema.String, startedAt: Schema.Number },
  Shipped: { orderId: Schema.String, trackingId: Schema.String },
  Cancelled: { orderId: Schema.String, reason: Schema.String },
});
type OrderState = typeof OrderState.Type;

const OrderEvent = Event({
  Process: {},
  Ship: { trackingId: Schema.String },
  Cancel: { reason: Schema.String },
});
type OrderEvent = typeof OrderEvent.Type;

// =============================================================================
// Machine definition (same pattern as before)
// =============================================================================

const orderMachine = Machine.make({
  state: OrderState,
  event: OrderEvent,
  initial: OrderState.Pending({ orderId: "" }),
})
  .on(OrderState.Pending, OrderEvent.Process, ({ state }) =>
    OrderState.Processing({ orderId: state.orderId, startedAt: Date.now() }),
  )
  .on(OrderState.Processing, OrderEvent.Ship, ({ state, event }) =>
    OrderState.Shipped({ orderId: state.orderId, trackingId: event.trackingId }),
  )
  .on(OrderState.Pending, OrderEvent.Cancel, ({ state, event }) =>
    OrderState.Cancelled({ orderId: state.orderId, reason: event.reason }),
  )
  .on(OrderState.Processing, OrderEvent.Cancel, ({ state, event }) =>
    OrderState.Cancelled({ orderId: state.orderId, reason: event.reason }),
  )
  .final(OrderState.Shipped)
  .final(OrderState.Cancelled);

// =============================================================================
// Entity definition using toEntity helper
// =============================================================================

// Schemas are attached to machine - no need to pass them!
const OrderEntity = toEntity(orderMachine, { type: "Order" });

// =============================================================================
// Sharding config for tests
// =============================================================================

const TestShardingConfig = ShardingConfig.layer({
  shardsPerGroup: 300,
  entityMailboxCapacity: 10,
  entityTerminationTimeout: 0,
  entityMessagePollInterval: 5000,
  sendRetryInterval: 100,
});

// =============================================================================
// Tests
// =============================================================================

describe("Cluster Integration with MachineSchema", () => {
  test("MachineSchema types work with simulate() (baseline)", async () => {
    // simulate() works great for testing pure state machine logic
    await Effect.runPromise(
      Effect.gen(function* () {
        const machineWithInitial = Machine.make({
          state: OrderState,
          event: OrderEvent,
          initial: OrderState.Pending({ orderId: "order-123" }),
        })
          .on(OrderState.Pending, OrderEvent.Process, ({ state }) =>
            OrderState.Processing({ orderId: state.orderId, startedAt: 1000 }),
          )
          .on(OrderState.Processing, OrderEvent.Ship, ({ state, event }) =>
            OrderState.Shipped({ orderId: state.orderId, trackingId: event.trackingId }),
          )
          .final(OrderState.Shipped);

        const result = yield* simulate(machineWithInitial, [
          OrderEvent.Process,
          OrderEvent.Ship({ trackingId: "TRACK-456" }),
        ]);

        expect(result.finalState._tag).toBe("Shipped");
        expect((result.finalState as { trackingId: string }).trackingId).toBe("TRACK-456");
        expect(result.states.map((s) => s._tag)).toEqual(["Pending", "Processing", "Shipped"]);
      }),
    );
  });

  test("Machine.task works with schema-first machine at runtime", async () => {
    const TaskState = State({
      Idle: {},
      Working: {},
      Done: {},
    });
    type TaskState = typeof TaskState.Type;

    const TaskEvent = Event({
      Start: {},
      Done: {},
    });
    type TaskEvent = typeof TaskEvent.Type;

    const taskMachine = Machine.make({
      state: TaskState,
      event: TaskEvent,
      initial: TaskState.Idle,
    })
      .on(TaskState.Idle, TaskEvent.Start, () => TaskState.Working)
      .on(TaskState.Working, TaskEvent.Done, () => TaskState.Done)
      .task(TaskState.Working, () => Effect.succeed("ok"), {
        onSuccess: () => TaskEvent.Done,
      })
      .final(TaskState.Done);

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const system = yield* ActorSystemService;
          const actor = yield* system.spawn("task", taskMachine);
          yield* actor.send(TaskEvent.Start);
          const finalState = yield* actor.awaitFinal;
          expect(finalState._tag).toBe("Done");
        }),
      ).pipe(Effect.provide(ActorSystemDefault)),
    );
  });

  test("toEntity generates correct Entity definition", () => {
    // toEntity creates an Entity with Send and GetState RPCs
    // OrderEntity.type is branded, so cast to string for comparison
    expect(OrderEntity.type as string).toBe("Order");
    expect(OrderEntity.protocol).toBeDefined();
  });

  test("MachineSchema $match works for pattern matching", () => {
    const state = OrderState.Pending({ orderId: "test-123" });

    const message = OrderState.$match(state, {
      Pending: (s) => `Order ${s.orderId} is pending`,
      Processing: (s) => `Order ${s.orderId} started at ${s.startedAt}`,
      Shipped: (s) => `Order ${s.orderId} shipped: ${s.trackingId}`,
      Cancelled: (s) => `Order ${s.orderId} cancelled: ${s.reason}`,
    });

    expect(message).toBe("Order test-123 is pending");
  });

  test("MachineSchema $is works for type guards", () => {
    const pending = OrderState.Pending({ orderId: "123" });
    const shipped = OrderState.Shipped({ orderId: "123", trackingId: "abc" });

    expect(OrderState.$is("Pending")(pending)).toBe(true);
    expect(OrderState.$is("Shipped")(pending)).toBe(false);
    expect(OrderState.$is("Shipped")(shipped)).toBe(true);
  });

  test("MachineSchema works as Schema for encode/decode", () => {
    const pending = OrderState.Pending({ orderId: "test" });

    // Encode
    const encoded = Schema.encodeSync(OrderState)(pending);
    expect(encoded).toEqual({ _tag: "Pending", orderId: "test" });

    // Decode
    const decoded = Schema.decodeUnknownSync(OrderState)({
      _tag: "Shipped",
      orderId: "123",
      trackingId: "abc",
    });
    expect(decoded._tag).toBe("Shipped");
  });

  test("Machine.findTransitions provides O(1) lookup", () => {
    // Using the indexed lookup
    const transitions = Machine.findTransitions(orderMachine, "Pending", "Process");
    expect(transitions.length).toBe(1);
    expect(transitions[0]?.stateTag).toBe("Pending");
    expect(transitions[0]?.eventTag).toBe("Process");

    // No transition for this pair
    const noMatch = Machine.findTransitions(orderMachine, "Shipped", "Process");
    expect(noMatch.length).toBe(0);
  });
});

// =============================================================================
// Integration tests with Entity.makeTestClient
// =============================================================================

describe("Entity.makeTestClient with machine handler", () => {
  // Counter machine for guard testing
  const CounterState = State({
    Counting: { count: Schema.Number },
    Done: { count: Schema.Number },
  });
  type CounterState = typeof CounterState.Type;

  const CounterEvent = Event({
    Increment: {},
    Finish: {},
  });
  type CounterEvent = typeof CounterEvent.Type;

  const counterMachine = Machine.make({
    state: CounterState,
    event: CounterEvent,
    initial: CounterState.Counting({ count: 0 }),
  })
    .on(CounterState.Counting, CounterEvent.Increment, ({ state }) =>
      state.count < 3 ? CounterState.Counting({ count: state.count + 1 }) : state,
    )
    .on(CounterState.Counting, CounterEvent.Finish, ({ state }) =>
      CounterState.Done({ count: state.count }),
    )
    .final(CounterState.Done);

  // Entity using MachineSchema directly as schemas
  const _CounterEntity = Entity.make("Counter", [
    Rpc.make("Send", { payload: { event: CounterEvent }, success: CounterState }),
    Rpc.make("GetState", { success: CounterState }),
  ]);

  // Entity using MachineSchema for Order as well
  const OrderEntityManual = Entity.make("OrderManual", [
    Rpc.make("Send", { payload: { event: OrderEvent }, success: OrderState }),
    Rpc.make("GetState", { success: OrderState }),
  ]);

  test("Entity.makeTestClient works with MachineSchema", async () => {
    const OrderEntityWithMachine = OrderEntityManual.toLayer(
      Effect.gen(function* () {
        const stateRef = yield* Ref.make<OrderState>(
          OrderState.Pending({ orderId: "will-be-set" }),
        );

        return OrderEntityManual.of({
          Send: (envelope) =>
            Effect.gen(function* () {
              const currentState = yield* Ref.get(stateRef);
              const event = envelope.payload.event;

              const newState = yield* Machine.replay(orderMachine, [event], {
                from: currentState,
              });
              yield* Ref.set(stateRef, newState);
              return newState;
            }),

          GetState: () => Ref.get(stateRef),
        });
      }),
    );

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(OrderEntityManual, OrderEntityWithMachine);
        const client = yield* makeClient("order-123");

        const initialState = yield* client.GetState();
        expect(initialState._tag).toBe("Pending");

        const processingState = yield* client.Send({ event: OrderEvent.Process });
        expect(processingState._tag).toBe("Processing");

        const shippedState = yield* client.Send({
          event: OrderEvent.Ship({ trackingId: "TRACK-789" }),
        });
        expect(shippedState._tag).toBe("Shipped");
        // eslint-disable-next-line @typescript-eslint/no-explicit-any -- test assertion
        expect((shippedState as any).trackingId).toBe("TRACK-789");
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)),
    );
  });

  test("guards work with simulate", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const result = yield* simulate(counterMachine, [
          CounterEvent.Increment,
          CounterEvent.Increment,
          CounterEvent.Increment,
          CounterEvent.Increment, // blocked by guard
          CounterEvent.Finish,
        ]);

        expect(result.finalState._tag).toBe("Done");
        // eslint-disable-next-line @typescript-eslint/no-explicit-any -- test assertion
        expect((result.finalState as any).count).toBe(3);
      }),
    );
  });
});

// =============================================================================
// EntityMachine.layer integration tests (Phase 1: red tests)
//
// These tests exercise EntityMachine.layer through Entity.makeTestClient.
// They pin bugs in the current implementation before fixing them.
// =============================================================================

describe("EntityMachine.layer", () => {
  // ---------------------------------------------------------------------------
  // Test 1: Basic send through EntityMachine.layer
  // ---------------------------------------------------------------------------
  test("basic send changes state via EntityMachine.layer", async () => {
    const entity = toEntity(orderMachine, { type: "OrderSend" });
    const entityLayer = EntityMachine.layer(entity, {
      initializeState: (entityId) => OrderState.Pending({ orderId: entityId }),
    });

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(
          entity,
          entityLayer.pipe(Layer.provide(ActorSystemDefault)),
        );
        const client = yield* makeClient("order-1");

        const state = yield* client.Send({ event: OrderEvent.Process });
        expect(state._tag).toBe("Processing");
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });

  // ---------------------------------------------------------------------------
  // Test 2: Ask reply through EntityMachine.layer
  // ---------------------------------------------------------------------------
  test("ask returns typed reply via EntityMachine.layer", async () => {
    const AskState = State({
      Active: { count: Schema.Number },
    });
    type AskState = typeof AskState.Type;

    const AskEvent = Event({
      GetCount: Event.reply({}, Schema.Number),
      Increment: {},
    });
    type AskEvent = typeof AskEvent.Type;

    const askMachine = Machine.make({
      state: AskState,
      event: AskEvent,
      initial: AskState.Active({ count: 0 }),
    })
      .on(AskState.Active, AskEvent.Increment, ({ state }) =>
        AskState.Active({ count: state.count + 1 }),
      )
      .on(AskState.Active, AskEvent.GetCount, ({ state }) => Machine.reply(state, state.count));

    const entity = toEntity(askMachine, { type: "AskReply" });
    const entityLayer = EntityMachine.layer(entity, {
      initializeState: () => AskState.Active({ count: 42 }),
    });

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(
          entity,
          entityLayer.pipe(Layer.provide(ActorSystemDefault)),
        );
        const client = yield* makeClient("ask-1");

        const ref = makeEntityActorRef(entity, client, "ask-1");
        const reply: number = yield* ref.ask(AskEvent.GetCount);
        expect(reply).toBe(42);
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );

    const invalidReplyLayer = entity.toLayer({
      Send: () => Effect.succeed(AskState.Active({ count: 0 })),
      Ask: () => Effect.succeed("not-a-number"),
      GetState: () => Effect.succeed(AskState.Active({ count: 0 })),
      WatchState: () => Stream.empty,
    });

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(entity, invalidReplyLayer);
        const client = yield* makeClient("invalid-reply");
        const ref = makeEntityActorRef(entity, client, "invalid-reply");
        const exit = yield* Effect.result(ref.ask(AskEvent.GetCount));
        expect(exit._tag).toBe("Failure");
        if (exit._tag === "Failure") {
          expect(Schema.isSchemaError(exit.failure)).toBe(true);
        }
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });

  test("deferred transforming ask decodes once via EntityMachine.layer", async () => {
    const AskState = State({
      Idle: {},
      Replying: {},
    });
    const AskEvent = Event({
      GetCount: Event.reply({}, Schema.NumberFromString),
    });
    const askMachine = Machine.make({
      state: AskState,
      event: AskEvent,
      initial: AskState.Idle,
    })
      .on(AskState.Idle, AskEvent.GetCount, () => Machine.deferReply(AskState.Replying))
      .spawn(AskState.Replying, ({ self }) =>
        Effect.sleep("10 millis").pipe(Effect.andThen(self.reply(42)), Effect.asVoid),
      );
    const entity = toEntity(askMachine, { type: "DeferredTransformAsk" });
    const entityLayer = EntityMachine.layer(entity, {
      initializeState: () => AskState.Idle,
    });

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(
          entity,
          entityLayer.pipe(Layer.provide(ActorSystemDefault)),
        );
        const client = yield* makeClient("deferred-ask-1");
        const ref = makeEntityActorRef(entity, client, "deferred-ask-1");
        const reply: number = yield* ref.ask(AskEvent.GetCount).pipe(Effect.timeout("2 seconds"));
        expect(reply).toBe(42);
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });

  // ---------------------------------------------------------------------------
  // Test 3: Background effects run
  // BUG: entity-machine never iterates machine.backgroundEffects
  // ---------------------------------------------------------------------------
  test("background effects run in entity context", async () => {
    const BgState = State({
      Idle: {},
      Done: { value: Schema.String },
    });
    type BgState = typeof BgState.Type;

    const BgEvent = Event({
      Complete: { value: Schema.String },
    });
    type BgEvent = typeof BgEvent.Type;

    const bgMachine = Machine.make({
      state: BgState,
      event: BgEvent,
      initial: BgState.Idle,
    })
      .on(BgState.Idle, BgEvent.Complete, ({ event }) => BgState.Done({ value: event.value }))
      .background(({ self }) => self.send(BgEvent.Complete({ value: "from-background" })))
      .final(BgState.Done);

    const entity = toEntity(bgMachine, { type: "Background" });
    const entityLayer = EntityMachine.layer(entity, {});

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(
          entity,
          entityLayer.pipe(Layer.provide(ActorSystemDefault)),
        );
        const client = yield* makeClient("bg-1");

        // Give background effect time to fire
        yield* Effect.sleep("100 millis");

        const state = yield* client.GetState();
        expect(state._tag).toBe("Done");
        expect((state as { value: string }).value).toBe("from-background");
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });

  // ---------------------------------------------------------------------------
  // Test 4: Final state stops accepting events
  // BUG: isFinal computed but never checked, entity keeps accepting RPCs
  // ---------------------------------------------------------------------------
  test("final state rejects further events", async () => {
    const entity = toEntity(orderMachine, { type: "OrderFinal" });
    const entityLayer = EntityMachine.layer(entity, {
      initializeState: (entityId) => OrderState.Pending({ orderId: entityId }),
    });

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(
          entity,
          entityLayer.pipe(Layer.provide(ActorSystemDefault)),
        );
        const client = yield* makeClient("final-1");

        // Drive to final state
        yield* client.Send({ event: OrderEvent.Process });
        const shipped = yield* client.Send({ event: OrderEvent.Ship({ trackingId: "T-1" }) });
        expect(shipped._tag).toBe("Shipped");

        // After final, sending more events should not change state
        // (at minimum, state should remain Shipped — not crash, not transition)
        const afterFinal = yield* client.Send({ event: OrderEvent.Process });
        expect(afterFinal._tag).toBe("Shipped");
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });

  // ---------------------------------------------------------------------------
  // Test 5: Spawn effects run on state entry
  // ---------------------------------------------------------------------------
  test("spawn effects run on state entry and self.send delivers", async () => {
    const SpawnState = State({
      Start: {},
      Working: {},
      Done: { result: Schema.String },
    });
    type SpawnState = typeof SpawnState.Type;

    const SpawnEvent = Event({
      Begin: {},
      Finished: { result: Schema.String },
    });
    type SpawnEvent = typeof SpawnEvent.Type;

    const spawnMachine = Machine.make({
      state: SpawnState,
      event: SpawnEvent,
      initial: SpawnState.Start,
    })
      .on(SpawnState.Start, SpawnEvent.Begin, () => SpawnState.Working)
      .on(SpawnState.Working, SpawnEvent.Finished, ({ event }) =>
        SpawnState.Done({ result: event.result }),
      )
      .spawn(SpawnState.Working, ({ self }) =>
        self.send(SpawnEvent.Finished({ result: "spawn-delivered" })),
      )
      .final(SpawnState.Done);

    const entity = toEntity(spawnMachine, { type: "SpawnEffect" });
    const entityLayer = EntityMachine.layer(entity, {});

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(
          entity,
          entityLayer.pipe(Layer.provide(ActorSystemDefault)),
        );
        const client = yield* makeClient("spawn-1");

        yield* client.Send({ event: SpawnEvent.Begin });

        // Give spawn effect time to fire and internal event to process
        yield* Effect.sleep("100 millis");

        const state = yield* client.GetState();
        expect(state._tag).toBe("Done");
        expect((state as { result: string }).result).toBe("spawn-delivered");
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });

  // ---------------------------------------------------------------------------
  // Test 6: Timeout fires
  // ---------------------------------------------------------------------------
  test("timeout fires event after duration", async () => {
    const TimeoutState = State({
      Waiting: {},
      TimedOut: {},
    });
    type TimeoutState = typeof TimeoutState.Type;

    const TimeoutEvent = Event({
      Start: {},
      Expired: {},
    });
    type TimeoutEvent = typeof TimeoutEvent.Type;

    const timeoutMachine = Machine.make({
      state: TimeoutState,
      event: TimeoutEvent,
      initial: TimeoutState.Waiting,
    })
      .on(TimeoutState.Waiting, TimeoutEvent.Start, () => TimeoutState.Waiting)
      .on(TimeoutState.Waiting, TimeoutEvent.Expired, () => TimeoutState.TimedOut)
      .timeout(TimeoutState.Waiting, {
        duration: Duration.millis(50),
        event: TimeoutEvent.Expired,
      })
      .final(TimeoutState.TimedOut);

    const entity = toEntity(timeoutMachine, { type: "Timeout" });
    const entityLayer = EntityMachine.layer(entity, {});

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(
          entity,
          entityLayer.pipe(Layer.provide(ActorSystemDefault)),
        );
        const client = yield* makeClient("timeout-1");

        // Wait for timeout to fire
        yield* Effect.sleep("200 millis");

        const state = yield* client.GetState();
        expect(state._tag).toBe("TimedOut");
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });

  // ---------------------------------------------------------------------------
  // Test 7: Postpone semantics
  // BUG: shouldPostpone imported but never called in entity-machine
  // ---------------------------------------------------------------------------
  test("postponed events drain after state change", async () => {
    const PostponeState = State({
      Connecting: {},
      Connected: {},
      Done: { data: Schema.String },
    });
    type PostponeState = typeof PostponeState.Type;

    const PostponeEvent = Event({
      Connect: {},
      Data: { payload: Schema.String },
    });
    type PostponeEvent = typeof PostponeEvent.Type;

    const postponeMachine = Machine.make({
      state: PostponeState,
      event: PostponeEvent,
      initial: PostponeState.Connecting,
    })
      .on(PostponeState.Connecting, PostponeEvent.Connect, () => PostponeState.Connected)
      .on(PostponeState.Connected, PostponeEvent.Data, ({ event }) =>
        PostponeState.Done({ data: event.payload }),
      )
      .postpone(PostponeState.Connecting, PostponeEvent.Data)
      .final(PostponeState.Done);

    const entity = toEntity(postponeMachine, { type: "Postpone" });
    const entityLayer = EntityMachine.layer(entity, {});

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(
          entity,
          entityLayer.pipe(Layer.provide(ActorSystemDefault)),
        );
        const client = yield* makeClient("postpone-1");

        // Send Data while in Connecting — should be postponed
        yield* client.Send({ event: PostponeEvent.Data({ payload: "hello" }) });

        // State should still be Connecting (Data was postponed)
        const beforeConnect = yield* client.GetState();
        expect(beforeConnect._tag).toBe("Connecting");

        // Connect — should transition, then drain postponed Data
        yield* client.Send({ event: PostponeEvent.Connect });

        // Give time for postpone drain
        yield* Effect.sleep("100 millis");

        const finalState = yield* client.GetState();
        expect(finalState._tag).toBe("Done");
        expect((finalState as { data: string }).data).toBe("hello");
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });

  // ---------------------------------------------------------------------------
  // Test 8: Task success/failure routing
  // ---------------------------------------------------------------------------
  test("task dispatches success event", async () => {
    const TaskState = State({
      Idle: {},
      Running: {},
      Done: { result: Schema.String },
      Failed: { error: Schema.String },
    });
    type TaskState = typeof TaskState.Type;

    const TaskEvent = Event({
      Start: {},
      TaskSucceeded: { result: Schema.String },
      TaskFailed: { error: Schema.String },
    });
    type TaskEvent = typeof TaskEvent.Type;

    const taskMachine = Machine.make({
      state: TaskState,
      event: TaskEvent,
      initial: TaskState.Idle,
    })
      .on(TaskState.Idle, TaskEvent.Start, () => TaskState.Running)
      .on(TaskState.Running, TaskEvent.TaskSucceeded, ({ event }) =>
        TaskState.Done({ result: event.result }),
      )
      .on(TaskState.Running, TaskEvent.TaskFailed, ({ event }) =>
        TaskState.Failed({ error: event.error }),
      )
      .task(TaskState.Running, () => Effect.succeed("task-result"), {
        onSuccess: (result) => TaskEvent.TaskSucceeded({ result }),
      })
      .final(TaskState.Done)
      .final(TaskState.Failed);

    const entity = toEntity(taskMachine, { type: "Task" });
    const entityLayer = EntityMachine.layer(entity, {});

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(
          entity,
          entityLayer.pipe(Layer.provide(ActorSystemDefault)),
        );
        const client = yield* makeClient("task-1");

        yield* client.Send({ event: TaskEvent.Start });

        // Give task time to complete and route success event
        yield* Effect.sleep("200 millis");

        const state = yield* client.GetState();
        expect(state._tag).toBe("Done");
        expect((state as { result: string }).result).toBe("task-result");
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });

  // ---------------------------------------------------------------------------
  // Test 9: Split-mailbox — external + internal events both increment
  // Architectural issue: external RPCs and internal self.send run on separate
  // fibers/queues. Test verifies all events are counted. Will fail if internal
  // events are silently dropped or if state updates are lost.
  // ---------------------------------------------------------------------------
  test("external sends and internal self.send both contribute to state", async () => {
    const RaceState = State({
      Active: { count: Schema.Number },
      Done: { count: Schema.Number },
    });
    type RaceState = typeof RaceState.Type;

    const RaceEvent = Event({
      ExternalIncrement: {},
      InternalIncrement: {},
      Finish: {},
    });
    type RaceEvent = typeof RaceEvent.Type;

    // Spawn effect fires internal increments continuously
    const raceMachine = Machine.make({
      state: RaceState,
      event: RaceEvent,
      initial: RaceState.Active({ count: 0 }),
    })
      .on(RaceState.Active, RaceEvent.ExternalIncrement, ({ state }) =>
        RaceState.Active({ count: state.count + 1 }),
      )
      .on(RaceState.Active, RaceEvent.InternalIncrement, ({ state }) =>
        RaceState.Active({ count: state.count + 1 }),
      )
      .on(RaceState.Active, RaceEvent.Finish, ({ state }) => RaceState.Done({ count: state.count }))
      .spawn(RaceState.Active, ({ self }) =>
        // Fire internal increments — each goes through a separate queue/fiber
        Effect.gen(function* () {
          for (let i = 0; i < 10; i++) {
            yield* self.send(RaceEvent.InternalIncrement);
          }
        }),
      )
      .final(RaceState.Done);

    const entity = toEntity(raceMachine, { type: "Race" });
    const entityLayer = EntityMachine.layer(entity, {});

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(
          entity,
          entityLayer.pipe(Layer.provide(ActorSystemDefault)),
        );
        const client = yield* makeClient("race-1");

        // Fire external increments concurrently — each hits processEvent directly
        // while internal ones are being processed by the queue fiber
        yield* Effect.all(
          Array.from({ length: 10 }, () => client.Send({ event: RaceEvent.ExternalIncrement })),
          { concurrency: "unbounded" },
        );

        // Give internal events time to finish
        yield* Effect.sleep("200 millis");

        yield* client.Send({ event: RaceEvent.Finish });
        const state = yield* client.GetState();
        expect(state._tag).toBe("Done");

        // If properly serialized: 10 external + 10 internal = 20
        // With split-mailbox, external RPCs mutate stateRef directly while
        // internal queue reads stale state — lost updates. Count will be < 20.
        expect((state as { count: number }).count).toBe(20);
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });

  // ---------------------------------------------------------------------------
  // Test 10: self.spawn works in entity context (implicit ActorSystem)
  // ---------------------------------------------------------------------------
  test("self.spawn works without explicit ActorSystem", async () => {
    const SpawnChildState = State({
      Idle: {},
      HasChild: { childSpawned: Schema.Boolean },
    });
    type SpawnChildState = typeof SpawnChildState.Type;

    const SpawnChildEvent = Event({
      Go: {},
      ChildReady: {},
    });
    type SpawnChildEvent = typeof SpawnChildEvent.Type;

    // Simple child machine
    const childState = State({ Running: {} });
    const childEvent = Event({ Ping: {} });
    const childMachine = Machine.make({
      state: childState,
      event: childEvent,
      initial: childState.Running,
    });

    const spawnChildMachine = Machine.make({
      state: SpawnChildState,
      event: SpawnChildEvent,
      initial: SpawnChildState.Idle,
    })
      .on(SpawnChildState.Idle, SpawnChildEvent.Go, () =>
        SpawnChildState.HasChild({ childSpawned: false }),
      )
      .on(SpawnChildState.HasChild, SpawnChildEvent.ChildReady, () =>
        SpawnChildState.HasChild({ childSpawned: true }),
      )
      .spawn(SpawnChildState.HasChild, ({ self }) =>
        Effect.gen(function* () {
          yield* self.spawn("child-1", childMachine).pipe(Effect.orDie);
          yield* self.send(SpawnChildEvent.ChildReady);
        }),
      );

    const entity = toEntity(spawnChildMachine, { type: "SpawnChild" });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const entityLayer = EntityMachine.layer(entity, {});

    await Effect.runPromise(
      Effect.gen(function* () {
        // No ActorSystemDefault provided — implicit system should be created
        const makeClient = yield* Entity.makeTestClient(entity, entityLayer);
        const client = yield* makeClient("spawn-child-1");

        yield* client.Send({ event: SpawnChildEvent.Go });
        yield* Effect.sleep("100 millis");

        const state = yield* client.GetState();
        expect(state._tag).toBe("HasChild");
        expect((state as { childSpawned: boolean }).childSpawned).toBe(true);
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });

  // ---------------------------------------------------------------------------
  // Test 11: WatchState streaming RPC delivers state changes
  test("WatchState streams state changes", async () => {
    const WatchState = State({
      A: {},
      B: {},
      C: {},
    });
    type WatchState = typeof WatchState.Type;

    const WatchEvent = Event({
      GoB: {},
      GoC: {},
    });
    type WatchEvent = typeof WatchEvent.Type;

    const watchMachine = Machine.make({
      state: WatchState,
      event: WatchEvent,
      initial: WatchState.A,
    })
      .on(WatchState.A, WatchEvent.GoB, () => WatchState.B)
      .on(WatchState.B, WatchEvent.GoC, () => WatchState.C)
      .final(WatchState.C);

    const entity = toEntity(watchMachine, { type: "Watch" });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const entityLayer = EntityMachine.layer(entity, {});

    await Effect.runPromise(
      Effect.gen(function* () {
        const makeClient = yield* Entity.makeTestClient(
          entity,
          entityLayer.pipe(Layer.provide(ActorSystemDefault)),
        );
        const client = yield* makeClient("watch-1");

        // Collect state changes in background via WatchState streaming RPC
        const collected: string[] = [];
        const watchStream = client.WatchState();

        const collectFiber = yield* Effect.forkScoped(
          watchStream.pipe(
            Stream.take(3),
            Stream.runForEach((s: WatchState) =>
              Effect.sync(() => {
                collected.push(s._tag);
              }),
            ),
          ),
        );

        // Give stream subscription time to establish
        yield* Effect.sleep("50 millis");

        // Drive state changes
        yield* client.Send({ event: WatchEvent.GoB });
        yield* client.Send({ event: WatchEvent.GoC });

        // Wait for stream to collect
        yield* Effect.sleep("200 millis");
        yield* Fiber.interrupt(collectFiber);

        // Stream includes initial state A, then transitions B, C
        expect(collected).toContain("A");
        expect(collected).toContain("B");
        expect(collected).toContain("C");
      }).pipe(Effect.scoped, Effect.provide(TestShardingConfig)) as Effect.Effect<void>,
    );
  });
});
