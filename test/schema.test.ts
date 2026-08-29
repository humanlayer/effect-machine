// @effect-diagnostics strictEffectProvide:off - tests are entry points
/**
 * State/Event Schema Tests
 *
 * Verifies schema-first State/Event definitions work correctly:
 * - Schema validation/encoding/decoding
 * - Variant constructors
 * - Pattern matching ($is, $match)
 * - Integration with Machine
 */
import { Effect, Schema } from "effect";
import { describe, expect, test } from "bun:test";

import { Machine, State, Event, simulate } from "../src/index.js";

describe("State (schema-first)", () => {
  test("creates variant constructors", () => {
    const OrderState = State({
      Pending: { orderId: Schema.String },
      Shipped: { trackingId: Schema.String },
    });

    const pending = OrderState.Pending({ orderId: "order-123" });
    expect(pending._tag).toBe("Pending");
    expect(pending.orderId).toBe("order-123");

    const shipped = OrderState.Shipped({ trackingId: "track-456" });
    expect(shipped._tag).toBe("Shipped");
    expect(shipped.trackingId).toBe("track-456");
  });

  test("$is type guard works", () => {
    const OrderState = State({
      Pending: { orderId: Schema.String },
      Shipped: { trackingId: Schema.String },
    });

    const pending = OrderState.Pending({ orderId: "123" });
    const shipped = OrderState.Shipped({ trackingId: "456" });

    expect(OrderState.$is("Pending")(pending)).toBe(true);
    expect(OrderState.$is("Shipped")(pending)).toBe(false);
    expect(OrderState.$is("Pending")(shipped)).toBe(false);
    expect(OrderState.$is("Shipped")(shipped)).toBe(true);

    // Invalid values
    expect(OrderState.$is("Pending")(null)).toBe(false);
    expect(OrderState.$is("Pending")(undefined)).toBe(false);
    expect(OrderState.$is("Pending")({ _tag: "Other" })).toBe(false);
  });

  test("$match works (uncurried)", () => {
    const OrderState = State({
      Pending: { orderId: Schema.String },
      Shipped: { trackingId: Schema.String },
    });

    const pending = OrderState.Pending({ orderId: "123" });
    const shipped = OrderState.Shipped({ trackingId: "456" });

    const pendingResult = OrderState.$match(pending, {
      Pending: (s) => `Order ${s.orderId} pending`,
      Shipped: (s) => `Shipped: ${s.trackingId}`,
    });
    expect(pendingResult).toBe("Order 123 pending");

    const shippedResult = OrderState.$match(shipped, {
      Pending: (s) => `Order ${s.orderId} pending`,
      Shipped: (s) => `Shipped: ${s.trackingId}`,
    });
    expect(shippedResult).toBe("Shipped: 456");
  });

  test("$match works (curried)", () => {
    const OrderState = State({
      Pending: { orderId: Schema.String },
      Shipped: { trackingId: Schema.String },
    });

    const matcher = OrderState.$match({
      Pending: (s) => `pending:${s.orderId}`,
      Shipped: (s) => `shipped:${s.trackingId}`,
    });

    expect(matcher(OrderState.Pending({ orderId: "a" }))).toBe("pending:a");
    expect(matcher(OrderState.Shipped({ trackingId: "b" }))).toBe("shipped:b");
  });

  test("works as Schema for decode", () => {
    const OrderState = State({
      Pending: { orderId: Schema.String },
      Shipped: { trackingId: Schema.String },
    });

    // Decode valid data
    const decoded = Schema.decodeUnknownSync(OrderState)({
      _tag: "Pending",
      orderId: "test-order",
    });
    expect(decoded._tag).toBe("Pending");
    expect((decoded as { orderId: string }).orderId).toBe("test-order");

    // Decode another variant
    const decoded2 = Schema.decodeUnknownSync(OrderState)({
      _tag: "Shipped",
      trackingId: "abc",
    });
    expect(decoded2._tag).toBe("Shipped");
  });

  test("works as Schema for encode", () => {
    const OrderState = State({
      Pending: { orderId: Schema.String },
      Shipped: { trackingId: Schema.String },
    });

    const pending = OrderState.Pending({ orderId: "123" });
    const encoded = Schema.encodeSync(OrderState)(pending);

    expect(encoded).toEqual({ _tag: "Pending", orderId: "123" });
  });

  test("validation rejects invalid data", () => {
    const OrderState = State({
      Pending: { orderId: Schema.String },
      Shipped: { trackingId: Schema.String },
    });

    // Invalid _tag
    expect(() => Schema.decodeUnknownSync(OrderState)({ _tag: "Invalid" })).toThrow();

    // Missing required field
    expect(() => Schema.decodeUnknownSync(OrderState)({ _tag: "Pending" })).toThrow();

    // Wrong field type
    expect(() => Schema.decodeUnknownSync(OrderState)({ _tag: "Pending", orderId: 123 })).toThrow();
  });

  test("variants property provides per-variant schemas", () => {
    const OrderState = State({
      Pending: { orderId: Schema.String },
      Shipped: { trackingId: Schema.String },
    });

    // Access individual variant schemas
    expect(OrderState.variants.Pending).toBeDefined();
    expect(OrderState.variants.Shipped).toBeDefined();

    // Can decode individual variant
    const pending = Schema.decodeUnknownSync(OrderState.variants.Pending)({
      _tag: "Pending",
      orderId: "test",
    });
    expect(pending.orderId).toBe("test");
  });

  test("handles empty fields variant", () => {
    const ToggleState = State({
      On: {},
      Off: {},
    });

    // Empty variants are values, not constructors
    const on = ToggleState.On;
    expect(on._tag).toBe("On");

    const off = ToggleState.Off;
    expect(off._tag).toBe("Off");
  });

  test("single-variant schemas retain decoding and narrowing", () => {
    const OnlyState = State({
      Ready: { value: Schema.Number },
    });

    const decoded = Schema.decodeUnknownSync(OnlyState)({ _tag: "Ready", value: 42 });
    if (decoded._tag === "Ready") {
      const value: number = decoded.value;
      expect(value).toBe(42);
    }
  });

  test("rejects an empty definition", () => {
    expect(() => State({})).toThrow();
  });
});

describe("State.with()", () => {
  test("same-state: preserves other fields, overrides specified", () => {
    const TS = State({
      Editor: { text: Schema.String, cursor: Schema.Number },
    });

    const s = TS.Editor({ text: "hello", cursor: 0 });
    const s2 = TS.Editor.with(s, { cursor: 5 });

    expect(s2._tag).toBe("Editor");
    expect(s2.text).toBe("hello");
    expect(s2.cursor).toBe(5);
  });

  test("same-state: no partial returns copy", () => {
    const TS = State({
      A: { x: Schema.Number, y: Schema.String },
    });

    const s = TS.A({ x: 10, y: "hi" });
    const s2 = TS.A.with(s);

    expect(s2._tag).toBe("A");
    expect(s2.x).toBe(10);
    expect(s2.y).toBe("hi");
  });

  test("cross-state: picks only target fields from source", () => {
    const TS = State({
      A: { x: Schema.Number, y: Schema.String },
      B: { x: Schema.Number },
    });

    const a = TS.A({ x: 42, y: "hello" });
    const b = TS.B.with(a);

    expect(b._tag).toBe("B");
    expect(b.x).toBe(42);
    expect(Object.hasOwn(b, "y")).toBe(false);
  });

  test("cross-state: picks + overrides", () => {
    const TS = State({
      A: { x: Schema.Number, y: Schema.String },
      B: { x: Schema.Number, z: Schema.Boolean },
    });

    const a = TS.A({ x: 1, y: "test" });
    const b = TS.B.with(a, { z: true });

    expect(b._tag).toBe("B");
    expect(b.x).toBe(1);
    expect(b.z).toBe(true);
  });

  test("empty variant: with returns tagged value", () => {
    const TS = State({
      Idle: {},
      Active: { value: Schema.Number },
    });

    const active = TS.Active({ value: 5 });
    const idle = TS.Idle.with(active);

    expect(idle._tag).toBe("Idle");
    expect(Object.keys(idle)).toEqual(["_tag"]);
  });

  test("partial overrides win over source fields", () => {
    const TS = State({
      A: { x: Schema.Number, y: Schema.Number },
    });

    const s = TS.A({ x: 1, y: 2 });
    const s2 = TS.A.with(s, { x: 99 });

    expect(s2.x).toBe(99);
    expect(s2.y).toBe(2);
  });

  test("partial cannot override reserved _tag", () => {
    const TS = State({
      A: { x: Schema.Number },
      B: { x: Schema.Number },
    });

    const a = TS.A({ x: 1 });
    const b = TS.B.with(a, { _tag: "A" } as never);

    expect(b._tag).toBe("B");
    expect(b.x).toBe(1);
  });

  test("fields not in target are dropped", () => {
    const TS = State({
      A: { x: Schema.Number, extra: Schema.String },
      B: { x: Schema.Number },
    });

    const a = TS.A({ x: 1, extra: "nope" });
    const b = TS.B.with(a);

    expect(b.x).toBe(1);
    expect(Object.hasOwn(b, "extra")).toBe(false);
  });
});

describe("State.with() (union-level)", () => {
  const TS = State({
    Idle: { queue: Schema.Array(Schema.String), currentAgent: Schema.optional(Schema.String) },
    Streaming: {
      queue: Schema.Array(Schema.String),
      currentAgent: Schema.optional(Schema.String),
      model: Schema.String,
    },
    Done: {},
  });
  type TSType = typeof TS.Type;

  test("preserves variant subtype when deriving on same variant", () => {
    const idle = TS.Idle({ queue: ["a"], currentAgent: "x" });
    const updated = TS.with(idle, { queue: ["b"] });

    expect(updated._tag).toBe("Idle");
    expect(updated.queue).toEqual(["b"]);
    expect(updated.currentAgent).toBe("x");
  });

  test("preserves specific variant type through narrowing", () => {
    const streaming = TS.Streaming({ queue: [], currentAgent: "x", model: "gpt-4" });
    // Type-level: TS.with returns the same specific type
    const updated: Extract<TSType, { _tag: "Streaming" }> = TS.with(streaming, { queue: ["a"] });

    expect(updated._tag).toBe("Streaming");
    expect(updated.model).toBe("gpt-4");
    expect(updated.queue).toEqual(["a"]);
  });

  test("works on union-typed value (runtime dispatch)", () => {
    const states: TSType[] = [
      TS.Idle({ queue: ["old"], currentAgent: "a" }),
      TS.Streaming({ queue: ["old"], currentAgent: "b", model: "gpt" }),
    ];

    for (const state of states) {
      if (state._tag === "Done") continue;
      const updated = TS.with(state, { queue: ["new"] });
      expect(updated._tag).toBe(state._tag);
      expect(updated.queue).toEqual(["new"]);
    }
  });

  test("empty variant with returns tagged value", () => {
    const done = TS.Done;
    const derived = TS.with(done);

    expect(derived._tag).toBe("Done");
    expect(Object.keys(derived)).toEqual(["_tag"]);
  });

  test("partial overrides win over source fields", () => {
    const s = TS.Idle({ queue: ["a"], currentAgent: "old" });
    const updated = TS.with(s, { currentAgent: "new" });

    expect(updated.currentAgent).toBe("new");
    expect(updated.queue).toEqual(["a"]);
  });

  test("works without partial (copy)", () => {
    const s = TS.Streaming({ queue: ["a"], currentAgent: "x", model: "m" });
    const copy = TS.with(s);

    expect(copy._tag).toBe("Streaming");
    expect(copy.queue).toEqual(["a"]);
    expect(copy.model).toBe("m");
  });

  test("partial keys not in target variant are dropped", () => {
    const idle = TS.Idle({ queue: [], currentAgent: "x" });
    // model is not a field of Idle — should be dropped
    const updated = TS.with(idle, { queue: ["a"], model: "gpt-4" } as never);

    expect(updated._tag).toBe("Idle");
    expect(updated.queue).toEqual(["a"]);
    expect(Object.hasOwn(updated, "model")).toBe(false);
  });

  test("throws on unknown _tag", () => {
    const fake = { _tag: "NonExistent", queue: [] } as never;
    expect(() => TS.with(fake)).toThrow();
  });
});

describe("Event.with() (union-level)", () => {
  const TE = Event({
    Start: { input: Schema.String },
    Stop: { reason: Schema.String },
    Ping: {},
  });
  type TEType = typeof TE.Type;

  test("preserves variant subtype", () => {
    const start = TE.Start({ input: "hello" });
    const updated = TE.with(start, { input: "world" });

    expect(updated._tag).toBe("Start");
    expect(updated.input).toBe("world");
  });

  test("works on union-typed event", () => {
    const events: TEType[] = [TE.Start({ input: "a" }), TE.Stop({ reason: "done" })];

    for (const e of events) {
      const copy = TE.with(e);
      expect(copy._tag).toBe(e._tag);
    }
  });

  test("empty variant with returns tagged value", () => {
    const ping = TE.Ping;
    const derived = TE.with(ping);

    expect(derived._tag).toBe("Ping");
  });
});

describe("Event (schema-first)", () => {
  test("creates event constructors", () => {
    const OrderEvent = Event({
      Ship: { trackingId: Schema.String },
      Cancel: { reason: Schema.String },
    });

    const ship = OrderEvent.Ship({ trackingId: "track-123" });
    expect(ship._tag).toBe("Ship");
    expect(ship.trackingId).toBe("track-123");
  });

  test("$is and $match work for events", () => {
    const OrderEvent = Event({
      Ship: { trackingId: Schema.String },
      Cancel: { reason: Schema.String },
    });

    const ship = OrderEvent.Ship({ trackingId: "abc" });

    expect(OrderEvent.$is("Ship")(ship)).toBe(true);
    expect(OrderEvent.$is("Cancel")(ship)).toBe(false);

    const result = OrderEvent.$match(ship, {
      Ship: (e) => `Shipping: ${e.trackingId}`,
      Cancel: (e) => `Cancelled: ${e.reason}`,
    });
    expect(result).toBe("Shipping: abc");
  });

  test("preserves reply schema metadata through dynamic schema construction", () => {
    const QueryEvent = Event({
      GetCount: Event.reply({}, Schema.Number),
      Reset: {},
    });

    expect(QueryEvent._replySchemas.get("GetCount")).toBe(Schema.Number);
    expect(QueryEvent._replySchemas.has("Reset")).toBe(false);
    expect(Schema.decodeUnknownSync(QueryEvent)({ _tag: "GetCount" })._tag).toBe("GetCount");
  });
});

describe("State/Event with Machine", () => {
  test("schema-first types work with Machine.make (using with)", async () => {
    const OrderState = State({
      Pending: { orderId: Schema.String },
      Processing: { orderId: Schema.String },
      Shipped: { orderId: Schema.String, trackingId: Schema.String },
    });
    type OrderState = typeof OrderState.Type;

    const OrderEvent = Event({
      Process: {},
      Ship: { trackingId: Schema.String },
    });
    type OrderEvent = typeof OrderEvent.Type;

    // Machine uses with to carry orderId across states
    const machine = Machine.make({
      state: OrderState,
      event: OrderEvent,
      initial: OrderState.Pending({ orderId: "test-order" }),
    })
      .on(OrderState.Pending, OrderEvent.Process, ({ state }) => OrderState.Processing.with(state))
      .on(OrderState.Processing, OrderEvent.Ship, ({ state, event }) =>
        OrderState.Shipped.with(state, { trackingId: event.trackingId }),
      )
      .final(OrderState.Shipped);

    const result = await Effect.runPromise(
      simulate(machine, [OrderEvent.Process, OrderEvent.Ship({ trackingId: "TRACK-123" })]),
    );

    expect(result.finalState._tag).toBe("Shipped");
    expect((result.finalState as { trackingId: string }).trackingId).toBe("TRACK-123");
    expect((result.finalState as { orderId: string }).orderId).toBe("test-order");
  });

  test("state constructors are compatible with Machine.on", () => {
    const TestState = State({
      A: { value: Schema.Number },
      B: { value: Schema.Number },
    });
    type TestState = typeof TestState.Type;

    const TestEvent = Event({
      Next: {},
    });
    type TestEvent = typeof TestEvent.Type;

    // This should compile - state constructors produce branded types
    const machine = Machine.make({
      state: TestState,
      event: TestEvent,
      initial: TestState.A({ value: 0 }),
    }).on(TestState.A, TestEvent.Next, ({ state }) => TestState.B({ value: state.value + 1 }));

    expect(machine.transitions.length).toBe(1);
  });

  test("fluent from() scopes transitions to a state", async () => {
    const EditorState = State({
      Idle: {},
      Typing: { text: Schema.String },
      Submitted: { text: Schema.String },
    });

    const EditorEvent = Event({
      Focus: {},
      KeyPress: { key: Schema.String },
      Submit: {},
    });

    const machine = Machine.make({
      state: EditorState,
      event: EditorEvent,
      initial: EditorState.Idle,
    })
      .on(EditorState.Idle, EditorEvent.Focus, () => EditorState.Typing({ text: "" }))
      .from(EditorState.Typing, (typing) =>
        typing
          .on(EditorEvent.KeyPress, ({ state, event }) =>
            EditorState.Typing({ text: state.text + event.key }),
          )
          .on(EditorEvent.Submit, ({ state }) => EditorState.Submitted({ text: state.text })),
      )
      .final(EditorState.Submitted);

    // 3 transitions: Idle->Focus, Typing->KeyPress, Typing->Submit
    expect(machine.transitions.length).toBe(3);

    const result = await Effect.runPromise(
      simulate(machine, [
        EditorEvent.Focus,
        EditorEvent.KeyPress({ key: "h" }),
        EditorEvent.KeyPress({ key: "i" }),
        EditorEvent.Submit,
      ]),
    );

    expect(result.finalState._tag).toBe("Submitted");
    expect((result.finalState as { text: string }).text).toBe("hi");
  });

  test("multiple state transitions with multi-state .on()", async () => {
    const WorkflowState = State({
      Draft: {},
      Review: {},
      Approved: {},
      Cancelled: {},
    });

    const WorkflowEvent = Event({
      Submit: {},
      Approve: {},
      Cancel: {},
    });

    const machine = Machine.make({
      state: WorkflowState,
      event: WorkflowEvent,
      initial: WorkflowState.Draft,
    })
      .on(WorkflowState.Draft, WorkflowEvent.Submit, () => WorkflowState.Review)
      .on(WorkflowState.Review, WorkflowEvent.Approve, () => WorkflowState.Approved)
      // Cancel from Draft or Review → Cancelled (multi-state .on)
      .on(
        [WorkflowState.Draft, WorkflowState.Review],
        WorkflowEvent.Cancel,
        () => WorkflowState.Cancelled,
      )
      .final(WorkflowState.Approved)
      .final(WorkflowState.Cancelled);

    // Cancel from Draft
    const result1 = await Effect.runPromise(simulate(machine, [WorkflowEvent.Cancel]));
    expect(result1.finalState._tag).toBe("Cancelled");

    // Cancel from Review
    const result2 = await Effect.runPromise(
      simulate(machine, [WorkflowEvent.Submit, WorkflowEvent.Cancel]),
    );
    expect(result2.finalState._tag).toBe("Cancelled");

    // Normal flow
    const result3 = await Effect.runPromise(
      simulate(machine, [WorkflowEvent.Submit, WorkflowEvent.Approve]),
    );
    expect(result3.finalState._tag).toBe("Approved");
  });
});
