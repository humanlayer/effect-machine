/**
 * Generate Entity definition from a machine.
 *
 * @module
 */
import { Entity } from "effect/unstable/cluster";
import { Rpc } from "effect/unstable/rpc";
import { Schema } from "effect";

import type { Machine } from "../machine.js";
import type { MachineSchemaDefinition } from "../schema.js";
import { MissingSchemaError } from "../errors.js";

/**
 * Options for toEntity.
 */
export interface ToEntityOptions<EntityType extends string = string> {
  /**
   * Entity type name (e.g., "Order", "User")
   */
  readonly type: EntityType;
}

/**
 * Default RPC protocol for entity machines.
 *
 * - `Send` - Send event to machine (fire-and-forget), returns new state
 * - `Ask` - Send event and get domain reply (typed via Event.reply() schemas)
 * - `GetState` - Get current state
 */
const makeEntityRpcs = <StateSchema extends Schema.Top, EventSchema extends Schema.Top>(
  stateSchema: StateSchema,
  eventSchema: EventSchema,
) =>
  [
    Rpc.make("Send", {
      payload: { event: eventSchema },
      success: stateSchema,
    }),
    Rpc.make("Ask", {
      payload: { event: eventSchema },
      success: Schema.Unknown,
    }),
    Rpc.make("GetState", {
      success: stateSchema,
    }),
    Rpc.make("WatchState", {
      success: stateSchema,
      stream: true,
    }),
  ] as const;

/** Canonical Send / Ask / GetState / WatchState protocol for machine entities. */
export type EntityRpcs<StateSchema extends Schema.Top, EventSchema extends Schema.Top> = ReturnType<
  typeof makeEntityRpcs<StateSchema, EventSchema>
>;

/** Entity definition tied to the exact machine and schemas that created it. */
export interface MachineEntity<
  State extends { readonly _tag: string },
  Event extends { readonly _tag: string },
  R,
  EntityType extends string,
  StateDefinition extends Record<string, Schema.Struct.Fields> = Record<
    string,
    Schema.Struct.Fields
  >,
  EventDefinition extends MachineSchemaDefinition = MachineSchemaDefinition,
> extends Entity.Entity<EntityType, EntityRpcs<Schema.Codec<State>, Schema.Codec<Event>>[number]> {
  readonly machine: Machine<State, Event, R, StateDefinition, EventDefinition>;
  readonly stateSchema: Schema.Codec<State>;
  readonly eventSchema: Schema.Codec<Event>;
  readonly rpcs: EntityRpcs<Schema.Codec<State>, Schema.Codec<Event>>;
}

/**
 * Generate an Entity definition from a machine.
 *
 * Creates an Entity with a standard RPC protocol:
 * - `Send(event)` - Process event through machine, returns new state
 * - `GetState()` - Returns current state
 *
 * Schemas are read from the machine - must use `Machine.make({ state, event, initial })`.
 *
 * @example
 * ```ts
 * const OrderState = State({
 *   Pending: { orderId: Schema.String },
 *   Shipped: { trackingId: Schema.String },
 * })
 *
 * const OrderEvent = Event({
 *   Ship: { trackingId: Schema.String },
 * })
 *
 * const orderMachine = Machine.make({
 *   state: OrderState,
 *   event: OrderEvent,
 *   initial: OrderState.Pending({ orderId: "" }),
 * }).pipe(
 *   Machine.on(OrderState.Pending, OrderEvent.Ship, ...),
 * )
 *
 * const OrderEntity = toEntity(orderMachine, { type: "Order" })
 * ```
 */
export const toEntity = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  const EntityType extends string,
  StateDefinition extends Record<string, Schema.Struct.Fields>,
  EventDefinition extends MachineSchemaDefinition,
>(
  machine: Machine<S, E, R, StateDefinition, EventDefinition>,
  options: ToEntityOptions<EntityType>,
): MachineEntity<S, E, R, EntityType, StateDefinition, EventDefinition> => {
  const stateSchema = machine.stateSchema;
  const eventSchema = machine.eventSchema;

  if (stateSchema === undefined || eventSchema === undefined) {
    throw new MissingSchemaError({ operation: "toEntity" });
  }

  const stateCodec = Schema.make<Schema.Codec<S>>(stateSchema.ast);
  const eventCodec = Schema.make<Schema.Codec<E>>(eventSchema.ast);
  const rpcs = makeEntityRpcs(stateCodec, eventCodec);
  return Object.assign(Entity.make(options.type, rpcs), {
    machine,
    stateSchema: stateCodec,
    eventSchema: eventCodec,
    rpcs,
  });
};
