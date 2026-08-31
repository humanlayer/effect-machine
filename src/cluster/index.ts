/**
 * Cluster integration for effect-machine.
 *
 * Provides bridges between effect-machine state machines and @effect/cluster:
 * - `toEntity` - Generate Entity definition from machine
 * - `EntityMachine` - Wire machine to cluster Entity layer
 *
 * @example
 * ```ts
 * import { Machine, MachineSchema } from "@humanlayer/effect-machine"
 * import { toEntity, EntityMachine } from "@humanlayer/effect-machine/cluster"
 *
 * // Schema-first definitions
 * const OrderState = MachineSchema.State({
 *   Pending: { orderId: Schema.String },
 *   Shipped: { trackingId: Schema.String },
 * })
 *
 * const OrderEvent = MachineSchema.Event({
 *   Ship: { trackingId: Schema.String },
 * })
 *
 * // Define machine
 * const orderMachine = Machine.make(OrderState.Pending({ orderId: "" })).pipe(
 *   Machine.on(OrderState.Pending, OrderEvent.Ship, ...)
 * )
 *
 * // Generate Entity
 * const OrderEntity = toEntity(orderMachine, {
 *   type: "Order",
 *   stateSchema: OrderState,
 *   eventSchema: OrderEvent,
 * })
 *
 * // Create layer
 * const OrderEntityLayer = EntityMachine.layer(OrderEntity)
 * ```
 *
 * @module
 */

export {
  toEntity,
  type ToEntityOptions,
  type EntityRpcs,
  type MachineEntity,
} from "./to-entity.js";
export { EntityMachine, type EntityMachineOptions } from "./entity-machine.js";
export { type EntityActorRef, makeEntityActorRef } from "./entity-actor-ref.js";
export {
  type PersistenceKey,
  type Snapshot,
  type PersistedEvent,
  type EntityPersistenceConfig,
  type PersistenceAdapterService as PersistenceAdapterInterface,
  PersistenceAdapter,
} from "./persistence.js";
export { makeInMemoryPersistenceAdapter } from "./adapters/in-memory.js";
