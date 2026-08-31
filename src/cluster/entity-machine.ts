// @effect-diagnostics anyUnknownInErrorContext:off
/**
 * EntityMachine adapter - wires a machine to a cluster Entity layer.
 *
 * Uses Entity.toLayerQueue for a single serialized mailbox per entity.
 * All events (external RPCs + internal self.send) go through the
 * runtime kernel's single queue — no split-mailbox race.
 *
 * Supports opt-in persistence (snapshot or journal strategy) for
 * state survival across entity deactivation/reactivation cycles.
 *
 * @module
 */
import { Entity } from "effect/unstable/cluster";
import type { Envelope, Sharding } from "effect/unstable/cluster";
import type { Rpc } from "effect/unstable/rpc";
import {
  Clock,
  type Duration,
  Effect,
  type Layer,
  Option,
  Queue,
  Ref,
  Schema,
  type Schedule,
  Stream,
  SubscriptionRef,
} from "effect";

import { type Machine, replay } from "../machine.js";
import type { ActorSystemService } from "../actor.js";
import { ActorSystem as ActorSystemTag, makeSystem } from "../actor.js";
import type { ProcessEventHooks } from "../internal/transition.js";
import { createRuntime, type RuntimeQueuedEvent } from "../internal/runtime.js";
import {
  PersistenceAdapter,
  type EntityPersistenceConfig,
  type PersistenceAdapterService,
  type PersistenceKey,
  type PersistedEvent,
  type Snapshot,
} from "./persistence.js";
import type { EntityRpcs, MachineEntity } from "./to-entity.js";

const matchesRpc = <Protocol extends Rpc.Any, Selected extends Protocol>(
  rpc: Selected,
  request: Envelope.Request<Protocol>,
): request is Envelope.Request<Protocol> & Envelope.Request<Selected> => request.tag === rpc._tag;

/**
 * Options for EntityMachine.layer
 */
export interface EntityMachineOptions<S, E> {
  /**
   * Initialize state from entity ID.
   * Called once when entity is first activated.
   */
  readonly initializeState?: (entityId: string) => S;

  /**
   * Optional hooks for inspection/tracing.
   */
  readonly hooks?: ProcessEventHooks<S, E>;

  /**
   * Maximum idle time before entity deactivation.
   * Forwarded to Entity.toLayerQueue.
   */
  readonly maxIdleTime?: Duration.Input;

  /**
   * Mailbox capacity. Default: "unbounded".
   * Forwarded to Entity.toLayerQueue.
   */
  readonly mailboxCapacity?: number | "unbounded";

  /**
   * Disable fatal defects (defects won't crash the entity activation).
   * Forwarded to Entity.toLayerQueue.
   */
  readonly disableFatalDefects?: boolean;

  /**
   * Retry policy for defects (schedule for restarting after defect).
   * Forwarded to Entity.toLayerQueue.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Schedule type needs wide acceptance
  readonly defectRetryPolicy?: Schedule.Schedule<any>;

  /**
   * Persistence configuration. When set, requires PersistenceAdapterService in R.
   */
  readonly persistence?: EntityPersistenceConfig;
}

type EntityOptionsWithoutPersistence<S, E> = Omit<EntityMachineOptions<S, E>, "persistence"> & {
  readonly persistence?: undefined;
};

type EntityOptionsWithPersistence<S, E> = Omit<EntityMachineOptions<S, E>, "persistence"> & {
  readonly persistence: EntityPersistenceConfig;
};

type EntityLayerRequirements<R, Persistence> = R | Persistence | Sharding.Sharding;

type EntityLayer<R, Persistence> = Layer.Layer<
  never,
  never,
  EntityLayerRequirements<R, Persistence>
>;

interface ClusterQueueOptions {
  maxIdleTime?: Duration.Input;
  mailboxCapacity?: number | "unbounded";
  disableFatalDefects?: boolean;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Effect cluster accepts schedules of any output
  defectRetryPolicy?: Schedule.Schedule<any>;
}

/**
 * Create an Entity layer that wires a machine to handle RPC calls.
 *
 * Uses `Entity.toLayerQueue` for a single serialized mailbox per entity.
 * The runtime kernel handles event processing, postpone, background effects,
 * spawn effects, and final state detection.
 *
 * @example
 * ```ts
 * const OrderEntity = toEntity(orderMachine, { type: "Order" })
 *
 * const OrderEntityLayer = EntityMachine.layer(OrderEntity, {
 *   initializeState: (entityId) => OrderState.Pending({ orderId: entityId }),
 * })
 * ```
 */
function layer<
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  EntityType extends string,
>(
  entity: MachineEntity<S, E, R, EntityType>,
  options?: EntityOptionsWithoutPersistence<S, E>,
): EntityLayer<R, never>;
function layer<
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  EntityType extends string,
>(
  entity: MachineEntity<S, E, R, EntityType>,
  options: EntityOptionsWithPersistence<S, E>,
): EntityLayer<R, PersistenceAdapter>;
function layer<
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  EntityType extends string,
>(
  entity: MachineEntity<S, E, R, EntityType>,
  options: EntityMachineOptions<S, E>,
): EntityLayer<R, PersistenceAdapter>;
function layer<
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  EntityType extends string,
>(entity: MachineEntity<S, E, R, EntityType>, options?: EntityMachineOptions<S, E>) {
  type Rpcs = EntityRpcs<Schema.Codec<S>, Schema.Codec<E>>[number];
  const machine = entity.machine;
  const persistence = options?.persistence;

  // Build function receives (queue, replier) from Entity.toLayerQueue
  const build = Effect.gen(function* () {
    // Get entity ID from context (provided by Entity activation)
    const entityId = yield* Effect.serviceOption(Entity.CurrentAddress).pipe(
      Effect.map((opt) => (opt._tag === "Some" ? opt.value.entityId : "")),
    );

    // Resolve actor system from context, or create implicit one
    const existingSystem = yield* Effect.serviceOption(ActorSystemTag);
    const system: ActorSystemService = Option.isSome(existingSystem)
      ? existingSystem.value
      : yield* makeSystem();

    // ----------------------------------------------------------------
    // Persistence: hydration
    // ----------------------------------------------------------------
    const persistCtx = yield* hydratePersistence<S, E>(
      persistence,
      entity,
      entityId,
      machine,
      options?.initializeState,
    );

    // Compute final initial state: hydrated > initializeState > machine.initial
    const initialState =
      persistCtx.hydratedState ??
      (options?.initializeState !== undefined ? options.initializeState(entityId) : undefined);

    // Version tracking
    const versionRef = yield* Ref.make(persistCtx.initialVersion);

    // Cell-owned resources — stable identity for this entity activation
    const computedInitial = initialState ?? machine.initial;
    const stateRef = yield* SubscriptionRef.make(computedInitial);
    const stoppedRef = yield* Ref.make(false);
    const eventQueue = yield* Queue.unbounded<RuntimeQueuedEvent<S, E>>();

    // Create runtime kernel — single queue, sequential processing
    const runtime = yield* createRuntime(machine, system, {
      actorId: entityId,
      initialState: computedInitial,
      hooks: options?.hooks,
      childIdPrefix: `${entityId}/`,
      cellResources: { stateRef, stoppedRef, eventQueue },
    });
    yield* Effect.addFinalizer(() => runtime.stop);
    yield* runtime.start;

    // ----------------------------------------------------------------
    // Persistence: snapshot scheduling
    // ----------------------------------------------------------------
    if (persistCtx.adapter !== undefined) {
      const { adapter: pAdapter, key } = persistCtx;
      const strategy = persistence?.strategy ?? "snapshot";
      const schedule = persistence?.snapshotSchedule;

      if (strategy === "snapshot") {
        // Snapshot-only mode: background scheduler is safe (no journal to tear against)
        yield* SubscriptionRef.changes(runtime.stateRef).pipe(
          schedule !== undefined ? Stream.schedule(schedule) : (s: Stream.Stream<S>) => s,
          Stream.runForEach((state) =>
            Effect.gen(function* () {
              const version = yield* Ref.get(versionRef);
              const now = yield* Clock.currentTimeMillis;
              const encodedState = yield* Schema.encodeEffect(entity.stateSchema)(state).pipe(
                Effect.orDie,
              );
              yield* pAdapter.saveSnapshot(key, {
                state: encodedState,
                version,
                timestamp: now,
              });
            }).pipe(Effect.catch(() => Effect.void)),
          ),
          Effect.forkScoped,
        );
      }
      // Journal mode: no background scheduler — snapshot only on deactivation
      // to avoid state/version tear between concurrent SubscriptionRef and versionRef reads

      // Deactivation finalizer — save final snapshot (safe: runs after event loop stops)
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          const state = yield* SubscriptionRef.get(runtime.stateRef);
          const version = yield* Ref.get(versionRef);
          const now = yield* Clock.currentTimeMillis;
          const encodedState = yield* Schema.encodeEffect(entity.stateSchema)(state).pipe(
            Effect.orDie,
          );
          yield* pAdapter.saveSnapshot(key, {
            state: encodedState,
            version,
            timestamp: now,
          });
        }).pipe(Effect.catch(() => Effect.void)),
      );
    }

    // Return the queue-draining loop function
    return (mailbox: Queue.Dequeue<Envelope.Request<Rpcs>>, replier: Entity.Replier<Rpcs>) =>
      Effect.gen(function* () {
        const hasPersistence = persistCtx.adapter !== undefined;
        const journalCtx =
          hasPersistence && (persistence?.strategy ?? "snapshot") === "journal"
            ? { adapter: persistCtx.adapter, key: persistCtx.key }
            : undefined;

        while (true) {
          const request = yield* Queue.take(mailbox);
          const tag = request.tag;

          switch (tag) {
            case "Send": {
              if (!matchesRpc(entity.rpcs[0], request)) break;
              const event = request.payload.event;
              // sendWait fails on defect — orDie propagates to toLayerQueue infrastructure
              yield* runtime.sendWait(event).pipe(Effect.orDie);

              if (journalCtx !== undefined) {
                // Journal append — inline, before replying. Defects entity on failure.
                yield* persistEvent(
                  journalCtx.adapter,
                  journalCtx.key,
                  versionRef,
                  entity.eventSchema,
                  event,
                );
              } else if (hasPersistence) {
                // Snapshot-only: bump version for consistent snapshot versioning
                yield* Ref.update(versionRef, (v) => v + 1);
              }

              const state = yield* runtime.getState;
              yield* replier.succeed(request, state);
              break;
            }
            case "Ask": {
              if (!matchesRpc(entity.rpcs[1], request)) break;
              const event = request.payload.event;
              const reply = yield* runtime.ask(event).pipe(Effect.orDie);

              if (journalCtx !== undefined) {
                yield* persistEvent(
                  journalCtx.adapter,
                  journalCtx.key,
                  versionRef,
                  entity.eventSchema,
                  event,
                );
              } else if (hasPersistence) {
                yield* Ref.update(versionRef, (v) => v + 1);
              }

              yield* replier.succeed(request, reply);
              break;
            }
            case "GetState": {
              if (!matchesRpc(entity.rpcs[2], request)) break;
              const state = yield* runtime.getState;
              yield* replier.succeed(request, state);
              break;
            }
            case "WatchState": {
              if (!matchesRpc(entity.rpcs[3], request)) break;
              // Streaming RPC — respond with SubscriptionRef.changes stream
              yield* replier.succeed(request, SubscriptionRef.changes(runtime.stateRef));
              break;
            }
            default:
              break;
          }
        }
      });
  });

  // Collect cluster options to forward
  const clusterOptions: ClusterQueueOptions = {};
  if (options?.maxIdleTime !== undefined) clusterOptions.maxIdleTime = options.maxIdleTime;
  if (options?.mailboxCapacity !== undefined)
    clusterOptions.mailboxCapacity = options.mailboxCapacity;
  if (options?.disableFatalDefects !== undefined)
    clusterOptions.disableFatalDefects = options.disableFatalDefects;
  if (options?.defectRetryPolicy !== undefined)
    clusterOptions.defectRetryPolicy = options.defectRetryPolicy;

  return entity.toLayerQueue(
    // orDie: persistence failures during activation are defects (entity retry handles them)
    build.pipe(Effect.orDie),
    Object.keys(clusterOptions).length > 0 ? clusterOptions : undefined,
  );
}

export const EntityMachine = { layer };

// ============================================================================
// Helpers
// ============================================================================

// ============================================================================
// Persistence context
// ============================================================================

type PersistenceContext<S> =
  | {
      readonly adapter: PersistenceAdapterService;
      readonly key: PersistenceKey;
      readonly hydratedState: S | undefined;
      readonly initialVersion: number;
    }
  | {
      readonly adapter: undefined;
      readonly key: undefined;
      readonly hydratedState: undefined;
      readonly initialVersion: 0;
    };

const noPersistence: PersistenceContext<never> = {
  adapter: undefined,
  key: undefined,
  hydratedState: undefined,
  initialVersion: 0,
};

/** Load snapshot/journal and compute hydrated state. */
const hydratePersistence = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
>(
  persistence: EntityPersistenceConfig | undefined,
  entityDef: {
    readonly type: string;
    readonly stateSchema: Schema.Codec<S>;
    readonly eventSchema: Schema.Codec<E>;
  },
  entityId: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Machine type params need wide acceptance
  machine: Machine<S, E, any, any, any>,
  initializeState?: (entityId: string) => S,
) =>
  Effect.gen(function* () {
    if (persistence === undefined) return noPersistence;

    const adapter = yield* PersistenceAdapter;
    const entityType = persistence.machineType ?? entityDef.type;
    const key: PersistenceKey = { entityType, entityId };

    const snapshotSchema = Schema.Struct({
      state: entityDef.stateSchema,
      version: Schema.Number,
      timestamp: Schema.Number,
    });
    const persistedEventSchema = Schema.Struct({
      event: entityDef.eventSchema,
      version: Schema.Number,
      timestamp: Schema.Number,
    });

    const storedSnapshot = yield* adapter.loadSnapshot(key);
    const maybeSnapshot = yield* Option.match(storedSnapshot, {
      onNone: () => Effect.succeed(Option.none<Snapshot<S>>()),
      onSome: (input) =>
        Schema.decodeUnknownEffect(snapshotSchema)(input).pipe(Effect.map(Option.some)),
    });

    const strategy = persistence.strategy ?? "snapshot";

    if (strategy === "journal") {
      const baseState: S = Option.isSome(maybeSnapshot)
        ? maybeSnapshot.value.state
        : initializeState !== undefined
          ? initializeState(entityId)
          : machine.initial;
      const snapshotVersion = Option.isSome(maybeSnapshot) ? maybeSnapshot.value.version : 0;

      const storedEvents = yield* adapter.loadEvents(key, snapshotVersion);
      const events = yield* Effect.forEach(storedEvents, (input) =>
        Schema.decodeUnknownEffect(persistedEventSchema)(input),
      );

      if (events.length > 0) {
        const eventValues = events.map((e: PersistedEvent<E>) => e.event);
        const hydratedState = yield* replay(machine, eventValues, { from: baseState });
        const lastEvent = events[events.length - 1];
        const initialVersion = lastEvent !== undefined ? lastEvent.version : snapshotVersion;
        return { adapter, key, hydratedState, initialVersion };
      }

      return {
        adapter,
        key,
        hydratedState: Option.isSome(maybeSnapshot) ? maybeSnapshot.value.state : undefined,
        initialVersion: snapshotVersion,
      };
    }

    // Snapshot strategy
    if (Option.isSome(maybeSnapshot)) {
      return {
        adapter,
        key,
        hydratedState: maybeSnapshot.value.state,
        initialVersion: maybeSnapshot.value.version,
      };
    }

    return { adapter, key, hydratedState: undefined, initialVersion: 0 };
  });

/**
 * Append a single event to the journal, incrementing version.
 *
 * On failure: defects the entity activation. The cluster's defectRetryPolicy
 * restarts the entity, which rehydrates from the last consistent snapshot +
 * whatever events made it to the journal. This is correct because the in-memory
 * state has already advanced — we can't un-ring that bell — so the activation
 * is now unreliable and must restart.
 */
const persistEvent = <E>(
  adapter: PersistenceAdapterService,
  key: PersistenceKey,
  versionRef: Ref.Ref<number>,
  eventSchema: Schema.Codec<E>,
  event: E,
): Effect.Effect<void, never, never> =>
  Effect.gen(function* () {
    const expectedVersion = yield* Ref.get(versionRef);
    const newVersion = expectedVersion + 1;
    const now = yield* Clock.currentTimeMillis;
    const encodedEvent = yield* Schema.encodeEffect(eventSchema)(event).pipe(Effect.orDie);
    const persisted: PersistedEvent<unknown> = {
      event: encodedEvent,
      version: newVersion,
      timestamp: now,
    };
    yield* adapter.appendEvents(key, [persisted], expectedVersion);
    yield* Ref.set(versionRef, newVersion);
  }).pipe(
    Effect.tapError((error) =>
      Effect.logWarning("Journal append failed, defecting entity", { key, error }),
    ),
    Effect.orDie,
  );
