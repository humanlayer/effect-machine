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
import type { Envelope } from "effect/unstable/cluster";
import type { Rpc } from "effect/unstable/rpc";
import {
  Clock,
  type Duration,
  Effect,
  type Layer,
  Option,
  Queue,
  Ref,
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
 * const OrderEntityLayer = EntityMachine.layer(OrderEntity, orderMachine, {
 *   initializeState: (entityId) => OrderState.Pending({ orderId: entityId }),
 * })
 * ```
 */
export const EntityMachine = {
  layer: <
    S extends { readonly _tag: string },
    E extends { readonly _tag: string },
    R,
    EntityType extends string,
    Rpcs extends Rpc.Any,
  >(
    entity: Entity.Entity<EntityType, Rpcs>,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Machine type params need wide acceptance
    machine: Machine<S, E, R, any, any, any>,
    options?: EntityMachineOptions<S, E>,
  ): Layer.Layer<never, never, R> => {
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

      const machineWithState =
        initialState !== undefined
          ? Object.create(machine, {
              initial: { value: initialState, enumerable: true },
            })
          : machine;

      // Version tracking
      const versionRef = yield* Ref.make(persistCtx.initialVersion);

      // Cell-owned resources — stable identity for this entity activation
      const computedInitial = initialState ?? machine.initial;
      const stateRef = yield* SubscriptionRef.make(computedInitial);
      const stoppedRef = yield* Ref.make(false);
      const eventQueue = yield* Queue.unbounded<RuntimeQueuedEvent<E>>();

      // Create runtime kernel — single queue, sequential processing
      const runtime = yield* createRuntime(machineWithState, system, {
        actorId: entityId,
        hooks: options?.hooks,
        childIdPrefix: `${entityId}/`,
        cellResources: { stateRef, stoppedRef, eventQueue },
      });
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
                yield* pAdapter.saveSnapshot(key, {
                  state,
                  version,
                  timestamp: now,
                } satisfies Snapshot<S>);
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
            yield* pAdapter.saveSnapshot(key, {
              state,
              version,
              timestamp: now,
            } satisfies Snapshot<S>);
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

          // eslint-disable-next-line no-constant-condition
          while (true) {
            const request = yield* Queue.take(mailbox);
            // SAFETY: Envelope.Request is discriminated by its protocol tag at runtime.
            const tag = (request as { readonly tag: string }).tag;

            switch (tag) {
              case "Send": {
                // SAFETY: the Send tag selects the RPC payload carrying machine event E.
                const event = (request as { readonly payload: { readonly event: E } }).payload
                  .event;
                // sendWait fails on defect — orDie propagates to toLayerQueue infrastructure
                yield* runtime.sendWait(event).pipe(Effect.orDie);

                if (journalCtx !== undefined) {
                  // Journal append — inline, before replying. Defects entity on failure.
                  yield* persistEvent(journalCtx.adapter, journalCtx.key, versionRef, event);
                } else if (hasPersistence) {
                  // Snapshot-only: bump version for consistent snapshot versioning
                  yield* Ref.update(versionRef, (v) => v + 1);
                }

                const state = yield* runtime.getState;
                yield* replier.succeed(
                  request,
                  // SAFETY: the Send RPC success schema is the machine state schema S.
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- RPC success type
                  state as any,
                );
                break;
              }
              case "Ask": {
                // SAFETY: the Ask tag selects the RPC payload carrying machine event E.
                const event = (request as { readonly payload: { readonly event: E } }).payload
                  .event;
                const reply = yield* runtime.ask(event).pipe(Effect.orDie);

                if (journalCtx !== undefined) {
                  yield* persistEvent(journalCtx.adapter, journalCtx.key, versionRef, event);
                } else if (hasPersistence) {
                  yield* Ref.update(versionRef, (v) => v + 1);
                }

                yield* replier.succeed(
                  request,
                  // SAFETY: runtime.ask validates replies against the event's registered reply schema.
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- RPC success type
                  reply as any,
                );
                break;
              }
              case "GetState": {
                const state = yield* runtime.getState;
                yield* replier.succeed(
                  request,
                  // SAFETY: the GetState RPC success schema is the machine state schema S.
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- RPC success type
                  state as any,
                );
                break;
              }
              case "WatchState": {
                // Streaming RPC — respond with SubscriptionRef.changes stream
                yield* replier.succeed(
                  request,
                  // SAFETY: WatchState streams values from the machine state SubscriptionRef<S>.
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- streaming RPC success type
                  SubscriptionRef.changes(runtime.stateRef) as any,
                );
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

    // SAFETY: Entity.toLayerQueue hides the machine Effect requirements that build retains as R.
    // eslint-disable-next-line anti-slop/no-chained-type-assertions -- Layer's R parameter is invariant
    return entity.toLayerQueue(
      // orDie: persistence failures during activation are defects (entity retry handles them)
      build.pipe(Effect.orDie),
      Object.keys(clusterOptions).length > 0 ? clusterOptions : undefined,
    ) as unknown as Layer.Layer<never, never, R>;
  },
};

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
  entityDef: { readonly type: string },
  entityId: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Machine type params need wide acceptance
  machine: Machine<S, E, any, any, any, any>,
  initializeState?: (entityId: string) => S,
) =>
  Effect.gen(function* () {
    if (persistence === undefined) return noPersistence;

    const adapter = yield* PersistenceAdapter;
    const entityType = persistence.machineType ?? entityDef.type;
    const key: PersistenceKey = { entityType, entityId };

    // Load snapshot
    // SAFETY: this persistence key belongs to a machine whose state schema is S.
    const maybeSnapshot = yield* adapter.loadSnapshot(key) as Effect.Effect<
      Option.Option<Snapshot<S>>
    >;

    const strategy = persistence.strategy ?? "snapshot";

    if (strategy === "journal") {
      const baseState: S = Option.isSome(maybeSnapshot)
        ? maybeSnapshot.value.state
        : initializeState !== undefined
          ? initializeState(entityId)
          : machine.initial;
      const snapshotVersion = Option.isSome(maybeSnapshot) ? maybeSnapshot.value.version : 0;

      // SAFETY: journal entries for this persistence key were written from events of E.
      const events = (yield* adapter.loadEvents(key, snapshotVersion)) as ReadonlyArray<
        PersistedEvent<E>
      >;

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
  event: E,
): Effect.Effect<void, never, never> =>
  Effect.gen(function* () {
    const expectedVersion = yield* Ref.get(versionRef);
    const newVersion = expectedVersion + 1;
    const now = yield* Clock.currentTimeMillis;
    const persisted: PersistedEvent<unknown> = {
      event,
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
