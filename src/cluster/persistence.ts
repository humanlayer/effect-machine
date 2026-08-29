/**
 * Entity persistence types and adapter interface.
 *
 * Provides snapshot and event journal persistence for entity-machine
 * state across deactivation/reactivation cycles.
 *
 * Two strategies:
 * - **snapshot**: Save full state periodically. Simple, fast.
 * - **journal**: Append events inline, replay on reactivation. Full audit trail.
 *
 * @module
 */
import { type Effect, type Option, type Schedule, Context } from "effect";

import type { PersistenceError, VersionConflictError } from "../errors.js";

// ============================================================================
// Types
// ============================================================================

/** Namespaced key preventing cross-type collisions (e.g. Order/123 vs User/123). */
export interface PersistenceKey {
  readonly entityType: string;
  readonly entityId: string;
}

/** Stored state snapshot with version and timestamp. */
export interface Snapshot<S> {
  readonly state: S;
  readonly version: number;
  readonly timestamp: number;
}

/** Stored event with version and timestamp. */
export interface PersistedEvent<E> {
  readonly event: E;
  readonly version: number;
  readonly timestamp: number;
}

// ============================================================================
// Config
// ============================================================================

/** Persistence configuration for EntityMachineOptions. */
export interface EntityPersistenceConfig {
  /** Persistence strategy. Default: "snapshot". */
  readonly strategy?: "snapshot" | "journal";

  /**
   * Schedule controlling snapshot frequency.
   * Only applies to snapshot strategy (or journal strategy's periodic snapshots).
   * Default: save on every state change.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Schedule types need wide acceptance
  readonly snapshotSchedule?: Schedule.Schedule<any, any>;

  /** Override entityType in the persistence key (defaults to Entity.type). */
  readonly machineType?: string;
}

// ============================================================================
// Adapter interface
// ============================================================================

/** Storage backend for entity state persistence. */
export interface PersistenceAdapterService {
  /** Save a state snapshot. Fails with VersionConflictError if version is stale. */
  readonly saveSnapshot: (
    key: PersistenceKey,
    snapshot: Snapshot<unknown>,
  ) => Effect.Effect<void, PersistenceError | VersionConflictError>;

  /** Load the latest snapshot, or None if no snapshot exists. */
  readonly loadSnapshot: (
    key: PersistenceKey,
  ) => Effect.Effect<Option.Option<Snapshot<unknown>>, PersistenceError>;

  /** Append events to the journal. Fails with VersionConflictError if expectedVersion doesn't match. */
  readonly appendEvents: (
    key: PersistenceKey,
    events: ReadonlyArray<PersistedEvent<unknown>>,
    expectedVersion: number,
  ) => Effect.Effect<void, PersistenceError | VersionConflictError>;

  /** Load events from the journal, optionally after a given version. */
  readonly loadEvents: (
    key: PersistenceKey,
    afterVersion?: number,
  ) => Effect.Effect<ReadonlyArray<PersistedEvent<unknown>>, PersistenceError>;
}

// ============================================================================
// Service tag
// ============================================================================

/** Service tag for PersistenceAdapter — resolve from context for shared infra. */
export class PersistenceAdapter extends Context.Service<
  PersistenceAdapter,
  PersistenceAdapterService
>()("@humanlayer/effect-machine/cluster/persistence/PersistenceAdapter") {}
