/**
 * Actor system: spawning, lifecycle, and event processing.
 *
 * Combines:
 * - ActorRef interface (running actor handle)
 * - ActorSystem service (spawn/stop/get actors)
 * - Actor creation and event loop
 */
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  MutableHashMap,
  Option,
  PubSub,
  Queue,
  Ref,
  Schedule,
  Scope,
  Semaphore,
  Context,
  Stream,
  SubscriptionRef,
} from "effect";

import type { Machine, Lifecycle, LifecycleEvent } from "./machine.js";
import { materializeMachine } from "./machine.js";
import { ActorExit, type Supervision } from "./supervision.js";
import type { ReplyTypeBrand, ExtractReply } from "./internal/brands.js";
import type { SlotsDef, ProvideSlots } from "./slot.js";
import type { InspectorService } from "./inspection.js";
import { Inspector as InspectorTag } from "./inspection.js";
import { resolveTransition } from "./internal/transition.js";
import type { ProcessEventHooks, ProcessEventResult } from "./internal/transition.js";
import { emitWithTimestamp } from "./internal/inspection.js";
import type { NoReplyError } from "./errors.js";
import { DuplicateActorError, ActorStoppedError } from "./errors.js";
import {
  createRuntime,
  type RuntimeLifecycleHooks,
  type RuntimeQueuedEvent,
  type RuntimeHandle,
} from "./internal/runtime.js";

// Re-export for external use (cluster)
export { resolveTransition, runSpawnEffects, processEventCore } from "./internal/transition.js";
export type {
  ProcessEventError,
  ProcessEventHooks,
  ProcessEventResult,
} from "./internal/transition.js";

// ============================================================================
// QueuedEvent — re-export from runtime kernel
// ============================================================================

/** Discriminated mailbox request — alias for RuntimeQueuedEvent */
export type QueuedEvent<E> = RuntimeQueuedEvent<E>;

// ============================================================================
// ActorRef Interface
// ============================================================================

/**
 * Sync projection of ActorRef for non-Effect boundaries (React hooks, framework callbacks).
 */
export interface ActorRefSync<State extends { readonly _tag: string }, Event> {
  readonly send: (event: Event) => void;
  readonly stop: () => void;
  readonly snapshot: () => State;
  readonly matches: (tag: State["_tag"]) => boolean;
  readonly can: (event: Event) => boolean;
}

/**
 * Information about a successful transition.
 * Emitted on the `transitions` stream after each accepted event.
 */
export interface TransitionInfo<State, Event> {
  readonly fromState: State;
  readonly toState: State;
  readonly event: Event;
}

type AnyState = { readonly _tag: string };

/**
 * Sound view of an actor whose concrete state and event types are not known.
 * Heterogeneous registries expose this surface instead of pretending that an
 * unknown actor can safely receive arbitrary events.
 */
export interface ActorHandle {
  readonly id: string;
  readonly stop: Effect.Effect<void>;
  readonly start: Effect.Effect<void>;
  readonly snapshot: Effect.Effect<AnyState>;
  readonly awaitExit: Effect.Effect<ActorExit<AnyState>>;
  readonly watch: (other: {
    readonly id: string;
    readonly awaitExit: Effect.Effect<ActorExit<unknown>>;
  }) => Effect.Effect<ActorExit<unknown>>;
  readonly drain: Effect.Effect<void>;
  readonly sync: {
    readonly stop: () => void;
    readonly snapshot: () => AnyState;
  };
  readonly system: ActorSystemService;
  readonly children: ReadonlyMap<string, ActorHandle>;
}

export interface ActorRef<State extends AnyState, Event> extends ActorHandle {
  readonly id: string;

  /** Send an event (fire-and-forget). */
  readonly send: (event: Event) => Effect.Effect<void>;

  /** Fire-and-forget alias for send (OTP gen_server:cast). */
  readonly cast: (event: Event) => Effect.Effect<void>;

  /**
   * Serialized request-reply (OTP gen_server:call).
   * Event is processed through the queue; caller gets ProcessEventResult back.
   */
  readonly call: (event: Event) => Effect.Effect<ProcessEventResult<State>>;

  /**
   * Typed request-reply. Accepts only events with a reply schema
   * (defined via `Event.reply()`). Return type is inferred from the schema.
   * Fails with NoReplyError if the handler doesn't provide a reply.
   */
  readonly ask: <E extends Event & ReplyTypeBrand<unknown>>(
    event: E,
  ) => Effect.Effect<ExtractReply<E>, NoReplyError | ActorStoppedError>;

  /** Observable state. */
  readonly state: SubscriptionRef.SubscriptionRef<State>;

  /** Stop the actor gracefully. */
  readonly stop: Effect.Effect<void>;

  /**
   * Start the actor — fork event loop, background effects, spawn effects.
   * Idempotent: first caller runs initialization, subsequent callers await completion.
   * Events sent before start() are queued and processed when start() runs.
   *
   * Called automatically by `system.spawn`. For `Machine.spawn`, the caller
   * must call `start` explicitly.
   */
  readonly start: Effect.Effect<void>;

  /** Get current state snapshot. */
  readonly snapshot: Effect.Effect<State>;

  /** Check if current state matches tag. */
  readonly matches: (tag: State["_tag"]) => Effect.Effect<boolean>;

  /** Check if event can be handled in current state. */
  readonly can: (event: Event) => Effect.Effect<boolean>;

  /** Stream of state changes. */
  readonly changes: Stream.Stream<State>;

  /**
   * Stream of accepted transitions (edge stream).
   *
   * Emits `{ fromState, toState, event }` on every successful transition,
   * including same-state reenters. PubSub-backed — late subscribers miss
   * past edges. This is observational, not a durability guarantee.
   */
  readonly transitions: Stream.Stream<TransitionInfo<State, Event>>;

  /** Wait for a state matching predicate or variant (includes current snapshot). */
  readonly waitFor: {
    (predicate: (state: State) => boolean): Effect.Effect<State>;
    (state: { readonly _tag: State["_tag"] }): Effect.Effect<State>;
  };

  /** Wait for a final state (includes current snapshot). */
  readonly awaitFinal: Effect.Effect<State>;

  /** Send event and wait for predicate, state variant, or final state. */
  readonly sendAndWait: {
    (event: Event, predicate: (state: State) => boolean): Effect.Effect<State>;
    (event: Event, state: { readonly _tag: State["_tag"] }): Effect.Effect<State>;
    (event: Event): Effect.Effect<State>;
  };

  /** Subscribe to state changes (sync callback). Returns unsubscribe function. */
  readonly subscribe: (fn: (state: State) => void) => () => void;

  /**
   * Wait for this actor's terminal exit. Resolves with the exit reason.
   * Set exactly once when the actor terminates (final, stop, drain, or defect).
   */
  readonly awaitExit: Effect.Effect<ActorExit<State>>;

  /**
   * Watch another actor. Returns an Effect that resolves with the exit reason
   * when the watched actor terminally stops. Ignores restarts (Step 3).
   * Built on the other actor's exitDeferred — authoritative, not system events.
   */
  readonly watch: (other: {
    readonly id: string;
    readonly awaitExit: Effect.Effect<ActorExit<unknown>>;
  }) => Effect.Effect<ActorExit<unknown>>;

  /**
   * Drain: process all remaining events in the queue, then stop.
   * Unlike `stop` (which interrupts immediately), `drain` lets the actor finish its work.
   */
  readonly drain: Effect.Effect<void>;

  /** Sync helpers for non-Effect boundaries. */
  readonly sync: ActorRefSync<State, Event>;

  /** The actor system this actor belongs to. */
  readonly system: ActorSystemService;

  /** Child actors spawned via `self.spawn` in this actor's handlers. */
  readonly children: ReadonlyMap<string, ActorHandle>;
}

// ============================================================================
// ActorSystem Interface
// ============================================================================

interface MutableCell<T> {
  current: T;
}

// eslint-disable-next-line typescript/no-explicit-any, anti-slop/no-unsafe-dictionary-type -- deprecated slots erase handlers here
type LegacySlotHandlers = Record<string, any>;

interface PendingReply {
  readonly failStopped: (error: ActorStoppedError) => Effect.Effect<void>;
}

const pendingReply = <A, E>(
  deferred: Deferred.Deferred<A, ActorStoppedError | E>,
): PendingReply => ({
  failStopped: (error) => Deferred.fail(deferred, error).pipe(Effect.asVoid),
});

const isStatePredicate = <S extends { readonly _tag: string }>(
  value: ((state: S) => boolean) | { readonly _tag: S["_tag"] },
): value is (state: S) => boolean => typeof value === "function" && !("_tag" in value);

// ============================================================================
// System Observation Types
// ============================================================================

/**
 * Events emitted by the ActorSystem when actors are spawned or stopped.
 */
export type SystemEvent =
  | {
      readonly _tag: "ActorSpawned";
      readonly id: string;
      readonly actor: ActorHandle;
    }
  | {
      readonly _tag: "ActorRestarted";
      readonly id: string;
      readonly actor: ActorHandle;
      readonly generation: number;
      readonly exit: ActorExit<unknown>;
    }
  | {
      readonly _tag: "ActorStopped";
      readonly id: string;
      readonly actor: ActorHandle;
      readonly exit: ActorExit<unknown>;
    };

/**
 * Listener callback for system events.
 */
export type SystemEventListener = (event: SystemEvent) => void;

/**
 * Actor system for managing actor lifecycles
 */
export interface ActorSystemService {
  /**
   * Spawn a new actor with the given machine.
   *
   * @example
   * ```ts
   * const actor = yield* system.spawn("my-actor", machine);
   * ```
   */
  readonly spawn: <
    S extends { readonly _tag: string },
    E extends { readonly _tag: string },
    R,
    SD extends SlotsDef = Record<string, never>,
  >(
    id: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    machine: Machine<S, E, R, any, any, SD>,
    options?: {
      readonly supervision?: Supervision.Policy;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      readonly slots?: ProvideSlots<SD, any>;
      readonly lifecycle?: Lifecycle<S, E>;
    },
  ) => Effect.Effect<ActorRef<S, E>, DuplicateActorError, R>;

  /**
   * Get an existing actor by ID
   */
  readonly get: (id: string) => Effect.Effect<Option.Option<ActorHandle>>;

  /**
   * Stop an actor by ID
   */
  readonly stop: (id: string) => Effect.Effect<boolean>;

  /**
   * Async stream of system events (actor spawned/stopped).
   * Each subscriber gets their own queue — late subscribers miss prior events.
   */
  readonly events: Stream.Stream<SystemEvent>;

  /**
   * Sync snapshot of all currently registered actors.
   * Returns a new Map on each access (not live).
   */
  readonly actors: ReadonlyMap<string, ActorHandle>;

  /**
   * Subscribe to system events synchronously.
   * Returns an unsubscribe function.
   */
  readonly subscribe: (fn: SystemEventListener) => () => void;
}

/**
 * ActorSystem service tag
 */
export class ActorSystem extends Context.Service<ActorSystem, ActorSystemService>()(
  "@humanlayer/effect-machine/actor/ActorSystem",
) {}

/**
 * Explicit scope for actor lifecycle management.
 *
 * When present in context, actors attach cleanup finalizers to this scope.
 * This replaces ambient `Scope.Scope` detection which caused bugs where
 * unrelated scopes would tear down actors unexpectedly.
 *
 * Provide via `Machine.scoped` or `Effect.provideService(ActorScope, scope)`.
 */
export class ActorScope extends Context.Service<ActorScope, Scope.Scope>()(
  "@humanlayer/effect-machine/actor/ActorScope",
) {}

// ============================================================================
// Actor Core Helpers
// ============================================================================

/** Listener set for sync subscriptions */
export type Listeners<S> = Set<(state: S) => void>;

/**
 * Notify all listeners of state change.
 */
export const notifyListeners = <S>(listeners: Listeners<S>, state: S): void => {
  for (const listener of listeners) {
    try {
      listener(state);
    } catch {
      // Ignore listener failures to avoid crashing the actor loop
    }
  }
};

/**
 * Build core ActorRef methods.
 */
export const buildActorRefCore = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef,
>(
  id: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Schema fields need wide acceptance
  machine: Machine<S, E, R, any, any, SD>,
  stateRef: SubscriptionRef.SubscriptionRef<S>,
  eventQueueRef: Ref.Ref<Queue.Queue<QueuedEvent<E>>>,
  stoppedRef: Ref.Ref<boolean>,
  listeners: Listeners<S>,
  stop: Effect.Effect<void>,
  start: Effect.Effect<void>,
  system: ActorSystemService,
  childrenMap: ReadonlyMap<string, ActorHandle>,
  pendingReplies: Set<PendingReply>,
  transitionsPubSub: PubSub.PubSub<TransitionInfo<S, E>> | undefined,
  exitDeferred: Deferred.Deferred<ActorExit<S>>,
): ActorRef<S, E> => {
  const send = Effect.fn("effect-machine.actor.send")(function* (event: E) {
    const stopped = yield* Ref.get(stoppedRef);
    if (stopped) {
      return;
    }
    const q = yield* Ref.get(eventQueueRef);
    yield* Queue.offer(q, { _tag: "send", event });
  });

  const call = Effect.fn("effect-machine.actor.call")(function* (event: E) {
    const stopped = yield* Ref.get(stoppedRef);
    if (stopped) {
      yield* Effect.logWarning("effect-machine.actor.call.stopped").pipe(
        Effect.annotateLogs({ actorId: id, eventTag: event._tag }),
      );
      const currentState = yield* SubscriptionRef.get(stateRef);
      return {
        newState: currentState,
        previousState: currentState,
        transitioned: false,
        hasReply: false,
        deferReply: false,
        reply: undefined,
        postponed: false,
        lifecycleRan: false,
        isFinal: machine.finalStates.has(currentState._tag),
      } satisfies ProcessEventResult<S>;
    }
    const reply = yield* Deferred.make<
      ProcessEventResult<{ readonly _tag: string }>,
      ActorStoppedError
    >();
    const pending = pendingReply(reply);
    // SAFETY: the runtime queue reports ActorStoppedError through its wider unknown error channel.
    const queuedReply = reply as Deferred.Deferred<
      ProcessEventResult<{ readonly _tag: string }>,
      unknown
    >;
    pendingReplies.add(pending);
    const q = yield* Ref.get(eventQueueRef);
    yield* Queue.offer(q, {
      _tag: "call",
      event,
      reply: queuedReply,
    });
    const result = yield* Deferred.await(reply).pipe(
      Effect.ensuring(Effect.sync(() => pendingReplies.delete(pending))),
      Effect.catchTag("ActorStoppedError", () =>
        SubscriptionRef.get(stateRef).pipe(
          Effect.map(
            (currentState) =>
              ({
                newState: currentState,
                previousState: currentState,
                transitioned: false,
                hasReply: false,
                deferReply: false,
                reply: undefined,
                postponed: false,
                lifecycleRan: false,
                isFinal: machine.finalStates.has(currentState._tag),
              }) satisfies ProcessEventResult<S>,
          ),
        ),
      ),
    );
    // SAFETY: this ActorRef and its queue share the same state type S.
    return result as ProcessEventResult<S>;
  });

  const ask = Effect.fn("effect-machine.actor.ask")(function* (event: E) {
    const stopped = yield* Ref.get(stoppedRef);
    if (stopped) {
      return yield* new ActorStoppedError({ actorId: id });
    }
    const reply = yield* Deferred.make<unknown, NoReplyError | ActorStoppedError>();
    const pending = pendingReply(reply);
    // SAFETY: queue processing emits NoReplyError; ActorStoppedError is managed by pending reply cleanup.
    const queuedReply = reply as Deferred.Deferred<unknown, NoReplyError>;
    pendingReplies.add(pending);
    const q = yield* Ref.get(eventQueueRef);
    yield* Queue.offer(q, {
      _tag: "ask",
      event,
      reply: queuedReply,
    });
    return yield* Deferred.await(reply).pipe(
      Effect.ensuring(Effect.sync(() => pendingReplies.delete(pending))),
    );
  });

  const snapshot = SubscriptionRef.get(stateRef).pipe(
    Effect.withSpan("effect-machine.actor.snapshot"),
  );

  const matches = Effect.fn("effect-machine.actor.matches")(function* (tag: S["_tag"]) {
    const state = yield* SubscriptionRef.get(stateRef);
    return state._tag === tag;
  });

  const can = Effect.fn("effect-machine.actor.can")(function* (event: E) {
    const state = yield* SubscriptionRef.get(stateRef);
    return resolveTransition(machine, state, event) !== undefined;
  });

  const waitFor = Effect.fn("effect-machine.actor.waitFor")(function* (
    predicateOrState: ((state: S) => boolean) | { readonly _tag: S["_tag"] },
  ) {
    const predicate = isStatePredicate(predicateOrState)
      ? predicateOrState
      : (s: S) => s._tag === predicateOrState._tag;

    // Check current state first — SubscriptionRef.get acquires/releases
    // the semaphore quickly (read-only), no deadlock risk.
    const current = yield* SubscriptionRef.get(stateRef);
    if (predicate(current)) return current;

    // Use sync listener + Deferred to avoid holding the SubscriptionRef
    // semaphore for the duration of a stream (which causes deadlock when
    // send triggers SubscriptionRef.set concurrently).
    const done = yield* Deferred.make<S>();
    // @effect-diagnostics runEffectInsideEffect:off
    const listener = (state: S) => {
      if (predicate(state)) {
        // Sync callback context — not inside Effect.gen
        Effect.runFork(Deferred.succeed(done, state));
      }
    };
    // @effect-diagnostics runEffectInsideEffect:on
    listeners.add(listener);

    // Re-check after subscribing to close the race window
    const afterSubscribe = yield* SubscriptionRef.get(stateRef);
    if (predicate(afterSubscribe)) {
      listeners.delete(listener);
      return afterSubscribe;
    }

    const result = yield* Deferred.await(done);
    listeners.delete(listener);
    return result;
  });

  const awaitFinal = waitFor((state) => machine.finalStates.has(state._tag)).pipe(
    Effect.withSpan("effect-machine.actor.awaitFinal"),
  );

  const sendAndWait = Effect.fn("effect-machine.actor.sendAndWait")(function* (
    event: E,
    predicateOrState?: ((state: S) => boolean) | { readonly _tag: S["_tag"] },
  ) {
    yield* send(event);
    if (predicateOrState !== undefined) {
      return yield* waitFor(predicateOrState);
    }
    return yield* awaitFinal;
  });

  return {
    id,
    send,
    cast: send,
    call,
    // SAFETY: the public ask signature narrows events using the reply brand carried by E.
    ask: ask as ActorRef<S, E>["ask"],
    state: stateRef,
    stop,
    start,
    snapshot,
    matches,
    can,
    changes: SubscriptionRef.changes(stateRef),
    transitions:
      transitionsPubSub !== undefined ? Stream.fromPubSub(transitionsPubSub) : Stream.empty,
    waitFor,
    awaitFinal,
    sendAndWait,
    subscribe: (fn) => {
      listeners.add(fn);
      return () => {
        listeners.delete(fn);
      };
    },
    awaitExit: Deferred.await(exitDeferred),
    watch: (other) =>
      // Bind to the other actor's exitDeferred — authoritative, not system events.
      // Resolves with exit reason on terminal stop (ignores restarts in Step 3).
      other.awaitExit,
    drain: Effect.gen(function* () {
      const stopped = yield* Ref.get(stoppedRef);
      if (stopped) return;
      const q = yield* Ref.get(eventQueueRef);
      const done = yield* Deferred.make<void>();
      yield* Queue.offer(q, { _tag: "drain" as const, done });
      yield* Deferred.await(done);
    }).pipe(Effect.asVoid),
    sync: {
      send: (event) => {
        const stopped = Effect.runSync(Ref.get(stoppedRef));
        if (!stopped) {
          const q = Effect.runSync(Ref.get(eventQueueRef));
          Effect.runSync(Queue.offer(q, { _tag: "send", event }));
        }
      },
      stop: () => Effect.runFork(stop),
      snapshot: () => Effect.runSync(SubscriptionRef.get(stateRef)),
      matches: (tag) => Effect.runSync(SubscriptionRef.get(stateRef))._tag === tag,
      can: (event) => {
        const state = Effect.runSync(SubscriptionRef.get(stateRef));
        return resolveTransition(machine, state, event) !== undefined;
      },
    },
    system,
    children: childrenMap,
  };
};

// ============================================================================
// Actor Creation — delegates to runtime kernel with actor-specific hooks
// ============================================================================

/** Build ProcessEventHooks from an inspector */
const buildInspectionHooks = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
>(
  actorId: string,
  inspector: InspectorService<S, E | LifecycleEvent>,
): ProcessEventHooks<S, E> => ({
  onSpawnEffect: (state) =>
    emitWithTimestamp(inspector, (timestamp) => ({
      type: "@machine.effect",
      actorId,
      effectType: "spawn",
      state,
      timestamp,
    })),
  onTransition: (from, to, ev) =>
    emitWithTimestamp(inspector, (timestamp) => ({
      type: "@machine.transition",
      actorId,
      fromState: from,
      toState: to,
      event: ev,
      timestamp,
    })),
  onError: (info) =>
    emitWithTimestamp(inspector, (timestamp) => ({
      type: "@machine.error",
      actorId,
      phase: info.phase,
      state: info.state,
      event: info.event,
      error: Cause.pretty(info.cause),
      timestamp,
    })),
});

/**
 * Resolve actor system from context, creating an implicit one if none exists.
 * @internal
 */
const resolveActorSystem = Effect.fn("effect-machine.resolveActorSystem")(function* () {
  const existingSystem = yield* Effect.serviceOption(ActorSystem);
  if (Option.isSome(existingSystem)) {
    return { system: existingSystem.value, implicitSystemScope: undefined };
  }
  const scope = yield* Scope.make();
  const system = yield* make().pipe(Effect.provideService(Scope.Scope, scope));
  return { system, implicitSystemScope: scope };
});

/**
 * Run the supervision loop for a supervised actor.
 * Observes exit deferred, applies restart policy, resets cell resources on restart.
 * @internal
 */
const runSupervisionLoop = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
>(params: {
  supervision: Supervision.Policy;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  machine: Machine<S, E, any, any, any, any>;
  id: string;
  runtimeRef: { current: RuntimeHandle<S, E> | undefined };
  terminalExitDeferred: Deferred.Deferred<ActorExit<S>>;
  pendingReplies: Set<PendingReply>;
  eventQueueRef: Ref.Ref<Queue.Queue<QueuedEvent<E>>>;
  stateRef: SubscriptionRef.SubscriptionRef<S>;
  stoppedRef: Ref.Ref<boolean>;
  childrenMap: Map<string, ActorHandle>;
  listeners: Listeners<S>;
  spawnGeneration: (initialState: S) => Effect.Effect<RuntimeHandle<S, E>>;
  lifecycle?: Lifecycle<S, E>;
  generationRef: { get: () => number; set: (g: number) => void };
  onRestart?: (generation: number, exit: ActorExit<unknown>) => Effect.Effect<void>;
}) =>
  Effect.gen(function* () {
    const step = yield* Schedule.toStepWithSleep(params.supervision.schedule);

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const currentRuntime = params.runtimeRef.current;
      if (currentRuntime === undefined) return;

      const generationExit = yield* Deferred.await(currentRuntime.exitDeferred);

      if (generationExit._tag !== "Defect") {
        yield* Deferred.succeed(params.terminalExitDeferred, generationExit);
        return;
      }

      if (
        params.supervision.shouldRestart !== undefined &&
        !params.supervision.shouldRestart(generationExit)
      ) {
        yield* Deferred.succeed(params.terminalExitDeferred, generationExit);
        return;
      }

      const pull = step(generationExit);
      const scheduleExit = yield* pull.pipe(Effect.exit);
      if (scheduleExit._tag === "Failure") {
        yield* Deferred.succeed(params.terminalExitDeferred, generationExit);
        return;
      }

      // Bump generation before restart — recovery.resolve sees the new generation
      const nextGeneration = params.generationRef.get() + 1;
      params.generationRef.set(nextGeneration);

      // Resolve restart state via recovery or fall back to machine.initial.
      // Recovery runs here (not in runtime.start) for supervision restarts
      // because the cell resources need the resolved state before runtime creation.
      let restartState = params.machine.initial;
      if (params.lifecycle?.recovery !== undefined) {
        const resolved = yield* params.lifecycle.recovery.resolve({
          actorId: params.id,
          generation: nextGeneration,
          machineInitial: params.machine.initial,
        });
        if (Option.isSome(resolved)) {
          restartState = resolved.value;
        }
      }

      yield* settlePendingReplies(params.pendingReplies, params.id);
      const freshQueue = yield* Queue.unbounded<QueuedEvent<E>>();
      yield* Ref.set(params.eventQueueRef, freshQueue);
      yield* SubscriptionRef.set(params.stateRef, restartState);
      yield* Ref.set(params.stoppedRef, false);
      params.childrenMap.clear();

      const newRuntime = yield* params.spawnGeneration(restartState);
      params.runtimeRef.current = newRuntime;
      yield* newRuntime.start;

      if (params.onRestart !== undefined) {
        yield* params.onRestart(nextGeneration, generationExit);
      }

      notifyListeners(params.listeners, restartState);
    }
  });

/**
 * Create and start an actor for a machine.
 * Delegates to the shared runtime kernel with actor-specific lifecycle hooks.
 */
export const createActor = Effect.fn("effect-machine.actor.spawn")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef,
>(
  id: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  machine: Machine<S, E, R, any, any, SD>,
  options?: {
    initialState?: S;
    supervision?: Supervision.Policy;
    lifecycle?: Lifecycle<S, E>;
    /** @internal Called by system after each restart — emits ActorRestarted system event */
    onRestart?: (generation: number, exit: ActorExit<unknown>) => Effect.Effect<void>;
  },
) {
  const lifecycle: Lifecycle<S, E> | undefined = options?.lifecycle;
  const serviceContext = yield* Effect.context<R>();

  // Spawn is cold — initial state from hydrate or machine.initial.
  // Recovery runs during start, not allocate.
  const initial: S = options?.initialState ?? machine.initial;
  yield* Effect.annotateCurrentSpan("effect_machine.actor.id", id);
  yield* Effect.annotateCurrentSpan("effect_machine.actor.initial_state", initial._tag);

  const { system, implicitSystemScope } = yield* resolveActorSystem();

  // Get optional inspector from context
  // SAFETY: Inspector is intentionally type-erased in Context and is used with this actor's S/E pair.
  const inspectorValue = Option.getOrUndefined(yield* Effect.serviceOption(InspectorTag)) as
    | InspectorService<S, E | LifecycleEvent>
    | undefined;

  // Actor-specific state
  const childrenMap = new Map<string, ActorHandle>();
  const pendingReplies = new Set<PendingReply>();
  const listeners: Listeners<S> = new Set();
  const transitionsPubSub = yield* PubSub.unbounded<TransitionInfo<S, E>>();

  // Build hooks from inspector
  const hooks = inspectorValue !== undefined ? buildInspectionHooks(id, inspectorValue) : undefined;

  // Cell-owned resources: stable across generations (supervision)
  const stateRef = yield* SubscriptionRef.make<S>(initial);
  const stoppedRef = yield* Ref.make(false);
  const initialQueue = yield* Queue.unbounded<QueuedEvent<E>>();
  const eventQueueRef = yield* Ref.make(initialQueue);

  // Terminal exit deferred — set exactly once when the actor truly terminates.
  // This is what awaitExit/watch bind to, NOT the per-generation exitDeferred.
  const terminalExitDeferred = yield* Deferred.make<ActorExit<S>>();

  // Track whether @machine.stop has been emitted
  let stopEmitted = false;

  // Generation counter — used by recovery to distinguish cold start from restart
  let generation = 0;

  // Mutable ref for the current runtime — supervision loop updates this
  const runtimeRef: MutableCell<RuntimeHandle<S, E> | undefined> = { current: undefined };

  // Mutable ref for supervisor fiber — set during start, used by stop
  const supervisorFiberRef: MutableCell<Fiber.Fiber<void> | undefined> = {
    current: undefined,
  };

  /** Build lifecycle hooks for a generation */
  const buildRuntimeLifecycle = (): RuntimeLifecycleHooks<S, E> => {
    stopEmitted = false;
    return {
      onEvent:
        inspectorValue !== undefined
          ? (state: S, event: E) =>
              emitWithTimestamp(inspectorValue, (timestamp) => ({
                type: "@machine.event",
                actorId: id,
                state,
                event,
                timestamp,
              }))
          : undefined,
      onStateChange: (result, event) =>
        Effect.gen(function* () {
          notifyListeners(listeners, result.newState);
          // Durability: save after state committed to ref, before reply settlement
          if (lifecycle?.durability !== undefined && result.transitioned) {
            const durability = lifecycle.durability;
            const shouldPersist =
              durability.shouldSave === undefined ||
              durability.shouldSave(result.newState, result.previousState);
            if (shouldPersist) {
              yield* durability.save({
                actorId: id,
                generation,
                previousState: result.previousState,
                nextState: result.newState,
                event,
              });
            }
          }
          yield* Effect.annotateCurrentSpan("effect_machine.transition.matched", true);
          if (result.lifecycleRan) {
            yield* Effect.annotateCurrentSpan(
              "effect_machine.state.from",
              result.previousState._tag,
            );
            yield* Effect.annotateCurrentSpan("effect_machine.state.to", result.newState._tag);
          }
        }),
      onProcessed: (result, event) =>
        result.transitioned
          ? PubSub.publish(transitionsPubSub, {
              fromState: result.previousState,
              toState: result.newState,
              event,
            }).pipe(Effect.asVoid)
          : Effect.void,
      onFinal:
        inspectorValue !== undefined
          ? (state: S) =>
              Effect.gen(function* () {
                stopEmitted = true;
                yield* emitWithTimestamp(inspectorValue, (timestamp) => ({
                  type: "@machine.stop",
                  actorId: id,
                  finalState: state,
                  timestamp,
                }));
              })
          : undefined,
      onShutdown: () =>
        Effect.gen(function* () {
          if (!stopEmitted) {
            const finalState = yield* SubscriptionRef.get(stateRef);
            yield* emitWithTimestamp(inspectorValue, (timestamp) => ({
              type: "@machine.stop",
              actorId: id,
              finalState,
              timestamp,
            }));
          }
          yield* settlePendingReplies(pendingReplies, id);
        }),
      onInitialSpawnEffects:
        inspectorValue !== undefined
          ? (state: S) =>
              emitWithTimestamp(inspectorValue, (timestamp) => ({
                type: "@machine.effect",
                actorId: id,
                effectType: "spawn",
                state,
                timestamp,
              }))
          : undefined,
    };
  };

  /** Create a single runtime generation with an explicit hydrated/recovered state. */
  const spawnGeneration = (generationInitial: S) =>
    Ref.get(eventQueueRef).pipe(
      Effect.flatMap(
        (currentQueue) =>
          // SAFETY: createRuntime is instantiated from this actor's exact machine, state, and event types.
          createRuntime(machine, system, {
            actorId: id,
            initialState: generationInitial,
            hooks,
            skipFinalizer: true,
            cellResources: { stateRef, stoppedRef, eventQueue: currentQueue },
            lifecycle: buildRuntimeLifecycle(),
            wrapProcess: (state, event, inner) =>
              Effect.withSpan("effect-machine.event.process", {
                attributes: {
                  "effect_machine.actor.id": id,
                  "effect_machine.state.current": state._tag,
                  "effect_machine.event.type": event._tag,
                },
              })(
                inner.pipe(
                  Effect.tap((r) =>
                    Effect.annotateCurrentSpan(
                      "effect_machine.transition.matched",
                      r.result.transitioned,
                    ),
                  ),
                ),
              ),
            onChildSpawned: <ChildState extends { readonly _tag: string }, ChildEvent>(
              childId: string,
              child: ActorRef<ChildState, ChildEvent>,
            ) =>
              Effect.gen(function* () {
                childrenMap.set(childId, child);
                // Use Scope.Scope here intentionally — this is the spawn handler's
                // state-scoped scope, not an ambient scope. When the state exits,
                // this scope closes and the child is removed from the map.
                const maybeScope = yield* Effect.serviceOption(Scope.Scope);
                if (Option.isSome(maybeScope)) {
                  yield* Scope.addFinalizer(
                    maybeScope.value,
                    Effect.sync(() => {
                      childrenMap.delete(childId);
                    }),
                  );
                }
              }),
          }) as Effect.Effect<RuntimeHandle<S, E>>,
      ),
    );

  // Spawn initial generation (with hydrated state if provided)
  const runtime = yield* spawnGeneration(initial);
  runtimeRef.current = runtime;

  const supervision = options?.supervision;

  // Build actor stop — wraps current runtime.stop with implicit system teardown.
  // For supervised actors: interrupt supervisor fiber first (cancels restart/backoff),
  // then stop the current runtime, then set terminal exit.
  const stop = Effect.gen(function* () {
    // Interrupt supervisor loop first — prevents restart during/after stop
    if (supervisorFiberRef.current !== undefined) {
      yield* Fiber.interrupt(supervisorFiberRef.current);
    }
    const currentRuntime = runtimeRef.current;
    if (currentRuntime !== undefined) {
      yield* currentRuntime.stop;
    }
    // Set terminal exit (Deferred.succeed is idempotent — no-op if already set)
    yield* Deferred.succeed(terminalExitDeferred, ActorExit.Stopped);
    if (implicitSystemScope !== undefined) {
      yield* Scope.close(implicitSystemScope, Exit.void);
    }
  }).pipe(
    Effect.provide(serviceContext),
    Effect.withSpan("effect-machine.actor.stop"),
    Effect.asVoid,
  );

  // Track whether hydrate was provided — skip recovery when hydrated
  const isHydrated = options?.initialState !== undefined;

  // Build actor start — runs recovery, emits @machine.spawn, arms supervisor, then delegates to runtime.start
  const start = Effect.gen(function* () {
    // Run recovery if lifecycle.recovery exists AND not hydrated (hydrate takes precedence)
    if (lifecycle?.recovery !== undefined && !isHydrated) {
      const resolved = yield* lifecycle.recovery.resolve({
        actorId: id,
        generation,
        machineInitial: machine.initial,
      });
      if (Option.isSome(resolved)) {
        // Update cell stateRef
        yield* SubscriptionRef.set(stateRef, resolved.value);
        // Runtime was created with cold initial — recreate with recovered state.
        const newRuntime = yield* spawnGeneration(resolved.value);
        runtimeRef.current = newRuntime;
      }
    }

    // Emit @machine.spawn inspection event (moved from allocate → start)
    const currentState = yield* SubscriptionRef.get(stateRef);
    yield* emitWithTimestamp(inspectorValue, (timestamp) => ({
      type: "@machine.spawn",
      actorId: id,
      initialState: currentState,
      timestamp,
    }));

    // Arm supervisor (moved from allocate → start)
    if (supervision !== undefined) {
      supervisorFiberRef.current = yield* Effect.forkDetach(
        runSupervisionLoop({
          supervision,
          machine,
          id,
          runtimeRef,
          terminalExitDeferred,
          pendingReplies,
          eventQueueRef,
          stateRef,
          stoppedRef,
          childrenMap,
          listeners,
          spawnGeneration,
          lifecycle,
          generationRef: {
            get: () => generation,
            set: (g: number) => {
              generation = g;
            },
          },
          onRestart: options?.onRestart,
        }),
      );
    } else {
      // No supervision — wire terminal exit from the current generation
      const currentRuntime = runtimeRef.current;
      if (currentRuntime !== undefined) {
        yield* Effect.forkDetach(
          Deferred.await(currentRuntime.exitDeferred).pipe(
            Effect.tap((exit) => Deferred.succeed(terminalExitDeferred, exit)),
          ),
        );
      }
    }

    // Delegate to runtime.start (forks event loop, background, spawn effects)
    const currentRuntime = runtimeRef.current;
    if (currentRuntime !== undefined) {
      yield* currentRuntime.start;
    }
  }).pipe(
    Effect.provide(serviceContext),
    Effect.withSpan("effect-machine.actor.start"),
    Effect.asVoid,
  );

  return buildActorRefCore(
    id,
    machine,
    stateRef,
    eventQueueRef,
    stoppedRef,
    listeners,
    stop,
    start,
    system,
    childrenMap,
    pendingReplies,
    transitionsPubSub,
    terminalExitDeferred,
  );
});

/** Fail all pending call/ask Deferreds with ActorStoppedError. Safe to call multiple times. */
export const settlePendingReplies = (pendingReplies: Set<PendingReply>, actorId: string) =>
  Effect.sync(() => {
    const error = new ActorStoppedError({ actorId });
    for (const pending of pendingReplies) {
      // Deferred.fail returns false if already completed — safe to double-settle
      Effect.runFork(pending.failStopped(error));
    }
    pendingReplies.clear();
  });

// ============================================================================
// ActorSystem Implementation
// ============================================================================

/** Notify all system event listeners (sync). */
const notifySystemListeners = (listeners: Set<SystemEventListener>, event: SystemEvent): void => {
  for (const listener of listeners) {
    try {
      listener(event);
    } catch {
      // Ignore listener failures to avoid crashing the system
    }
  }
};

const make = Effect.fn("effect-machine.actorSystem.make")(function* () {
  // MutableHashMap for O(1) spawn/stop/get operations
  const actorsMap = MutableHashMap.empty<string, ActorHandle>();
  const spawnGate = yield* Semaphore.make(1);
  const withSpawnGate = spawnGate.withPermits(1);

  // Observable infrastructure
  const eventPubSub = yield* PubSub.unbounded<SystemEvent>();
  const eventListeners = new Set<SystemEventListener>();

  const emitSystemEvent = (event: SystemEvent): Effect.Effect<void> =>
    Effect.sync(() => notifySystemListeners(eventListeners, event)).pipe(
      Effect.andThen(PubSub.publish(eventPubSub, event)),
      Effect.catchCause(() => Effect.void),
      Effect.asVoid,
    );

  // Stop all actors on system teardown (no events — PubSub is about to die)
  yield* Effect.addFinalizer(() => {
    const stops: Effect.Effect<void>[] = [];
    MutableHashMap.forEach(actorsMap, (actor) => {
      stops.push(actor.stop);
    });
    return Effect.all(stops, { concurrency: "unbounded" }).pipe(
      Effect.andThen(PubSub.shutdown(eventPubSub)),
      Effect.asVoid,
    );
  });

  /** Check for duplicate ID, register actor, attach scope cleanup if available */
  const registerActor = Effect.fn("effect-machine.actorSystem.register")(function* <
    T extends ActorHandle,
  >(id: string, actor: T) {
    // Check if actor already exists
    if (MutableHashMap.has(actorsMap, id)) {
      // Stop the newly created actor to avoid leaks
      yield* actor.stop;
      return yield* new DuplicateActorError({ actorId: id });
    }

    // Register it - O(1)
    MutableHashMap.set(actorsMap, id, actor);

    // Emit spawned event
    yield* emitSystemEvent({ _tag: "ActorSpawned", id, actor });

    // If ActorScope available, attach per-actor cleanup
    const maybeScope = yield* Effect.serviceOption(ActorScope);
    if (Option.isSome(maybeScope)) {
      yield* Scope.addFinalizer(
        maybeScope.value,
        Effect.gen(function* () {
          // Guard: only emit if still registered (system.stop may have already removed it)
          if (MutableHashMap.has(actorsMap, id)) {
            // Scope cleanup — use Stopped as the exit reason.
            // The authoritative exit is on actor.awaitExit, not here.
            yield* emitSystemEvent({
              _tag: "ActorStopped",
              id,
              actor,
              exit: ActorExit.Stopped,
            });
            MutableHashMap.remove(actorsMap, id);
          }
          yield* actor.stop;
        }),
      );
    }

    return actor;
  });

  const spawnRegular = Effect.fn("effect-machine.actorSystem.spawnRegular")(function* <
    S extends { readonly _tag: string },
    E extends { readonly _tag: string },
    R,
  >(
    id: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    machine: Machine<S, E, R, any, any, any>,
    spawnOptions?: {
      readonly supervision?: Supervision.Policy;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      readonly slots?: LegacySlotHandlers;
      readonly lifecycle?: Lifecycle<S, E>;
    },
  ) {
    if (MutableHashMap.has(actorsMap, id)) {
      return yield* new DuplicateActorError({ actorId: id });
    }
    // Materialize slots if provided
    const materialized =
      spawnOptions?.slots !== undefined ? materializeMachine(machine, spawnOptions.slots) : machine;
    // Mutable ref for the actor �� onRestart closure needs it, but actor isn't registered yet
    let actorRef: ActorHandle | undefined;
    const actor = yield* createActor(id, materialized, {
      supervision: spawnOptions?.supervision,
      lifecycle: spawnOptions?.lifecycle,
      onRestart:
        spawnOptions?.supervision !== undefined
          ? (generation, exit) =>
              actorRef !== undefined
                ? emitSystemEvent({
                    _tag: "ActorRestarted",
                    id,
                    actor: actorRef,
                    generation,
                    exit,
                  })
                : Effect.void
          : undefined,
    });
    actorRef = actor;
    // Register before start — actor is in the map before lifecycle hooks fire
    yield* registerActor(id, actor);
    // Auto-start: system.spawn returns a running actor
    yield* actor.start.pipe(
      Effect.catchCause((cause) => actor.stop.pipe(Effect.andThen(Effect.failCause(cause)))),
    );
    return actor;
  });

  const spawn = <
    S extends { readonly _tag: string },
    E extends { readonly _tag: string },
    R,
    SD extends SlotsDef = Record<string, never>,
  >(
    id: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    machine: Machine<S, E, R, any, any, SD>,
    options?: {
      readonly supervision?: Supervision.Policy;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      readonly slots?: ProvideSlots<SD, any>;
      readonly lifecycle?: Lifecycle<S, E>;
    },
  ): Effect.Effect<ActorRef<S, E>, DuplicateActorError, R> =>
    withSpawnGate(spawnRegular(id, machine, options));

  const get = Effect.fn("effect-machine.actorSystem.get")(function* (id: string) {
    return yield* Effect.sync(() => MutableHashMap.get(actorsMap, id));
  });

  const stop = Effect.fn("effect-machine.actorSystem.stop")(function* (id: string) {
    const maybeActor = MutableHashMap.get(actorsMap, id);
    if (Option.isNone(maybeActor)) {
      return false;
    }

    const actor = maybeActor.value;
    // Remove first to prevent scope finalizer double-emit
    MutableHashMap.remove(actorsMap, id);
    yield* emitSystemEvent({
      _tag: "ActorStopped",
      id,
      actor,
      exit: ActorExit.Stopped,
    });
    yield* actor.stop;
    return true;
  });

  return ActorSystem.of({
    spawn,
    get,
    stop,
    events: Stream.fromPubSub(eventPubSub),
    get actors() {
      const snapshot = new Map<string, ActorHandle>();
      MutableHashMap.forEach(actorsMap, (actor, id) => {
        snapshot.set(id, actor);
      });
      return snapshot;
    },
    subscribe: (fn) => {
      eventListeners.add(fn);
      return () => {
        eventListeners.delete(fn);
      };
    },
  });
});

/**
 * Create an ActorSystem instance. Must be run in a Scope.
 * @internal — use Default layer for normal usage
 */
export const makeSystem = make;

/**
 * Default ActorSystem layer
 */
export const Default = Layer.effect(ActorSystem, make());
