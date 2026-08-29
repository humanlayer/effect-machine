// @effect-diagnostics anyUnknownInErrorContext:off
/**
 * Shared runtime kernel for machine event processing.
 *
 * Provides a single-queue event loop with:
 * - Sequential event processing (no split-mailbox race)
 * - Postpone buffer with drain-on-state-change (gen_statem)
 * - Background effect lifecycle (under actorScope fault boundary)
 * - Spawn effect lifecycle (per-state scope)
 * - Final state detection → stop
 * - Reply settlement (call/ask Deferreds)
 * - Reply schema validation
 * - Lifecycle hooks for actor-specific concerns (inspection, listeners, etc.)
 * - ActorExit with exit reason (Final/Stopped/Defect) via exitDeferred
 *
 * Used by entity-machine and local actor (actor.ts delegates here).
 *
 * @internal
 */
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Queue,
  Ref,
  Schema,
  Scope,
  SubscriptionRef,
} from "effect";

import type { Machine, MachineRef } from "../machine.js";
import type { ActorRef, ActorSystemService } from "../actor.js";
import { ActorSystem as ActorSystemTag } from "../actor.js";
import type { ProcessEventHooks, ProcessEventResult } from "./transition.js";
import type { SlotsDef, MachineContext } from "../slot.js";
import { processEventCore, runSpawnEffects, shouldPostpone } from "./transition.js";
import { NoReplyError } from "../errors.js";
import { INTERNAL_INIT_EVENT } from "./utils.js";
import { ActorExit, type DefectPhase } from "../supervision.js";

// ============================================================================
// QueuedEvent — unified type for all event loop consumers
// ============================================================================

/** @internal */
export type RuntimeQueuedEvent<E> =
  | { readonly _tag: "send"; readonly event: E }
  | {
      readonly _tag: "sendWait";
      readonly event: E;
      readonly done: Deferred.Deferred<void, unknown>;
    }
  | {
      readonly _tag: "call";
      readonly event: E;
      readonly reply: Deferred.Deferred<ProcessEventResult<{ readonly _tag: string }>, unknown>;
    }
  | {
      readonly _tag: "ask";
      readonly event: E;
      readonly reply: Deferred.Deferred<unknown, NoReplyError>;
    }
  | {
      readonly _tag: "drain";
      readonly done: Deferred.Deferred<void>;
    };

// ============================================================================
// Cell resources — stable across runtime generations
// ============================================================================

/**
 * Resources owned by the actor cell (stable across generations).
 * When provided, createRuntime uses these instead of allocating its own.
 * @internal
 */
export interface RuntimeCellResources<S, E> {
  readonly stateRef: SubscriptionRef.SubscriptionRef<S>;
  readonly eventQueue: Queue.Queue<RuntimeQueuedEvent<E>>;
  readonly stoppedRef: Ref.Ref<boolean>;
}

// ============================================================================
// Runtime interface
// ============================================================================

/** @internal */
export interface RuntimeHandle<S, E> {
  /** Enqueue a fire-and-forget event */
  readonly send: (event: E) => Effect.Effect<void>;
  /** Enqueue event and wait for processing to complete (for RPC Send). Fails on defect. */
  readonly sendWait: (event: E) => Effect.Effect<void, unknown>;
  /** Enqueue an ask event, returns the reply value */
  readonly ask: (event: E) => Effect.Effect<unknown, NoReplyError>;
  /** Get current state */
  readonly getState: Effect.Effect<S>;
  /** SubscriptionRef for state observation (WatchState streaming) */
  readonly stateRef: SubscriptionRef.SubscriptionRef<S>;
  /** Whether the runtime has stopped (final state reached) */
  readonly isStopped: Effect.Effect<boolean>;
  /** Stop the runtime (interrupt event loop, clean up) */
  readonly stop: Effect.Effect<void>;
  /**
   * Start the runtime — fork event loop, background effects, spawn effects.
   * Idempotent: first caller runs initialization, subsequent callers await completion.
   * Events sent before start() are queued and processed when start() runs.
   */
  readonly start: Effect.Effect<void>;
  /** @internal — raw event queue for direct enqueue (actor.ts uses this for pendingReplies tracking) */
  readonly _queue: Queue.Queue<RuntimeQueuedEvent<E>>;
  /** @internal — stopped ref for direct access */
  readonly _stoppedRef: Ref.Ref<boolean>;
  /**
   * Exit deferred — set exactly once with the exit reason when the runtime stops.
   * Final state → ActorExit.Final, explicit stop → ActorExit.Stopped, defect → ActorExit.Defect.
   */
  readonly exitDeferred: Deferred.Deferred<ActorExit<S>>;
  /**
   * Actor scope — owns background fibers for this generation.
   * Closing this scope interrupts all background fibers.
   */
  readonly actorScope: Scope.Closeable;
}

// ============================================================================
// Lifecycle hooks — actor-specific concerns injected into the kernel
// ============================================================================

/** @internal */
export interface RuntimeLifecycleHooks<S, E> {
  /** Before processEventCore — actor emits @machine.event inspection */
  readonly onEvent?: (state: S, event: E) => Effect.Effect<void>;
  /** After SubscriptionRef.set on transition — actor notifies listeners, annotates spans */
  readonly onStateChange?: (result: ProcessEventResult<S>, event: E) => Effect.Effect<void>;
  /** After reply settlement when transition occurred — actor publishes to transitionsPubSub */
  readonly onProcessed?: (result: ProcessEventResult<S>, event: E) => Effect.Effect<void>;
  /** When final state detected in event loop — actor emits @machine.stop */
  readonly onFinal?: (state: S) => Effect.Effect<void>;
  /** Before stop resource cleanup — actor emits @machine.stop, settles pending replies */
  readonly onShutdown?: () => Effect.Effect<void>;
  /** Before initial spawn effects — actor emits @machine.effect inspection */
  readonly onInitialSpawnEffects?: (state: S) => Effect.Effect<void>;
}

// ============================================================================
// Runtime creation
// ============================================================================

/** @internal */
export interface RuntimeConfig<S, E> {
  readonly actorId: string;
  /** Runtime initial state, used for hydration/recovery without cloning the machine. */
  readonly initialState?: S;
  readonly hooks?: ProcessEventHooks<S, E>;
  /**
   * Cell-owned resources. When provided, the runtime uses the cell's stateRef,
   * eventQueue, and stoppedRef instead of creating its own.
   * Used by actor.ts for supervision (cell owns stable resources across generations).
   */
  readonly cellResources?: RuntimeCellResources<S, E>;
  /**
   * Custom queue factory. Default: `Queue.unbounded()`.
   * Use `Queue.sliding(n)` or `Queue.dropping(n)` for bounded queues.
   * Ignored when cellResources is provided.
   */
  readonly queueFactory?: Effect.Effect<Queue.Queue<RuntimeQueuedEvent<E>>>;
  /** Lifecycle callbacks for actor-specific concerns */
  readonly lifecycle?: RuntimeLifecycleHooks<S, E>;
  /** Wrap each processQueued invocation — actor uses for span annotations */
  readonly wrapProcess?: (
    state: S,
    event: E,
    inner: Effect.Effect<ProcessQueuedResult<S>>,
  ) => Effect.Effect<ProcessQueuedResult<S>>;
  /** Called after self.spawn succeeds — actor tracks children */
  readonly onChildSpawned?: <ChildState extends { readonly _tag: string }, ChildEvent>(
    childId: string,
    child: ActorRef<ChildState, ChildEvent>,
  ) => Effect.Effect<void>;
  /** Skip registering stop as scope finalizer — actor manages its own lifecycle */
  readonly skipFinalizer?: boolean;
  /** Prefix for child actor IDs in self.spawn. Entity-machine uses `${actorId}/`. Default: no prefix. */
  readonly childIdPrefix?: string;
}

/** @internal */
export interface ProcessQueuedResult<S> {
  readonly shouldStop: boolean;
  readonly stateChanged: boolean;
  readonly result: ProcessEventResult<S>;
}

interface MutableCell<T> {
  current: T;
}

/**
 * Create a runtime for a machine. Returns a handle for sending events
 * and querying state. The runtime owns:
 * - Event loop fiber
 * - Postpone buffer
 * - Background effects (under actorScope)
 * - State scope (spawn effects)
 * - Final state detection
 * - Exit reason via exitDeferred
 *
 * Resources (stateRef, eventQueue, stoppedRef) are either cell-provided
 * or allocated fresh by the runtime.
 *
 * @internal
 */
export const createRuntime = Effect.fn("effect-machine.runtime.create")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- wide acceptance for Machine type params
  machine: Machine<S, E, R, any, any, SD>,
  system: ActorSystemService,
  config: RuntimeConfig<S, E>,
) {
  const { actorId, hooks, lifecycle } = config;
  const initialState = config.initialState ?? machine.initial;

  // Capture services at allocation so start, stop, and deferred settlement retain them.
  const services = yield* Effect.context<R>();
  const fork = Effect.runForkWith(services);

  // Resources: use cell-provided or allocate fresh
  const stateRef = config.cellResources?.stateRef ?? (yield* SubscriptionRef.make<S>(initialState));
  const stoppedRef = config.cellResources?.stoppedRef ?? (yield* Ref.make(false));
  const eventQueue =
    config.cellResources?.eventQueue ??
    (yield* config.queueFactory ?? Queue.unbounded<RuntimeQueuedEvent<E>>());

  // Exit deferred — set exactly once with the exit reason
  const exitDeferred = yield* Deferred.make<ActorExit<S>>();

  // Actor scope — owns background fibers for this generation
  const actorScope = yield* Scope.make();

  // Pending deferred reply — stored when handler returns Machine.deferReply()
  // Settled by self.reply() from spawn handler
  const deferredReplyRef: MutableCell<Deferred.Deferred<unknown, NoReplyError> | undefined> = {
    current: undefined,
  };

  // Self reference — sends go through the same queue
  const selfSend = Effect.fn("effect-machine.runtime.self.send")(function* (event: E) {
    const stopped = yield* Ref.get(stoppedRef);
    if (!stopped) {
      yield* Queue.offer(eventQueue, { _tag: "send", event });
    }
  });
  const childPrefix = config.childIdPrefix ?? "";
  const defaultSpawn: MachineRef<E>["spawn"] = (childId, childMachine) =>
    system
      .spawn(`${childPrefix}${childId}`, childMachine)
      .pipe(Effect.provideService(ActorSystemTag, system));
  const onChildSpawned = config.onChildSpawned;
  const self: MachineRef<E> = {
    send: selfSend,
    cast: selfSend,
    spawn:
      onChildSpawned !== undefined
        ? (childId, childMachine) =>
            defaultSpawn(childId, childMachine).pipe(
              Effect.tap((child) => onChildSpawned(childId, child)),
            )
        : defaultSpawn,
    reply: <Reply>(value: Reply) =>
      Effect.sync(() => {
        const deferred = deferredReplyRef.current;
        if (deferred !== undefined) {
          deferredReplyRef.current = undefined;
          fork(Deferred.succeed(deferred, value));
          return true;
        }
        return false;
      }),
  };

  // State scope for spawn effects
  const stateScopeRef: MutableCell<Scope.Closeable> = {
    current: yield* Scope.make(),
  };

  // Shared mutable refs used by both start() and stop()
  const initEvent = { _tag: INTERNAL_INIT_EVENT } as const;
  const ctx: MachineContext<S, E | typeof initEvent, MachineRef<E>> = {
    actorId,
    state: initialState,
    event: initEvent,
    self,
    system,
  };
  const slots = machine._slots;

  // Mutable holder for the loop fiber — needed by stop() and spawn defect signals
  const loopFiberRef: MutableCell<Fiber.Fiber<void> | undefined> = { current: undefined };

  /** Set the exit deferred exactly once. */
  const setExit = (exit: ActorExit<S>) => Deferred.succeed(exitDeferred, exit).pipe(Effect.asVoid);

  // Idempotent start gate — first caller runs initialization, subsequent callers await
  const startDeferred = yield* Deferred.make<void, unknown>();
  const startedRef = yield* Ref.make(false);

  const start = Effect.gen(function* () {
    // Idempotent: if already started, just await completion
    const alreadyStarted = yield* Ref.getAndSet(startedRef, true);
    if (alreadyStarted) {
      yield* Deferred.await(startDeferred);
      return;
    }

    // Fork background effects under actorScope
    const backgroundFibers: Fiber.Fiber<void>[] = [];

    for (const bg of machine.backgroundEffects) {
      const fiber = yield* bg
        .handler({
          actorId,
          state: initialState,
          event: initEvent,
          self,
          slots,
          system,
        })
        .pipe(Effect.provideService(machine.Context, ctx), Effect.forkIn(actorScope));
      backgroundFibers.push(fiber);
    }

    // Run initial spawn effects — catch defects, tag as initial-spawn, and propagate.
    // For unsupervised actors this fails createActor (correct: don't register dead actors).
    // For supervised actors (Step 3), the supervision loop will catch and restart.
    if (lifecycle?.onInitialSpawnEffects !== undefined) {
      yield* lifecycle.onInitialSpawnEffects(initialState);
    }
    // Note: onSpawnDefect for initial spawn fibers that defect asynchronously (after forking).
    // If they defect later, this signals through exitDeferred and interrupts the loop.
    const initialSpawnDefectSignal = (cause: Cause.Cause<unknown>) =>
      Deferred.succeed(exitDeferred, ActorExit.Defect(cause, "initial-spawn")).pipe(
        Effect.andThen(Ref.set(stoppedRef, true)),
        Effect.andThen(
          Effect.suspend(() =>
            loopFiberRef.current !== undefined
              ? Fiber.interrupt(loopFiberRef.current)
              : Effect.void,
          ),
        ),
        Effect.asVoid,
      );
    yield* runSpawnEffects(
      machine,
      initialState,
      initEvent,
      self,
      stateScopeRef.current,
      system,
      actorId,
      hooks?.onError,
      initialSpawnDefectSignal,
    ).pipe(
      Effect.catchCause((cause) =>
        // Tag as initial-spawn defect, set exit, clean up, then propagate
        Effect.gen(function* () {
          yield* Ref.set(stoppedRef, true);
          yield* Scope.close(stateScopeRef.current, Exit.void);
          yield* Scope.close(actorScope, Exit.void);
          yield* Deferred.succeed(exitDeferred, ActorExit.Defect(cause, "initial-spawn"));
          return yield* Effect.failCause(cause);
        }),
      ),
    );

    // Check if initial state is final — if so, clean up and signal done
    if (machine.finalStates.has(initialState._tag)) {
      if (lifecycle?.onFinal !== undefined) yield* lifecycle.onFinal(initialState);
      yield* Ref.set(stoppedRef, true);
      yield* Scope.close(stateScopeRef.current, Exit.void);
      yield* Scope.close(actorScope, Exit.void);
      yield* setExit(ActorExit.Final(initialState));
      yield* Deferred.succeed(startDeferred, undefined);
      return;
    }

    // Augment hooks with spawn defect signal — spawn fibers signal through this
    // instead of dying silently, so the runtime can set exitDeferred and terminate.
    const augmentedHooks: ProcessEventHooks<S, E> = {
      ...hooks,
      onSpawnDefect: (cause: Cause.Cause<unknown>) =>
        Deferred.succeed(exitDeferred, ActorExit.Defect(cause, "spawn")).pipe(
          Effect.andThen(Ref.set(stoppedRef, true)),
          Effect.andThen(
            Effect.suspend(() =>
              loopFiberRef.current !== undefined
                ? Fiber.interrupt(loopFiberRef.current)
                : Effect.void,
            ),
          ),
          Effect.asVoid,
        ),
    };

    // Start event loop — forked OUTSIDE actorScope (not a background fiber).
    // The generation owner fiber below observes its exit and closes actorScope.
    const loopFiber = yield* Effect.forkDetach(
      runtimeEventLoop(
        machine,
        stateRef,
        eventQueue,
        stoppedRef,
        self,
        stateScopeRef,
        actorId,
        system,
        exitDeferred,
        augmentedHooks,
        deferredReplyRef,
        lifecycle,
        config.wrapProcess,
        fork,
      ),
    );
    loopFiberRef.current = loopFiber;

    // Background defect observer: Fiber.await each background fiber.
    // forkIn defects are silent (not propagated to scope), so we must explicitly watch them.
    // On defect: set exitDeferred with phase "background", then interrupt the event loop.
    // Interrupt-only exits are normal lifecycle (scope close on stop/final) — not defects.
    // Forked INTO actorScope — gets interrupted when actorScope closes (no leak).
    if (backgroundFibers.length > 0) {
      yield* Effect.raceAll(
        backgroundFibers.map((fiber) =>
          Fiber.await(fiber).pipe(
            Effect.flatMap((exit) => {
              if (exit._tag === "Failure" && !Cause.hasInterruptsOnly(exit.cause)) {
                return setExit(ActorExit.Defect(exit.cause, "background")).pipe(
                  Effect.andThen(Ref.set(stoppedRef, true)),
                  Effect.andThen(Fiber.interrupt(loopFiber)),
                );
              }
              // Normal exit or clean interrupt — ignore, wait forever (scope close will interrupt)
              return Effect.never;
            }),
          ),
        ),
      ).pipe(Effect.forkIn(actorScope));
    }

    // Generation owner: observes loop exit, then closes actorScope to clean up
    // background fibers. The loop sets exitDeferred before exiting.
    yield* Effect.forkDetach(
      Effect.gen(function* () {
        const loopExit = yield* Fiber.await(loopFiber);
        // Close actorScope — interrupts background fibers and their defect watchers
        if (loopExit._tag === "Success") {
          yield* Scope.close(actorScope, Exit.void);
        } else {
          yield* Scope.close(actorScope, loopExit);
        }
      }),
    );

    yield* Deferred.succeed(startDeferred, undefined);
  }).pipe(
    Effect.catchCause((cause) =>
      Deferred.failCause(startDeferred, cause).pipe(Effect.andThen(Effect.failCause(cause))),
    ),
  );

  const stop = Effect.gen(function* () {
    const alreadyStopped = yield* Ref.get(stoppedRef);
    if (alreadyStopped) return;
    if (lifecycle?.onShutdown !== undefined) yield* lifecycle.onShutdown();
    yield* Ref.set(stoppedRef, true);
    const loopFiber = loopFiberRef.current;
    if (loopFiber !== undefined) {
      yield* Fiber.interrupt(loopFiber);
    }
    yield* Scope.close(stateScopeRef.current, Exit.void);
    yield* Scope.close(actorScope, Exit.void);
    yield* setExit(ActorExit.Stopped);
  }).pipe(Effect.asVoid);

  // Register stop as scope finalizer so entity teardown cleans up fibers.
  // Skipped for actor.ts which manages its own stop lifecycle.
  if (config.skipFinalizer !== true) {
    yield* Effect.addFinalizer(() => stop);
  }

  return {
    ...makeHandle(stateRef, stoppedRef, eventQueue, exitDeferred, actorScope),
    stop: stop.pipe(Effect.provide(services)),
    start: start.pipe(Effect.provide(services)),
  };
});

/**
 * Build the runtime handle (send/ask/getState/isStopped).
 * Shared between initial-final and normal paths.
 */
const makeHandle = <S extends { readonly _tag: string }, E extends { readonly _tag: string }>(
  stateRef: SubscriptionRef.SubscriptionRef<S>,
  stoppedRef: Ref.Ref<boolean>,
  eventQueue: Queue.Queue<RuntimeQueuedEvent<E>>,
  exitDeferred: Deferred.Deferred<ActorExit<S>>,
  actorScope: Scope.Closeable,
): RuntimeHandle<S, E> => ({
  send: (event: E) =>
    Effect.gen(function* () {
      const stopped = yield* Ref.get(stoppedRef);
      if (!stopped) {
        yield* Queue.offer(eventQueue, { _tag: "send", event });
      }
    }),
  sendWait: (event: E) =>
    Effect.gen(function* () {
      const stopped = yield* Ref.get(stoppedRef);
      if (!stopped) {
        const done = yield* Deferred.make<void, unknown>();
        yield* Queue.offer(eventQueue, { _tag: "sendWait", event, done });
        yield* Deferred.await(done);
      }
    }),
  ask: (event: E) =>
    Effect.gen(function* () {
      const stopped = yield* Ref.get(stoppedRef);
      if (stopped) {
        return yield* new NoReplyError({ actorId: "stopped", eventTag: event._tag });
      }
      const reply = yield* Deferred.make<unknown, NoReplyError>();
      yield* Queue.offer(eventQueue, { _tag: "ask", event, reply });
      return yield* Deferred.await(reply);
    }),
  getState: SubscriptionRef.get(stateRef),
  stateRef,
  isStopped: Ref.get(stoppedRef),
  stop: Effect.void,
  start: Effect.void,
  _queue: eventQueue,
  _stoppedRef: stoppedRef,
  exitDeferred,
  actorScope,
});

// ============================================================================
// Event loop
// ============================================================================

const runtimeEventLoop = Effect.fn("effect-machine.runtime.eventLoop")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- wide acceptance
  machine: Machine<S, E, R, any, any, SD>,
  stateRef: SubscriptionRef.SubscriptionRef<S>,
  eventQueue: Queue.Queue<RuntimeQueuedEvent<E>>,
  stoppedRef: Ref.Ref<boolean>,
  self: MachineRef<E>,
  stateScopeRef: { current: Scope.Closeable },
  actorId: string,
  system: ActorSystemService,
  exitDeferred: Deferred.Deferred<ActorExit<S>>,
  hooks?: ProcessEventHooks<S, E>,
  deferredReplyRef?: { current: Deferred.Deferred<unknown, NoReplyError> | undefined },
  lifecycle?: RuntimeLifecycleHooks<S, E>,
  wrapProcess?: (
    state: S,
    event: E,
    inner: Effect.Effect<ProcessQueuedResult<S>>,
  ) => Effect.Effect<ProcessQueuedResult<S>>,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  fork?: (effect: Effect.Effect<any>) => Fiber.Fiber<any>,
) {
  // Fire-and-forget fork with captured services
  const forkEffect = fork ?? Effect.runFork;

  // Event-bearing queue variants (excludes drain sentinel)
  type EventQueued = Exclude<RuntimeQueuedEvent<E>, { readonly _tag: "drain" }>;

  /** Set the exit deferred exactly once. */
  const setExit = (exit: ActorExit<S>) => Deferred.succeed(exitDeferred, exit).pipe(Effect.asVoid);

  // Postpone buffer — only event-bearing variants, never drain
  const postponed: EventQueued[] = [];
  const hasPostponeRules = machine.postponeRules.length > 0;

  const postponeQueued = Effect.fn("effect-machine.runtime.postponeQueued")(function* (
    queued: EventQueued,
    currentState: S,
  ) {
    if (queued._tag === "call") {
      const postponedResult: ProcessEventResult<{ readonly _tag: string }> = {
        newState: currentState,
        previousState: currentState,
        transitioned: false,
        lifecycleRan: false,
        isFinal: false,
        hasReply: false,
        deferReply: false,
        reply: undefined,
        postponed: true,
      };
      yield* Deferred.succeed(queued.reply, postponedResult);
    }
    if (queued._tag === "sendWait") {
      yield* Deferred.succeed(queued.done, undefined);
    }
    postponed.push({ _tag: "send", event: queued.event });
    const result: ProcessEventResult<S> = {
      newState: currentState,
      previousState: currentState,
      transitioned: false,
      lifecycleRan: false,
      isFinal: false,
      hasReply: false,
      deferReply: false,
      reply: undefined,
      postponed: true,
    };
    return { shouldStop: false, stateChanged: false, result };
  });

  const settleQueued = Effect.fn("effect-machine.runtime.settleQueued")(function* (
    queued: EventQueued,
    event: E,
    result: ProcessEventResult<S>,
  ) {
    switch (queued._tag) {
      case "call":
        yield* Deferred.succeed(queued.reply, result);
        return;
      case "sendWait":
        yield* Deferred.succeed(queued.done, undefined);
        return;
      case "send":
        return;
      case "ask": {
        if (result.hasReply) {
          const replySchema = machine._replySchemas?.get(event._tag);
          if (replySchema === undefined) {
            yield* Deferred.succeed(queued.reply, result.reply);
            return;
          }
          const decoded = yield* Schema.decodeUnknownEffect(replySchema)(result.reply).pipe(
            Effect.catch((decodeError) =>
              Deferred.die(queued.reply, decodeError).pipe(Effect.andThen(Effect.die(decodeError))),
            ),
          );
          yield* Deferred.succeed(queued.reply, decoded);
          return;
        }
        if (result.deferReply && deferredReplyRef !== undefined) {
          deferredReplyRef.current = queued.reply;
          return;
        }
        yield* Deferred.fail(queued.reply, new NoReplyError({ actorId, eventTag: event._tag }));
      }
    }
  });

  const processQueued = Effect.fn("effect-machine.runtime.processQueued")(function* (
    queued: EventQueued,
  ) {
    const event = queued.event;
    const currentState = yield* SubscriptionRef.get(stateRef);

    // Check postpone rules
    if (hasPostponeRules && shouldPostpone(machine, currentState._tag, event._tag)) {
      return yield* postponeQueued(queued, currentState);
    }

    // Lifecycle: onEvent (actor emits @machine.event)
    if (lifecycle?.onEvent !== undefined) yield* lifecycle.onEvent(currentState, event);

    // Process event through core
    const result: ProcessEventResult<S> = yield* processEventCore(
      machine,
      currentState,
      event,
      self,
      stateScopeRef,
      system,
      actorId,
      hooks,
    );

    // Update state if transitioned
    if (result.transitioned) {
      yield* SubscriptionRef.set(stateRef, result.newState);
    }

    // Lifecycle: onStateChange (actor notifies listeners, annotates spans)
    if (lifecycle?.onStateChange !== undefined && result.transitioned) {
      yield* lifecycle.onStateChange(result, event);
    }

    yield* settleQueued(queued, event, result);

    // Lifecycle: onProcessed (actor publishes to transitionsPubSub)
    if (lifecycle?.onProcessed !== undefined && result.transitioned) {
      yield* lifecycle.onProcessed(result, event);
    }

    const shouldStop = result.isFinal && result.lifecycleRan;

    // Lifecycle: onFinal (actor emits @machine.stop)
    if (shouldStop && lifecycle?.onFinal !== undefined) {
      yield* lifecycle.onFinal(result.newState);
    }

    return {
      shouldStop,
      stateChanged: result.lifecycleRan,
      result,
    };
  });

  // Shutdown helper — settles postponed, drains queue, closes scopes
  const shutdown = (exitReason: ActorExit<S>) =>
    Effect.gen(function* () {
      yield* Ref.set(stoppedRef, true);
      if (lifecycle?.onShutdown !== undefined) yield* lifecycle.onShutdown();
      settlePostponed(postponed, actorId, forkEffect);
      // Drain remaining events non-blocking
      const remaining = yield* Queue.clear(eventQueue);
      for (const entry of remaining) {
        if (entry._tag === "sendWait") {
          forkEffect(Deferred.succeed(entry.done, undefined));
        } else if (entry._tag === "ask") {
          forkEffect(
            Deferred.fail(entry.reply, new NoReplyError({ actorId, eventTag: entry.event._tag })),
          );
        } else if (entry._tag === "call") {
          // Settle with a stopped result
          const currentState = yield* SubscriptionRef.get(stateRef);
          forkEffect(
            Deferred.succeed(entry.reply, {
              newState: currentState,
              previousState: currentState,
              transitioned: false,
              lifecycleRan: false,
              isFinal: machine.finalStates.has(currentState._tag),
              hasReply: false,
              deferReply: false,
              reply: undefined,
              postponed: false,
            }),
          );
        }
      }
      yield* Scope.close(stateScopeRef.current, Exit.void);
      // actorScope is closed by the generation owner fiber (which observes loop exit),
      // or by stop(). Not closed here — the loop just sets the exit reason and returns.
      yield* setExit(exitReason);
    });

  while (true) {
    const queued = yield* Queue.take(eventQueue);

    // Drain: graceful shutdown — process remaining queue then stop
    if (queued._tag === "drain") {
      yield* shutdown(ActorExit.Stopped);
      yield* Deferred.succeed(queued.done, undefined);
      return;
    }

    // queued is narrowed: drain is handled above, so it's always an event-bearing variant here
    const eventQueued = queued;
    // SAFETY: createRuntime captures and supplies the machine's R before forking this loop.
    const processInner = processQueued(eventQueued) as Effect.Effect<ProcessQueuedResult<S>>;
    const wrapped =
      wrapProcess !== undefined
        ? Effect.gen(function* () {
            const currentState = yield* SubscriptionRef.get(stateRef);
            return yield* wrapProcess(currentState, eventQueued.event, processInner);
          })
        : processInner;

    const { shouldStop, stateChanged } = yield* wrapped.pipe(
      Effect.catchCause((cause) => {
        // On defect: settle the current event's Deferred, run shutdown cleanup, then die
        if (queued._tag === "sendWait") {
          forkEffect(Deferred.failCause(queued.done, cause));
        } else if (queued._tag === "ask") {
          forkEffect(Deferred.die(queued.reply, cause));
        } else if (queued._tag === "call") {
          forkEffect(Deferred.failCause(queued.reply, cause));
        }
        // Determine defect phase from cause
        const phase: DefectPhase = "transition";
        return shutdown(ActorExit.Defect(cause, phase)).pipe(
          Effect.andThen(Effect.failCause(cause)),
        );
      }),
    );

    if (shouldStop) {
      const finalState = yield* SubscriptionRef.get(stateRef);
      yield* shutdown(ActorExit.Final(finalState));
      return;
    }

    // Drain postponed events — loop until stable
    let drainTriggered = stateChanged;
    while (drainTriggered && postponed.length > 0) {
      drainTriggered = false;
      const drained = postponed.splice(0);
      for (const entry of drained) {
        const drain = yield* processQueued(entry);
        if (drain.shouldStop) {
          const finalState = yield* SubscriptionRef.get(stateRef);
          yield* shutdown(ActorExit.Final(finalState));
          return;
        }
        if (drain.stateChanged) {
          drainTriggered = true;
        }
      }
    }
  }
});

/** Settle all pending Deferreds in the postpone buffer on shutdown. */
const settlePostponed = <E extends { readonly _tag: string }>(
  postponed: Exclude<RuntimeQueuedEvent<E>, { readonly _tag: "drain" }>[],
  actorId: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  forkFn: (effect: Effect.Effect<any>) => Fiber.Fiber<any>,
): void => {
  for (const entry of postponed) {
    if (entry._tag === "ask") {
      forkFn(Deferred.fail(entry.reply, new NoReplyError({ actorId, eventTag: entry.event._tag })));
    } else if (entry._tag === "sendWait") {
      forkFn(Deferred.succeed(entry.done, undefined));
    }
    // call entries in postpone buffer were already settled on postpone
    // send entries have no Deferred
  }
  postponed.length = 0;
};
