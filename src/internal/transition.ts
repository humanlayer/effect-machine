/**
 * Transition execution and indexing.
 *
 * Combines:
 * - Transition execution logic (for event processing, simulation, test harness)
 * - Event processing core (shared between actor and cluster entity)
 * - O(1) indexed lookup by state/event tag
 *
 * @internal
 */
import { Cause, Effect, Exit, Scope } from "effect";

import type { Machine, MachineRef, Transition, SpawnEffect, HandlerContext } from "../machine.js";
import type { ActorSystemService } from "../actor.js";
import type { SlotsDef, MachineContext } from "../slot.js";
import { isEffect, isReplyResult, isDeferReplyResult, INTERNAL_ENTER_EVENT } from "./utils.js";
import type { ReplyResult, DeferReplyResult } from "./utils.js";

// ============================================================================
// Transition Execution
// ============================================================================

/**
 * Result of executing a transition.
 */
export interface TransitionExecutionResult<S> {
  /** New state after transition (or current state if no transition matched) */
  readonly newState: S;
  /** Whether a transition was executed */
  readonly transitioned: boolean;
  /** Whether reenter was specified on the transition */
  readonly reenter: boolean;
}

/**
 * Run a transition handler and return the new state.
 * Shared logic for executing handlers with proper context.
 *
 * Used by:
 * - executeTransition (actor event loop, testing)
 * - Machine.replay (event sourcing restore)
 *
 * @internal
 */
export const runTransitionHandler = Effect.fn("effect-machine.runTransitionHandler")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  machine: Machine<S, E, R, any, any, SD>,
  transition: Transition<S, E, SD, R>,
  state: S,
  event: E,
  self: MachineRef<E>,
  system: ActorSystemService,
  actorId: string,
) {
  const ctx: MachineContext<S, E, MachineRef<E>> = { actorId, state, event, self, system };
  const slots = machine._slots;

  const handlerCtx: HandlerContext<S, E, SD> = { state, event, slots };
  const raw = transition.handler(handlerCtx);

  const resolved = isEffect(raw)
    ? yield* (
        // SAFETY: isEffect established the runtime branch; handler typing supplies its result domains.
        (raw as Effect.Effect<S | ReplyResult<S, unknown> | DeferReplyResult<S>, never, R>).pipe(
          Effect.provideService(machine.Context, ctx),
        )
      )
    : raw;

  // Detect branded ReplyResult (created via Machine.reply())
  if (isReplyResult(resolved)) {
    return {
      newState: resolved.state,
      hasReply: true,
      deferReply: false,
      reply: resolved.reply,
    };
  }

  // Detect branded DeferReplyResult (created via Machine.deferReply())
  if (isDeferReplyResult(resolved)) {
    return {
      newState: resolved.state,
      hasReply: false,
      deferReply: true,
      reply: undefined,
    };
  }

  return { newState: resolved, hasReply: false, deferReply: false, reply: undefined };
});

/**
 * Execute a transition for a given state and event.
 * Handles transition resolution, handler invocation, and guard/effect slot creation.
 *
 * Used by:
 * - processEvent in actor.ts (actual actor event loop)
 * - simulate in testing.ts (pure transition simulation)
 * - createTestHarness.send in testing.ts (step-by-step testing)
 *
 * @internal
 */
export const executeTransition = Effect.fn("effect-machine.executeTransition")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  machine: Machine<S, E, R, any, any, SD>,
  currentState: S,
  event: E,
  self: MachineRef<E>,
  system: ActorSystemService,
  actorId: string,
) {
  const transition = resolveTransition(machine, currentState, event);

  if (transition === undefined) {
    return {
      newState: currentState,
      transitioned: false,
      reenter: false,
      hasReply: false,
      deferReply: false,
      reply: undefined,
    };
  }

  const { newState, hasReply, deferReply, reply } = yield* runTransitionHandler(
    machine,
    transition,
    currentState,
    event,
    self,
    system,
    actorId,
  );

  return {
    newState,
    transitioned: true,
    reenter: transition.reenter === true,
    hasReply,
    deferReply,
    reply,
  };
});

// ============================================================================
// Event Processing Core (shared by actor and entity-machine)
// ============================================================================

/**
 * Optional hooks for event processing inspection/tracing.
 */
export interface ProcessEventHooks<S, E> {
  /** Called before running spawn effects */
  readonly onSpawnEffect?: (state: S) => Effect.Effect<void>;
  /** Called after transition completes */
  readonly onTransition?: (from: S, to: S, event: E) => Effect.Effect<void>;
  /** Called when a transition handler or spawn effect fails with a defect */
  readonly onError?: (info: ProcessEventError<S, E>) => Effect.Effect<void>;
  /** Called when a forked spawn fiber defects — signals the runtime to set exitDeferred */
  readonly onSpawnDefect?: (cause: Cause.Cause<unknown>) => Effect.Effect<void>;
}

/**
 * Error info for inspection hooks.
 */
export interface ProcessEventError<S, E> {
  readonly phase: "transition" | "spawn";
  readonly state: S;
  readonly event: E;
  readonly cause: Cause.Cause<unknown>;
}

/**
 * Result of processing an event through the machine.
 */
export interface ProcessEventResult<S> {
  /** New state after processing */
  readonly newState: S;
  /** Previous state before processing */
  readonly previousState: S;
  /** Whether a transition occurred */
  readonly transitioned: boolean;
  /** Whether lifecycle effects ran (state change or reenter) */
  readonly lifecycleRan: boolean;
  /** Whether new state is final */
  readonly isFinal: boolean;
  /** Whether the handler provided a reply (structural, not value-based) */
  readonly hasReply: boolean;
  /** Whether the handler deferred the reply to a spawn handler (Machine.deferReply) */
  readonly deferReply: boolean;
  /** Domain reply value from handler (used by ask). Only meaningful when hasReply is true. */
  readonly reply?: unknown;
  /** Whether the event was postponed (buffered for retry after next state change) */
  readonly postponed: boolean;
}

/**
 * Check if an event should be postponed in the current state.
 * @internal
 */
export const shouldPostpone = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  machine: Machine<S, E, R, any, any, any>,
  stateTag: string,
  eventTag: string,
): boolean => {
  for (const rule of machine.postponeRules) {
    if (rule.stateTag === stateTag && rule.eventTag === eventTag) {
      return true;
    }
  }
  return false;
};

/**
 * Process a single event through the machine.
 *
 * Handles:
 * - Transition execution
 * - State scope lifecycle (close old, create new)
 * - Running spawn effects
 *
 * Optional hooks allow inspection/tracing without coupling to specific impl.
 *
 * @internal
 */
export const processEventCore = Effect.fn("effect-machine.processEventCore")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  machine: Machine<S, E, R, any, any, SD>,
  currentState: S,
  event: E,
  self: MachineRef<E>,
  stateScopeRef: { current: Scope.Closeable },
  system: ActorSystemService,
  actorId: string,
  hooks?: ProcessEventHooks<S, E>,
) {
  // Execute transition (defect-aware)
  const result = yield* executeTransition(machine, currentState, event, self, system, actorId).pipe(
    Effect.catchCause((cause) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.interrupt;
      }
      const onError = hooks?.onError;
      if (onError === undefined) {
        return Effect.failCause(cause).pipe(Effect.orDie);
      }
      return onError({
        phase: "transition",
        state: currentState,
        event,
        cause,
      }).pipe(Effect.andThen(Effect.failCause(cause).pipe(Effect.orDie)));
    }),
  );

  if (!result.transitioned) {
    return {
      newState: currentState,
      previousState: currentState,
      transitioned: false,
      lifecycleRan: false,
      isFinal: false,
      hasReply: false,
      deferReply: false,
      reply: undefined,
      postponed: false,
    };
  }

  const newState = result.newState;
  const stateTagChanged = newState._tag !== currentState._tag;
  const runLifecycle = stateTagChanged || result.reenter;

  if (runLifecycle) {
    // Close old state scope (interrupts spawn fibers)
    yield* Scope.close(stateScopeRef.current, Exit.void);

    // Create new state scope
    stateScopeRef.current = yield* Scope.make();

    // Hook: transition complete (before spawn effects)
    if (hooks?.onTransition !== undefined) {
      yield* hooks.onTransition(currentState, newState, event);
    }

    // Hook: about to run spawn effects
    if (hooks?.onSpawnEffect !== undefined) {
      yield* hooks.onSpawnEffect(newState);
    }

    // Run spawn effects for new state
    // SAFETY: internal lifecycle events are consumed only by state effects and carry the required tag.
    const enterEvent = { _tag: INTERNAL_ENTER_EVENT } as E;
    yield* runSpawnEffects(
      machine,
      newState,
      enterEvent,
      self,
      stateScopeRef.current,
      system,
      actorId,
      hooks?.onError,
      hooks?.onSpawnDefect,
    );
  }

  return {
    newState,
    previousState: currentState,
    transitioned: true,
    lifecycleRan: runLifecycle,
    isFinal: machine.finalStates.has(newState._tag),
    hasReply: result.hasReply,
    deferReply: result.deferReply,
    reply: result.reply,
    postponed: false,
  };
});

/**
 * Run spawn effects for a state (forked into state scope, auto-cancelled on state exit).
 *
 * @internal
 */
export const runSpawnEffects = Effect.fn("effect-machine.runSpawnEffects")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  machine: Machine<S, E, R, any, any, SD>,
  state: S,
  event: E,
  self: MachineRef<E>,
  stateScope: Scope.Closeable,
  system: ActorSystemService,
  actorId: string,
  onError?: (info: ProcessEventError<S, E>) => Effect.Effect<void>,
  onSpawnDefect?: (cause: Cause.Cause<unknown>) => Effect.Effect<void>,
) {
  const spawnEffects = findSpawnEffects(machine, state._tag);
  const ctx: MachineContext<S, E, MachineRef<E>> = { actorId, state, event, self, system };
  const slots = machine._slots;
  const reportError = onError;
  const defectSignal = onSpawnDefect;

  for (const spawnEffect of spawnEffects) {
    // Fork the spawn effect into the state scope - interrupted when scope closes
    const effect = spawnEffect
      .handler({
        actorId,
        state,
        event,
        self,
        slots,
        system,
      })
      .pipe(
        Effect.provideService(machine.Context, ctx),
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.interrupt;
          }
          const report =
            reportError !== undefined
              ? reportError({ phase: "spawn", state, event, cause })
              : Effect.void;
          // Signal spawn defect to runtime (if provided) so it can set exitDeferred
          const signal = defectSignal !== undefined ? defectSignal(cause) : Effect.void;
          return report.pipe(
            Effect.andThen(signal),
            Effect.andThen(Effect.failCause(cause).pipe(Effect.orDie)),
          );
        }),
      );

    yield* Effect.forkScoped(effect).pipe(Effect.provideService(Scope.Scope, stateScope));
  }
});

/**
 * Resolve which transition should fire for a given state and event.
 * Uses indexed O(1) lookup. First matching transition wins.
 */
export const resolveTransition = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Schema fields need wide acceptance
  machine: Machine<S, E, R, any, any, any>,
  currentState: S,
  event: E,
): (typeof machine.transitions)[number] | undefined => {
  const candidates = findTransitions(machine, currentState._tag, event._tag);
  return candidates[0];
};

// ============================================================================
// Transition Index (O(1) Lookup)
// ============================================================================

/**
 * Index structure: stateTag -> eventTag -> transitions[]
 * Array preserves registration order for guard cascade evaluation.
 */
type TransitionIndex<S, E, SD extends SlotsDef, R> = Map<
  string,
  Map<string, Array<Transition<S, E, SD, R>>>
>;

/**
 * Index for spawn effects: stateTag -> effects[]
 */
type SpawnIndex<S, E, SD extends SlotsDef, R> = Map<string, Array<SpawnEffect<S, E, SD, R>>>;

/**
 * Combined index for a machine
 */
interface MachineIndex<S, E, SD extends SlotsDef, R> {
  readonly transitions: TransitionIndex<S, E, SD, R>;
  readonly spawn: SpawnIndex<S, E, SD, R>;
}

// Module-level cache - WeakMap allows GC of unreferenced machines
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const indexCache = new WeakMap<object, MachineIndex<any, any, any, any>>();

/**
 * Invalidate cached index for a machine (call after mutation).
 */
export const invalidateIndex = <M extends object>(machine: M): void => {
  indexCache.delete(machine);
};

/**
 * Build transition index from machine definition.
 * O(n) where n = number of transitions.
 */
const buildTransitionIndex = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  SD extends SlotsDef,
  R,
>(
  transitions: ReadonlyArray<Transition<S, E, SD, R>>,
): TransitionIndex<S, E, SD, R> => {
  const index: TransitionIndex<S, E, SD, R> = new Map();

  for (const t of transitions) {
    let stateMap = index.get(t.stateTag);
    if (stateMap === undefined) {
      stateMap = new Map();
      index.set(t.stateTag, stateMap);
    }

    let eventList = stateMap.get(t.eventTag);
    if (eventList === undefined) {
      eventList = [];
      stateMap.set(t.eventTag, eventList);
    }

    eventList.push(t);
  }

  return index;
};

/**
 * Build spawn index from machine definition.
 */
const buildSpawnIndex = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  SD extends SlotsDef,
  R,
>(
  effects: ReadonlyArray<SpawnEffect<S, E, SD, R>>,
): SpawnIndex<S, E, SD, R> => {
  const index: SpawnIndex<S, E, SD, R> = new Map();

  for (const e of effects) {
    let stateList = index.get(e.stateTag);
    if (stateList === undefined) {
      stateList = [];
      index.set(e.stateTag, stateList);
    }
    stateList.push(e);
  }

  return index;
};

/**
 * Get or build index for a machine.
 */
const getIndex = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Schema fields need wide acceptance
  machine: Machine<S, E, R, any, any, SD>,
): MachineIndex<S, E, SD, R> => {
  // SAFETY: each cache entry is created from and keyed by this exact machine instance.
  let index = indexCache.get(machine) as MachineIndex<S, E, SD, R> | undefined;
  if (index === undefined) {
    index = {
      transitions: buildTransitionIndex(machine.transitions),
      spawn: buildSpawnIndex(machine.spawnEffects),
    };
    indexCache.set(machine, index);
  }
  return index;
};

/**
 * Find all transitions matching a state/event pair.
 * Returns empty array if no matches.
 *
 * O(1) lookup after first access (index is lazily built).
 */
export const findTransitions = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef = Record<string, never>,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Schema fields need wide acceptance
  machine: Machine<S, E, R, any, any, SD>,
  stateTag: string,
  eventTag: string,
): ReadonlyArray<Transition<S, E, SD, R>> => {
  const index = getIndex(machine);
  const specific = index.transitions.get(stateTag)?.get(eventTag) ?? [];
  if (specific.length > 0) return specific;
  // Fallback to wildcard transitions
  return index.transitions.get("*")?.get(eventTag) ?? [];
};

/**
 * Find all spawn effects for a state.
 * Returns empty array if no matches.
 *
 * O(1) lookup after first access (index is lazily built).
 */
export const findSpawnEffects = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef = Record<string, never>,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Schema fields need wide acceptance
  machine: Machine<S, E, R, any, any, SD>,
  stateTag: string,
): ReadonlyArray<SpawnEffect<S, E, SD, R>> => {
  const index = getIndex(machine);
  return index.spawn.get(stateTag) ?? [];
};
