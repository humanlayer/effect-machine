/**
 * Machine namespace - fluent builder API for state machines.
 *
 * @example
 * ```ts
 * import { Context, Effect, Layer, Schema } from "effect"
 * import { Machine, State, Event } from "@humanlayer/effect-machine"
 *
 * const MyState = State({ Idle: {}, Running: {}, Done: { count: Schema.Number } })
 * const MyEvent = Event({ Start: {}, Loaded: { count: Schema.Number } })
 *
 * class Counter extends Context.Service<
 *   Counter,
 *   { readonly load: () => Effect.Effect<number> }
 * >()("@app/Counter") {}
 *
 * const machine = Machine.make({
 *   state: MyState,
 *   event: MyEvent,
 *   initial: MyState.Idle,
 * })
 *   .on(MyState.Idle, MyEvent.Start, () => MyState.Running({ count: 0 }))
 *   .task(
 *     MyState.Running,
 *     () => Counter.pipe(Effect.flatMap((counter) => counter.load())),
 *     { onSuccess: (count) => MyEvent.Loaded({ count }) },
 *   )
 *   .on(MyState.Running, MyEvent.Loaded, ({ event }) => MyState.Done({ count: event.count }))
 *   .final(MyState.Done)
 *
 * const CounterLive = Layer.succeed(Counter, { load: () => Effect.succeed(0) })
 * const actor = yield* Machine.spawn(machine).pipe(Effect.provide(CounterLive))
 * ```
 *
 * @module
 */
import type { Context, Duration } from "effect";
import { Cause, Effect, Exit, Option, Random, Schema, Scope } from "effect";

import type { TransitionResult } from "./internal/utils.js";
import { getTag, stubSystem, makeReply, makeDeferReply } from "./internal/utils.js";
import type {
  TaggedOrConstructor,
  BrandedState,
  BrandedEvent,
  ExtractReply,
} from "./internal/brands.js";
import type { MachineStateSchema, MachineEventSchema, VariantsUnion } from "./schema.js";
import { SlotProvisionError, SlotCodecError, ProvisionValidationError } from "./errors.js";
import type { DuplicateActorError } from "./errors.js";
import {
  invalidateIndex,
  resolveTransition,
  runTransitionHandler,
  shouldPostpone,
} from "./internal/transition.js";
import { emitWithTimestamp } from "./internal/inspection.js";
import type { ActorRef, ActorSystemService } from "./actor.js";
import { Inspector as InspectorTag } from "./inspection.js";
import type { SlotsDef, SlotsSchema, SlotCalls, ProvideSlots, MachineContext } from "./slot.js";
import { MachineContextTag } from "./slot.js";

// ============================================================================
// Core types
// ============================================================================

/**
 * Self reference for sending events back to the machine
 */
export interface MachineRef<Event> {
  readonly send: (event: Event) => Effect.Effect<void>;
  /** Fire-and-forget alias for send (OTP gen_server:cast). */
  readonly cast: (event: Event) => Effect.Effect<void>;
  readonly spawn: <S2 extends { readonly _tag: string }, E2 extends { readonly _tag: string }, R2>(
    id: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    machine: Machine<S2, E2, R2, any, any, any>,
  ) => Effect.Effect<ActorRef<S2, E2>, DuplicateActorError, R2>;
  /**
   * Settle a deferred reply from a spawn handler.
   * Only usable when the transition handler returned `Machine.deferReply(state)`.
   * Returns true if a pending reply was settled, false if none was pending.
   */
  readonly reply: (value: unknown) => Effect.Effect<boolean>;
}

/**
 * Handler context passed to transition handlers
 */
export interface HandlerContext<State, Event, SD extends SlotsDef = Record<string, never>> {
  readonly state: State;
  readonly event: Event;
  readonly slots: SlotCalls<SD>;
}

/**
 * Handler context passed to state effect handlers (onEnter, spawn, background)
 */
export interface StateHandlerContext<State, Event, SD extends SlotsDef = Record<string, never>> {
  readonly actorId: string;
  readonly state: State;
  readonly event: Event;
  readonly self: MachineRef<Event>;
  readonly slots: SlotCalls<SD>;
  readonly system: ActorSystemService;
}

/**
 * Transition handler function.
 * When Reply is concrete (event has a reply schema), handler must return Machine.reply().
 * When Reply is never, handler returns plain state.
 */
export type TransitionHandler<S, E, NewState, SD extends SlotsDef, R, Reply = never> = (
  ctx: HandlerContext<S, E, SD>,
) => TransitionResult<NewState, R, Reply>;

/**
 * State effect handler function
 */
export type StateEffectHandler<S, E, SD extends SlotsDef, R> = (
  ctx: StateHandlerContext<S, E, SD>,
) => Effect.Effect<void, never, R>;

/**
 * Transition definition
 */
export interface Transition<State, Event, SD extends SlotsDef, R> {
  readonly stateTag: string;
  readonly eventTag: string;
  readonly handler: TransitionHandler<State, Event, State, SD, R>;
  readonly reenter?: boolean;
}

/**
 * Spawn effect - state-scoped forked effect
 */
export interface SpawnEffect<State, Event, SD extends SlotsDef, R> {
  readonly stateTag: string;
  readonly handler: StateEffectHandler<State, Event, SD, R>;
}

/**
 * Background effect - runs for entire machine lifetime
 */
export interface BackgroundEffect<State, Event, SD extends SlotsDef, R> {
  readonly handler: StateEffectHandler<State, Event, SD, R>;
}

// ============================================================================
// Options types
// ============================================================================

export interface TaskOptions<State, Event, SD extends SlotsDef, A, E1, ES, EF> {
  readonly onSuccess?: (value: A, ctx: StateHandlerContext<State, Event, SD>) => ES;
  readonly onFailure?: (cause: Cause.Cause<E1>, ctx: StateHandlerContext<State, Event, SD>) => EF;
  readonly name?: string;
}

// ============================================================================
// Recovery / Durability
// ============================================================================

/**
 * Recovery resolves the initial state for a generation. Runs during actor.start.
 *
 * For initial start (generation 0): loads persisted state.
 * For supervision restart (generation 1+): reloads state after crash.
 */
export interface Recovery<S> {
  readonly resolve: (ctx: RecoveryContext<S>) => Effect.Effect<Option.Option<S>>;
}

export interface RecoveryContext<S> {
  readonly actorId: string;
  readonly generation: number;
  readonly machineInitial: S;
}

/**
 * Durability saves state after committed transitions. Runs during runtime.
 */
export interface Durability<S, E> {
  readonly save: (commit: DurabilityCommit<S, E>) => Effect.Effect<void>;
  readonly shouldSave?: (state: S, previousState: S) => boolean;
}

export interface DurabilityCommit<S, E> {
  readonly actorId: string;
  readonly generation: number;
  readonly previousState: S;
  readonly nextState: S;
  readonly event: E;
}

/**
 * Actor lifecycle configuration.
 */
export interface Lifecycle<S, E> {
  readonly recovery?: Recovery<S>;
  readonly durability?: Durability<S, E>;
}

/**
 * Configuration for `.timeout()` — gen_statem-style state timeouts.
 *
 * Entering the state starts a timer. Leaving cancels it.
 * `.reenter()` restarts the timer with fresh state values.
 */
export interface TimeoutConfig<State, Event> {
  /** Duration before firing. Static or derived from current state. */
  readonly duration: Duration.Input | ((state: State) => Duration.Input);
  /** Event to send when the timer fires. Static or derived from current state. */
  readonly event: Event | ((state: State) => Event);
}

// ============================================================================
// Internal helpers
// ============================================================================

const emitTaskInspection = <S extends { readonly _tag: string }>(input: {
  readonly actorId: string;
  readonly state: S;
  readonly taskName: string | undefined;
  readonly phase: "start" | "success" | "failure" | "interrupt";
  readonly error?: string;
}) =>
  Effect.flatMap(Effect.serviceOption(InspectorTag), (inspector) =>
    Option.isNone(inspector)
      ? Effect.void
      : emitWithTimestamp(inspector.value, (timestamp) => ({
          type: "@machine.task",
          actorId: input.actorId,
          state: input.state,
          taskName: input.taskName,
          phase: input.phase,
          error: input.error,
          timestamp,
        })),
  );

// ============================================================================
// MakeConfig
// ============================================================================

export interface MakeConfig<
  SD extends Record<string, Schema.Struct.Fields>,
  ED extends Record<string, Schema.Struct.Fields>,
  S extends BrandedState,
  E extends BrandedEvent,
  SLD extends SlotsDef = Record<string, never>,
> {
  readonly state: MachineStateSchema<SD> & { Type: S };
  readonly event: MachineEventSchema<ED> & { Type: E };
  /** @deprecated Prefer Effect `Context.Service` dependencies in state effects. */
  readonly slots?: SlotsSchema<SLD>;
  readonly initial: S;
  /** @deprecated Only applies to the legacy slot API. */
  readonly slotValidation?: boolean;
}

// ============================================================================
// Provide types
// ============================================================================

// ============================================================================
// materializeMachine — internal slot binding at execution boundaries
// ============================================================================

/**
 * Bind slot handlers to a machine, returning a fresh copy with handlers installed.
 * If no handlers provided and machine has no slots, returns the machine as-is.
 * Validates that all required slots are provided and no extra slots are given.
 *
 * @internal — used by spawn, replay, simulate, test harness, entity-machine
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const materializeMachine = <S, E, R, SD extends SlotsDef>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  machine: Machine<S, E, R, any, any, SD>,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  handlers?: Record<string, any>,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): Machine<S, E, never, any, any, SD> => {
  if (handlers === undefined) {
    // Validate: slot-free machines can skip handlers, slotful machines must provide them
    if (
      machine._slotsSchema !== undefined &&
      Object.keys(machine._slotsSchema.definitions).length > 0
    ) {
      const missing = Object.keys(machine._slotsSchema.definitions);
      throw new ProvisionValidationError({ missing, extra: [] });
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return machine as any;
  }

  // Collect all required slot names
  const requiredSlots = new Set<string>();
  if (machine._slotsSchema !== undefined) {
    for (const name of Object.keys(machine._slotsSchema.definitions)) {
      requiredSlots.add(name);
    }
  }

  // Single-pass validation
  const providedSlots = new Set(Object.keys(handlers));
  const missing: string[] = [];
  const extra: string[] = [];

  for (const name of requiredSlots) {
    if (!providedSlots.has(name)) {
      missing.push(name);
    }
  }
  for (const name of providedSlots) {
    if (!requiredSlots.has(name)) {
      extra.push(name);
    }
  }

  if (missing.length > 0 || extra.length > 0) {
    throw new ProvisionValidationError({ missing, extra });
  }

  // Create fresh copy to avoid mutation bleed between actors
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = new Machine<S, E, never, any, any, SD>(
    machine.initial,
    machine.stateSchema,
    machine.eventSchema,
    machine._slotsSchema,
    machine._slotValidation,
  );

  // Copy arrays/sets
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (result as any)._transitions = [...machine._transitions];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (result as any)._finalStates = new Set(machine._finalStates);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (result as any)._spawnEffects = [...machine._spawnEffects];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (result as any)._backgroundEffects = [...machine._backgroundEffects];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (result as any)._postponeRules = [...machine._postponeRules];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (result as any)._replySchemas = machine._replySchemas;

  // Register handlers — single map
  if (machine._slotsSchema !== undefined) {
    for (const name of Object.keys(machine._slotsSchema.definitions)) {
      result._slotHandlers.set(name, handlers[name]);
    }
  }

  return result;
};

// ============================================================================
// Machine class
// ============================================================================

/**
 * Machine definition with fluent builder API.
 *
 * Type parameters:
 * - `State`: The state union type
 * - `Event`: The event union type
 * - `R`: Effect requirements
 * - `_SD`: State schema definition (for compile-time validation)
 * - `_ED`: Event schema definition (for compile-time validation)
 * - `SD`: Slot definitions
 */
export class Machine<
  State,
  Event,
  R = never,
  _SD extends Record<string, Schema.Struct.Fields> = Record<string, Schema.Struct.Fields>,
  _ED extends Record<string, Schema.Struct.Fields> = Record<string, Schema.Struct.Fields>,
  SD extends SlotsDef = Record<string, never>,
> {
  readonly initial: State;
  /** @internal */ readonly _transitions: Array<Transition<State, Event, SD, R>>;
  /** @internal */ readonly _spawnEffects: Array<SpawnEffect<State, Event, SD, R>>;
  /** @internal */ readonly _backgroundEffects: Array<BackgroundEffect<State, Event, SD, R>>;
  /** @internal */ readonly _finalStates: Set<string>;
  /** @internal */ readonly _postponeRules: Array<{
    readonly stateTag: string;
    readonly eventTag: string;
  }>;
  /** @internal */ readonly _slotsSchema?: SlotsSchema<SD>;
  /** @internal */ readonly _slotHandlers: Map<
    string,
    (params: unknown) => unknown | Effect.Effect<unknown, never, R>
  >;
  /** @internal */ readonly _slots: SlotCalls<SD>;
  /** @internal */ readonly _slotValidation: boolean;
  readonly stateSchema?: Schema.Schema<State>;
  readonly eventSchema?: Schema.Schema<Event>;
  /** @internal */ readonly _replySchemas: ReadonlyMap<string, Schema.Decoder<unknown>>;

  /**
   * Context tag for accessing machine state/event/self in slot handlers.
   * Uses shared module-level tag for all machines.
   */
  readonly Context: Context.Service<
    MachineContextTag,
    MachineContext<State, Event, MachineRef<Event>>
  > = MachineContextTag;

  // Public readonly views
  get transitions(): ReadonlyArray<Transition<State, Event, SD, R>> {
    return this._transitions;
  }
  get spawnEffects(): ReadonlyArray<SpawnEffect<State, Event, SD, R>> {
    return this._spawnEffects;
  }
  get backgroundEffects(): ReadonlyArray<BackgroundEffect<State, Event, SD, R>> {
    return this._backgroundEffects;
  }
  get finalStates(): ReadonlySet<string> {
    return this._finalStates;
  }
  get postponeRules(): ReadonlyArray<{ readonly stateTag: string; readonly eventTag: string }> {
    return this._postponeRules;
  }
  get slotsSchema(): SlotsSchema<SD> | undefined {
    return this._slotsSchema;
  }
  get replySchemas(): ReadonlyMap<string, Schema.Decoder<unknown>> {
    return this._replySchemas;
  }

  /** @internal */
  constructor(
    initial: State,
    stateSchema?: Schema.Schema<State>,
    eventSchema?: Schema.Schema<Event>,
    slotsSchema?: SlotsSchema<SD>,
    slotValidation = true,
  ) {
    this.initial = initial;
    this._transitions = [];
    this._spawnEffects = [];
    this._backgroundEffects = [];
    this._finalStates = new Set();
    this._postponeRules = [];
    this._slotsSchema = slotsSchema;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    this._replySchemas = (eventSchema as any)?._replySchemas ?? new Map();
    this._slotHandlers = new Map();
    this._slotValidation = slotValidation;
    this.stateSchema = stateSchema;
    this.eventSchema = eventSchema;

    // Precompile slot validators (decode input, decode output) if validation enabled
    const validators =
      slotValidation && slotsSchema !== undefined
        ? new Map(
            Object.entries(slotsSchema.definitions).map(([name, def]) => [
              name,
              {
                decodeInput: Schema.decodeUnknownSync(def.inputSchema),
                decodeOutput: Schema.decodeUnknownSync(def.outputSchema),
              },
            ]),
          )
        : undefined;

    // Create slot closures — unified single map
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const resolve = (name: string, params: unknown): Effect.Effect<any> =>
      Effect.flatMap(Effect.serviceOption(this.Context), (maybeCtx) => {
        if (Option.isNone(maybeCtx)) {
          return Effect.die("MachineContext not available");
        }
        const handler = this._slotHandlers.get(name);
        if (handler === undefined) {
          return Effect.die(new SlotProvisionError({ slotName: name, slotType: "slot" }));
        }

        // Validate input
        const validatedParams =
          validators !== undefined
            ? (() => {
                try {
                  const v = validators.get(name);
                  return v !== undefined ? v.decodeInput(params) : params;
                } catch (e) {
                  return Effect.die(
                    new SlotCodecError({
                      slotName: name,
                      phase: "input",
                      message: e instanceof Error ? e.message : String(e),
                    }),
                  );
                }
              })()
            : params;

        // If decodeInput returned an Effect.die, short-circuit
        if (Effect.isEffect(validatedParams)) {
          // @effect-diagnostics anyUnknownInErrorContext:off
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          return validatedParams as Effect.Effect<any>;
        }

        // Invoke handler
        const result = handler(validatedParams);

        // Wrap result into Effect
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        let resultEffect: Effect.Effect<any>;
        if (result === undefined || result === null) {
          resultEffect = Effect.void;
        } else if (Effect.isEffect(result)) {
          // @effect-diagnostics anyUnknownInErrorContext:off
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          resultEffect = result as Effect.Effect<any>;
        } else {
          resultEffect = Effect.succeed(result);
        }

        // Validate output
        if (validators !== undefined) {
          const v = validators.get(name);
          if (v !== undefined) {
            return Effect.flatMap(resultEffect, (value) => {
              try {
                const decoded = v.decodeOutput(value);
                return Effect.succeed(decoded);
              } catch (e) {
                return Effect.die(
                  new SlotCodecError({
                    slotName: name,
                    phase: "output",
                    message: e instanceof Error ? e.message : String(e),
                  }),
                );
              }
            });
          }
        }
        return resultEffect;
      });

    this._slots =
      this._slotsSchema !== undefined
        ? this._slotsSchema._createSlots(resolve)
        : ({} as SlotCalls<SD>);
  }

  // ---- on ----

  from<NS extends VariantsUnion<_SD> & BrandedState, R1>(
    state: TaggedOrConstructor<NS>,
    build: (scope: TransitionScope<State, Event, R, _SD, _ED, SD, NS>) => R1,
  ): Machine<State, Event, R, _SD, _ED, SD>;
  from<NS extends ReadonlyArray<TaggedOrConstructor<VariantsUnion<_SD> & BrandedState>>, R1>(
    states: NS,
    build: (
      scope: TransitionScope<
        State,
        Event,
        R,
        _SD,
        _ED,
        SD,
        NS[number] extends TaggedOrConstructor<infer S extends VariantsUnion<_SD> & BrandedState>
          ? S
          : never
      >,
    ) => R1,
  ): Machine<State, Event, R, _SD, _ED, SD>;
  from(
    stateOrStates:
      | TaggedOrConstructor<VariantsUnion<_SD> & BrandedState>
      | ReadonlyArray<TaggedOrConstructor<VariantsUnion<_SD> & BrandedState>>,
    build: (
      scope: TransitionScope<State, Event, R, _SD, _ED, SD, VariantsUnion<_SD> & BrandedState>,
    ) => unknown,
  ) {
    const states = Array.isArray(stateOrStates) ? stateOrStates : [stateOrStates];
    build(new TransitionScope(this, states));
    return this;
  }

  /** @internal */
  scopeTransition<
    NS extends VariantsUnion<_SD> & BrandedState,
    NE extends VariantsUnion<_ED> & BrandedEvent,
    RS extends VariantsUnion<_SD> & BrandedState,
  >(
    states: ReadonlyArray<TaggedOrConstructor<NS>>,
    event: TaggedOrConstructor<NE>,
    handler: TransitionHandler<NS, NE, RS, SD, never, ExtractReply<NE>>,
    reenter: boolean,
  ): Machine<State, Event, R, _SD, _ED, SD> {
    for (const state of states) {
      this.addTransition(
        state,
        event,
        handler as TransitionHandler<NS, NE, BrandedState, SD, never>,
        reenter,
      );
    }
    return this;
  }

  /** Register transition for a single state */
  on<
    NS extends VariantsUnion<_SD> & BrandedState,
    NE extends VariantsUnion<_ED> & BrandedEvent,
    RS extends VariantsUnion<_SD> & BrandedState,
  >(
    state: TaggedOrConstructor<NS>,
    event: TaggedOrConstructor<NE>,
    handler: TransitionHandler<NS, NE, RS, SD, never, ExtractReply<NE>>,
  ): Machine<State, Event, R, _SD, _ED, SD>;
  /** Register transition for multiple states (handler receives union of state types) */
  on<
    NS extends ReadonlyArray<TaggedOrConstructor<VariantsUnion<_SD> & BrandedState>>,
    NE extends VariantsUnion<_ED> & BrandedEvent,
    RS extends VariantsUnion<_SD> & BrandedState,
  >(
    states: NS,
    event: TaggedOrConstructor<NE>,
    handler: TransitionHandler<
      NS[number] extends TaggedOrConstructor<infer S> ? S : never,
      NE,
      RS,
      SD,
      never,
      ExtractReply<NE>
    >,
  ): Machine<State, Event, R, _SD, _ED, SD>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  on(stateOrStates: any, event: any, handler: any): Machine<State, Event, R, _SD, _ED, SD> {
    const states = Array.isArray(stateOrStates) ? stateOrStates : [stateOrStates];
    for (const s of states) {
      this.addTransition(s, event, handler, false);
    }
    return this;
  }

  // ---- reenter ----

  /**
   * Like `on()`, but forces onEnter/spawn to run even when transitioning to the same state tag.
   * Use this to restart timers, re-run spawned effects, or reset state-scoped effects.
   */
  /** Single state */
  reenter<
    NS extends VariantsUnion<_SD> & BrandedState,
    NE extends VariantsUnion<_ED> & BrandedEvent,
    RS extends VariantsUnion<_SD> & BrandedState,
  >(
    state: TaggedOrConstructor<NS>,
    event: TaggedOrConstructor<NE>,
    handler: TransitionHandler<NS, NE, RS, SD, never, ExtractReply<NE>>,
  ): Machine<State, Event, R, _SD, _ED, SD>;
  /** Multiple states */
  reenter<
    NS extends ReadonlyArray<TaggedOrConstructor<VariantsUnion<_SD> & BrandedState>>,
    NE extends VariantsUnion<_ED> & BrandedEvent,
    RS extends VariantsUnion<_SD> & BrandedState,
  >(
    states: NS,
    event: TaggedOrConstructor<NE>,
    handler: TransitionHandler<
      NS[number] extends TaggedOrConstructor<infer S> ? S : never,
      NE,
      RS,
      SD,
      never,
      ExtractReply<NE>
    >,
  ): Machine<State, Event, R, _SD, _ED, SD>;
  /* eslint-disable @typescript-eslint/no-explicit-any */
  reenter(stateOrStates: any, event: any, handler: any): Machine<State, Event, R, _SD, _ED, SD> {
    /* eslint-enable @typescript-eslint/no-explicit-any */
    const states = Array.isArray(stateOrStates) ? stateOrStates : [stateOrStates];
    for (const s of states) {
      this.addTransition(s, event, handler, true);
    }
    return this;
  }

  // ---- onAny ----

  /**
   * Register a wildcard transition that fires from any state when no specific transition matches.
   * Specific `.on()` transitions always take priority over `.onAny()`.
   */
  onAny<NE extends VariantsUnion<_ED> & BrandedEvent, RS extends VariantsUnion<_SD> & BrandedState>(
    event: TaggedOrConstructor<NE>,
    handler: TransitionHandler<VariantsUnion<_SD> & BrandedState, NE, RS, SD, never>,
  ): Machine<State, Event, R, _SD, _ED, SD> {
    const eventTag = getTag(event);
    const transition: Transition<State, Event, SD, R> = {
      stateTag: "*",
      eventTag,
      handler: handler as unknown as Transition<State, Event, SD, R>["handler"],
      reenter: false,
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (this._transitions as any[]).push(transition);
    invalidateIndex(this);
    return this;
  }

  /** @internal */
  private addTransition<NS extends BrandedState, NE extends BrandedEvent>(
    state: TaggedOrConstructor<NS>,
    event: TaggedOrConstructor<NE>,
    handler: TransitionHandler<NS, NE, BrandedState, SD, never>,
    reenter: boolean,
  ): Machine<State, Event, R, _SD, _ED, SD> {
    const stateTag = getTag(state);
    const eventTag = getTag(event);

    const transition: Transition<State, Event, SD, R> = {
      stateTag,
      eventTag,
      handler: handler as unknown as Transition<State, Event, SD, R>["handler"],
      reenter,
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (this._transitions as any[]).push(transition);
    invalidateIndex(this);

    return this;
  }

  // ---- spawn ----

  /**
   * State-scoped effect that is forked on state entry and automatically cancelled on state exit.
   *
   * @example
   * ```ts
   * machine.spawn(State.Loading, ({ self, state }) =>
   *   Effect.gen(function* () {
   *     yield* Effect.addFinalizer(() => Effect.log("Leaving Loading"));
   *     const data = yield* Http.get(state.url);
   *     yield* self.send(Event.Loaded({ data }));
   *   }),
   * );
   * ```
   */
  /** Single state */
  spawn<NS extends VariantsUnion<_SD> & BrandedState, R1>(
    state: TaggedOrConstructor<NS>,
    handler: StateEffectHandler<NS, VariantsUnion<_ED> & BrandedEvent, SD, Scope.Scope | R1>,
  ): Machine<State, Event, R | R1, _SD, _ED, SD>;
  /** Multiple states */
  spawn<NS extends ReadonlyArray<TaggedOrConstructor<VariantsUnion<_SD> & BrandedState>>, R1>(
    states: NS,
    handler: StateEffectHandler<
      NS[number] extends TaggedOrConstructor<infer S> ? S : never,
      VariantsUnion<_ED> & BrandedEvent,
      SD,
      Scope.Scope | R1
    >,
  ): Machine<State, Event, R | R1, _SD, _ED, SD>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  spawn(stateOrStates: any, handler: any): Machine<State, Event, R, _SD, _ED, SD> {
    const states = Array.isArray(stateOrStates) ? stateOrStates : [stateOrStates];
    for (const s of states) {
      const stateTag = getTag(s);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (this._spawnEffects as any[]).push({
        stateTag,
        handler: handler as unknown as SpawnEffect<State, Event, SD, R>["handler"],
      });
    }
    invalidateIndex(this);
    return this;
  }

  // ---- task ----

  /**
   * State-scoped task that runs on entry and sends success/failure events.
   * Interrupts do not emit failure events.
   *
   * Supports multi-state and shorthand overloads:
   * - `.task(State.X, run, { onSuccess, onFailure })` — explicit mapping
   * - `.task(State.X, run, { onFailure })` — shorthand when run returns Event directly
   * - `.task([State.X, State.Y], run, opts)` — multi-state
   */
  /** Single state — onSuccess optional (defaults to identity when task returns Event) */
  task<
    NS extends VariantsUnion<_SD> & BrandedState,
    A,
    E1,
    R1,
    ES extends VariantsUnion<_ED> & BrandedEvent,
    EF extends VariantsUnion<_ED> & BrandedEvent,
  >(
    state: TaggedOrConstructor<NS>,
    run: (
      ctx: StateHandlerContext<NS, VariantsUnion<_ED> & BrandedEvent, SD>,
    ) => Effect.Effect<A, E1, Scope.Scope | R1>,
    options: TaskOptions<NS, VariantsUnion<_ED> & BrandedEvent, SD, A, E1, ES, EF>,
  ): Machine<State, Event, R | R1, _SD, _ED, SD>;
  /** Multiple states, explicit onSuccess */
  task<
    NS extends ReadonlyArray<TaggedOrConstructor<VariantsUnion<_SD> & BrandedState>>,
    A,
    E1,
    R1,
    ES extends VariantsUnion<_ED> & BrandedEvent,
    EF extends VariantsUnion<_ED> & BrandedEvent,
  >(
    states: NS,
    run: (
      ctx: StateHandlerContext<
        NS[number] extends TaggedOrConstructor<infer S> ? S : never,
        VariantsUnion<_ED> & BrandedEvent,
        SD
      >,
    ) => Effect.Effect<A, E1, Scope.Scope | R1>,
    options: TaskOptions<
      NS[number] extends TaggedOrConstructor<infer S> ? S : never,
      VariantsUnion<_ED> & BrandedEvent,
      SD,
      A,
      E1,
      ES,
      EF
    >,
  ): Machine<State, Event, R | R1, _SD, _ED, SD>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  task(stateOrStates: any, run: any, options: any): Machine<State, Event, R, _SD, _ED, SD> {
    const handler = Effect.fn("effect-machine.task")(function* (
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ctx: StateHandlerContext<any, any, SD>,
    ) {
      yield* emitTaskInspection({
        actorId: ctx.actorId,
        state: ctx.state,
        taskName: options.name,
        phase: "start",
      });

      // @effect-diagnostics anyUnknownInErrorContext:off — implementation overload uses `any`
      const exit = yield* Effect.exit(run(ctx));

      if (Exit.isSuccess(exit)) {
        yield* emitTaskInspection({
          actorId: ctx.actorId,
          state: ctx.state,
          taskName: options.name,
          phase: "success",
        });
        const successEvent =
          options.onSuccess !== undefined ? options.onSuccess(exit.value, ctx) : exit.value;
        yield* ctx.self.send(successEvent);
        yield* Effect.yieldNow;
        return;
      }

      const cause = exit.cause;
      if (Cause.hasInterruptsOnly(cause)) {
        yield* emitTaskInspection({
          actorId: ctx.actorId,
          state: ctx.state,
          taskName: options.name,
          phase: "interrupt",
        });
        return;
      }
      yield* emitTaskInspection({
        actorId: ctx.actorId,
        state: ctx.state,
        taskName: options.name,
        phase: "failure",
        error: Cause.pretty(cause),
      });
      if (options.onFailure !== undefined) {
        yield* ctx.self.send(options.onFailure(cause, ctx));
        yield* Effect.yieldNow;
        return;
      }
      // @effect-diagnostics anyUnknownInErrorContext:off
      return yield* Effect.failCause(cause).pipe(Effect.orDie);
    });

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.spawn(stateOrStates, handler as any);
  }

  // ---- timeout ----

  /**
   * State timeout — gen_statem's `state_timeout`.
   *
   * Entering the state starts a timer. Leaving cancels it (via state scope).
   * `.reenter()` restarts the timer with fresh state values.
   * Compiles to `.task()` internally — preserves `@machine.task` inspection events.
   *
   * @example
   * ```ts
   * machine
   *   .timeout(State.Loading, {
   *     duration: Duration.seconds(30),
   *     event: Event.Timeout,
   *   })
   *   // Dynamic duration from state
   *   .timeout(State.Retrying, {
   *     duration: (state) => Duration.seconds(state.backoff),
   *     event: Event.GiveUp,
   *   })
   * ```
   */
  timeout<NS extends VariantsUnion<_SD> & BrandedState>(
    state: TaggedOrConstructor<NS>,
    config: TimeoutConfig<NS, VariantsUnion<_ED> & BrandedEvent>,
  ): Machine<State, Event, R, _SD, _ED, SD> {
    const stateTag = getTag(state);
    const resolveDuration =
      typeof config.duration === "function"
        ? (config.duration as (state: NS) => Duration.Input)
        : () => config.duration as Duration.Input;
    const resolveEvent =
      typeof config.event === "function"
        ? (config.event as (state: NS) => VariantsUnion<_ED> & BrandedEvent)
        : () => config.event as VariantsUnion<_ED> & BrandedEvent;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (this as any).task(state, (ctx: any) => Effect.sleep(resolveDuration(ctx.state)), {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      onSuccess: (_: void, ctx: any) => resolveEvent(ctx.state),
      name: `$timeout:${stateTag}`,
    });
  }

  // ---- background ----

  /**
   * Machine-lifetime effect that is forked on actor spawn and runs until the actor stops.
   *
   * @example
   * ```ts
   * machine.background(({ self }) =>
   *   Effect.forever(
   *     Effect.sleep("30 seconds").pipe(Effect.andThen(self.send(Event.Ping))),
   *   ),
   * );
   * ```
   */
  background<R1>(
    handler: StateEffectHandler<State, Event, SD, Scope.Scope | R1>,
  ): Machine<State, Event, R | R1, _SD, _ED, SD> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (this._backgroundEffects as any[]).push({
      handler: handler as unknown as BackgroundEffect<State, Event, SD, R>["handler"],
    });
    return this;
  }

  // ---- postpone ----

  /**
   * Postpone events — gen_statem's event postpone.
   *
   * When a matching event arrives in the given state, it is buffered instead of
   * processed. After the next state transition (tag change), all buffered events
   * are drained through the loop in FIFO order.
   *
   * Reply-bearing events (from `call`/`ask`) in the postpone buffer are settled
   * with `ActorStoppedError` on stop/interrupt/final-state.
   *
   * @example
   * ```ts
   * machine
   *   .postpone(State.Connecting, Event.Data)           // single event
   *   .postpone(State.Connecting, [Event.Data, Event.Cmd]) // multiple events
   * ```
   */
  postpone<NS extends VariantsUnion<_SD> & BrandedState>(
    state: TaggedOrConstructor<NS>,
    events:
      | TaggedOrConstructor<VariantsUnion<_ED> & BrandedEvent>
      | ReadonlyArray<TaggedOrConstructor<VariantsUnion<_ED> & BrandedEvent>>,
  ): Machine<State, Event, R, _SD, _ED, SD> {
    const stateTag = getTag(state);
    const eventList = Array.isArray(events) ? events : [events];
    for (const ev of eventList) {
      const eventTag = getTag(ev);
      this._postponeRules.push({ stateTag, eventTag });
    }
    return this;
  }

  // ---- final ----

  final<NS extends VariantsUnion<_SD> & BrandedState>(
    state: TaggedOrConstructor<NS>,
  ): Machine<State, Event, R, _SD, _ED, SD> {
    const stateTag = getTag(state);
    this._finalStates.add(stateTag);
    return this;
  }

  // ---- build ----

  // ---- Static factory ----

  static make<
    SD extends Record<string, Schema.Struct.Fields>,
    ED extends Record<string, Schema.Struct.Fields>,
    S extends BrandedState,
    E extends BrandedEvent,
    SLD extends SlotsDef = Record<string, never>,
  >(config: MakeConfig<SD, ED, S, E, SLD>): Machine<S, E, never, SD, ED, SLD> {
    return new Machine<S, E, never, SD, ED, SLD>(
      config.initial,
      config.state as unknown as Schema.Schema<S>,
      config.event as unknown as Schema.Schema<E>,
      config.slots,
      config.slotValidation ?? true,
    );
  }
}

class TransitionScope<
  State,
  Event,
  R,
  _SD extends Record<string, Schema.Struct.Fields>,
  _ED extends Record<string, Schema.Struct.Fields>,
  SD extends SlotsDef,
  SelectedState extends VariantsUnion<_SD> & BrandedState,
> {
  constructor(
    private readonly machine: Machine<State, Event, R, _SD, _ED, SD>,
    private readonly states: ReadonlyArray<TaggedOrConstructor<SelectedState>>,
  ) {}

  on<NE extends VariantsUnion<_ED> & BrandedEvent, RS extends VariantsUnion<_SD> & BrandedState>(
    event: TaggedOrConstructor<NE>,
    handler: TransitionHandler<SelectedState, NE, RS, SD, never, ExtractReply<NE>>,
  ): TransitionScope<State, Event, R, _SD, _ED, SD, SelectedState> {
    this.machine.scopeTransition(this.states, event, handler, false);
    return this;
  }

  reenter<
    NE extends VariantsUnion<_ED> & BrandedEvent,
    RS extends VariantsUnion<_SD> & BrandedState,
  >(
    event: TaggedOrConstructor<NE>,
    handler: TransitionHandler<SelectedState, NE, RS, SD, never, ExtractReply<NE>>,
  ): TransitionScope<State, Event, R, _SD, _ED, SD, SelectedState> {
    this.machine.scopeTransition(this.states, event, handler, true);
    return this;
  }
}

// ============================================================================
// make function (alias for Machine.make)
// ============================================================================

export const make = Machine.make;

// ============================================================================
// spawn function - simple actor creation without ActorSystem
// ============================================================================

import { createActor, ActorScope } from "./actor.js";
import type { Supervision } from "./supervision.js";

/**
 * Spawn an actor directly without ActorSystem ceremony.
 * Accepts a `Machine` directly. For slotful machines, pass `{ slots }` in options.
 *
 * **Single actor, no registry.** Caller manages lifetime via `actor.stop`.
 * If an `ActorScope` exists in context, cleanup attaches automatically on scope close.
 * Use `Machine.scoped` to bridge from `Scope` to `ActorScope`.
 *
 * For registry, lookup by ID, persistence, or multi-actor coordination,
 * use `ActorSystem` / `system.spawn` instead.
 *
 * @example
 * ```ts
 * // Fire-and-forget — caller manages lifetime
 * const actor = yield* Machine.spawn(machine);
 * yield* actor.start;
 * yield* actor.send(Event.Start);
 * yield* actor.awaitFinal;
 * yield* actor.stop;
 *
 * // Scope-aware — auto-cleans up on scope close
 * yield* Effect.scoped(Machine.scoped(Effect.gen(function* () {
 *   const actor = yield* Machine.spawn(machine);
 *   yield* actor.start;
 *   yield* actor.send(Event.Start);
 *   // actor.stop called automatically when scope closes
 * })));
 * ```
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyMachine<S, E, R> = Machine<S, E, R, any, any, any>;

const spawnImpl = Effect.fn("effect-machine.spawn")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
>(
  machine: AnyMachine<S, E, R>,
  idOrOptions?:
    | string
    | {
        id?: string;
        hydrate?: S;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        slots?: Record<string, any>;
        supervision?: Supervision.Policy;
        lifecycle?: Lifecycle<S, E>;
      },
) {
  const opts = typeof idOrOptions === "string" ? { id: idOrOptions } : idOrOptions;
  const actorId = opts?.id ?? `actor-${(yield* Random.next).toString(36).slice(2)}`;
  const materialized = materializeMachine(machine, opts?.slots);
  const actor = yield* createActor(actorId, materialized as AnyMachine<S, E, never>, {
    initialState: opts?.hydrate,
    supervision: opts?.supervision,
    lifecycle: opts?.lifecycle,
  });

  // If an ActorScope exists in context, attach cleanup automatically
  const maybeScope = yield* Effect.serviceOption(ActorScope);
  if (Option.isSome(maybeScope)) {
    yield* Scope.addFinalizer(maybeScope.value, actor.stop);
  }

  return actor;
});

/**
 * Spawn an actor from a machine.
 *
 * For machines with slots, pass implementations via `{ slots: { ... } }`.
 *
 * @example
 * ```ts
 * // No slots
 * const actor = yield* Machine.spawn(machine);
 *
 * // With slots
 * const actor = yield* Machine.spawn(machine, {
 *   slots: { canRetry: ({ max }) => attempts < max },
 * });
 *
 * // With lifecycle (recovery + durability)
 * const actor = yield* Machine.spawn(machine, {
 *   lifecycle: {
 *     recovery: { resolve: (ctx) => storage.get("actor-state") },
 *     durability: { save: (commit) => storage.set("actor-state", commit.nextState) },
 *   },
 * });
 * ```
 */
export const spawn: <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
  SD extends SlotsDef = Record<string, never>,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  machine: Machine<S, E, R, any, any, SD>,
  options?:
    | string
    | {
        id?: string;
        hydrate?: S;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        slots?: ProvideSlots<SD, any>;
        supervision?: Supervision.Policy;
        lifecycle?: Lifecycle<S, E>;
      },
) => Effect.Effect<ActorRef<S, E>, never, R> = spawnImpl;

/**
 * Wrap an effect to provide an `ActorScope` from the current `Scope`.
 *
 * Actors spawned inside will attach cleanup finalizers to this scope,
 * so they are automatically stopped when the scope closes.
 *
 * @example
 * ```ts
 * yield* Effect.scoped(
 *   Machine.scoped(
 *     Effect.gen(function* () {
 *       const actor = yield* Machine.spawn(machine);
 *       yield* actor.start;
 *       // actor auto-stopped when scope closes
 *     }),
 *   ),
 * );
 * ```
 */
export const scoped = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R | Scope.Scope> =>
  Effect.flatMap(Effect.service(Scope.Scope), (scope) =>
    Effect.provideService(effect, ActorScope, scope),
  );

/**
 * Replay events through a machine to compute the final state.
 *
 * Folds events through transition handlers — the same state computation
 * that runs in a live actor, minus runtime side effects:
 * - Transition handlers run (pure or effectful — they compute state)
 * - `self.send`/`self.spawn` are no-ops (stubbed)
 * - Spawn effects, background effects, and timeouts do NOT run
 * - Postpone rules are respected (postponed events drain on state change)
 * - Final states stop replay (remaining events ignored)
 * - Unhandled events are silently skipped (matches live actor behavior)
 *
 * Use `from` to replay from a snapshot midpoint instead of the machine's initial state.
 *
 * @example
 * ```ts
 * // Restore from event log
 * const state = yield* Machine.replay(machine, savedEvents);
 * const actor = yield* Machine.spawn(machine, { hydrate: state });
 *
 * // Restore from snapshot + tail events
 * const state = yield* Machine.replay(machine, tailEvents, { from: snapshot });
 * const actor = yield* Machine.spawn(machine, { hydrate: state });
 * ```
 */
const replayImpl = Effect.fn("effect-machine.replay")(function* <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
  R,
>(
  input: AnyMachine<S, E, R>,
  events: ReadonlyArray<E>,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  options?: { from?: S; slots?: Record<string, any> },
) {
  const machine = materializeMachine(input, options?.slots);
  let state: S = options?.from ?? machine.initial;

  const hasPostponeRules = machine.postponeRules.length > 0;
  const postponed: E[] = [];

  const dummySend = Effect.fn("effect-machine.replay.send")((_event: E) => Effect.void);
  const self: MachineRef<E> = {
    send: dummySend,
    cast: dummySend,
    spawn: () => Effect.die("spawn not supported in replay"),
    reply: () => Effect.succeed(false),
  };

  for (const event of events) {
    // Final state stops replay
    if (machine.finalStates.has(state._tag)) break;

    // Check postpone rules
    if (hasPostponeRules && shouldPostpone(machine, state._tag, event._tag)) {
      postponed.push(event);
      continue;
    }

    const transition = resolveTransition(machine, state, event);
    if (transition !== undefined) {
      const result = yield* runTransitionHandler(
        machine,
        transition,
        state,
        event,
        self,
        stubSystem,
        "replay",
      );
      const previousTag = state._tag;
      state = result.newState;

      // Drain postponed events on state change — loop until stable
      const stateChanged = state._tag !== previousTag || transition.reenter === true;
      if (stateChanged && postponed.length > 0) {
        let drainTag = previousTag;
        while (state._tag !== drainTag && postponed.length > 0) {
          if (machine.finalStates.has(state._tag)) break;
          drainTag = state._tag;
          const drained = postponed.splice(0);
          for (const postponedEvent of drained) {
            if (machine.finalStates.has(state._tag)) break;
            if (shouldPostpone(machine, state._tag, postponedEvent._tag)) {
              postponed.push(postponedEvent);
              continue;
            }
            const pTransition = resolveTransition(machine, state, postponedEvent);
            if (pTransition !== undefined) {
              const pResult = yield* runTransitionHandler(
                machine,
                pTransition,
                state,
                postponedEvent,
                self,
                stubSystem,
                "replay",
              );
              state = pResult.newState;
            }
          }
        }
      }
    }
  }

  return state;
});

export const replay: {
  <
    S extends { readonly _tag: string },
    E extends { readonly _tag: string },
    R,
    SD extends SlotsDef = Record<string, never>,
  >(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    machine: Machine<S, E, R, any, any, SD>,
    events: ReadonlyArray<E>,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    options?: { from?: S; slots?: ProvideSlots<SD, any> },
  ): Effect.Effect<S, never, R>;
} = replayImpl;

// Transition lookup (introspection)
export { findTransitions } from "./internal/transition.js";

// Reply helpers
export const reply = makeReply;
export const deferReply = makeDeferReply;
export type { ReplyResult, DeferReplyResult } from "./internal/utils.js";

// Supervision (Machine.supervise) deferred to a dedicated PR — requires
// deeper integration with the runtime kernel for defect detection and
// restart semantics that don't fit cleanly into the current ActorRef surface.
