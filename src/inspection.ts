import { Effect, Option, Context, type Schema } from "effect";

// ============================================================================
// Type-level helpers
// ============================================================================

/**
 * Resolve a type param: if it's a Schema, extract `.Type`; otherwise use as-is.
 */
type ResolveType<T> = T extends Schema.Schema<infer A> ? A : T;

// ============================================================================
// Inspection Events
// ============================================================================

/**
 * Event emitted when an actor is spawned
 */
export interface SpawnEvent<S> {
  readonly type: "@machine.spawn";
  readonly actorId: string;
  readonly initialState: S;
  readonly timestamp: number;
}

/**
 * Event emitted when an actor receives an event
 */
export interface EventReceivedEvent<S, E> {
  readonly type: "@machine.event";
  readonly actorId: string;
  readonly state: S;
  readonly event: E;
  readonly timestamp: number;
}

/**
 * Event emitted when a transition occurs
 */
export interface TransitionEvent<S, E> {
  readonly type: "@machine.transition";
  readonly actorId: string;
  readonly fromState: S;
  readonly toState: S;
  readonly event: E;
  readonly timestamp: number;
}

/**
 * Event emitted when a spawn effect runs
 */
export interface EffectEvent<S> {
  readonly type: "@machine.effect";
  readonly actorId: string;
  readonly effectType: "spawn";
  readonly state: S;
  readonly timestamp: number;
}

export interface TaskEvent<S> {
  readonly type: "@machine.task";
  readonly actorId: string;
  readonly state: S;
  readonly taskName?: string;
  readonly phase: "start" | "success" | "failure" | "interrupt";
  readonly error?: string;
  readonly timestamp: number;
}

/**
 * Event emitted when a transition handler or spawn effect fails with a defect
 */
export interface ErrorEvent<S, E> {
  readonly type: "@machine.error";
  readonly actorId: string;
  readonly phase: "transition" | "spawn";
  readonly state: S;
  readonly event: E;
  readonly error: string;
  readonly timestamp: number;
}

/**
 * Event emitted when an actor stops
 */
export interface StopEvent<S> {
  readonly type: "@machine.stop";
  readonly actorId: string;
  readonly finalState: S;
  readonly timestamp: number;
}

/**
 * Union of all inspection events
 */
export type InspectionEvent<S, E> =
  | SpawnEvent<S>
  | EventReceivedEvent<S, E>
  | TransitionEvent<S, E>
  | EffectEvent<S>
  | TaskEvent<S>
  | ErrorEvent<S, E>
  | StopEvent<S>;

/**
 * Convenience alias for untyped inspection events.
 * Useful for general-purpose inspectors that don't need specific state/event types.
 * State and event fields are typed as `{ readonly _tag: string }` so discriminated
 * access to `_tag` works without casting.
 */
export type AnyInspectionEvent = InspectionEvent<
  { readonly _tag: string },
  { readonly _tag: string }
>;

// ============================================================================
// InspectorService Service
// ============================================================================

/**
 * Inspector interface for observing machine behavior
 */
export type InspectorHandler<S, E> = (event: InspectionEvent<S, E>) => void | Effect.Effect<void>;

export interface InspectorService<S, E> {
  readonly onInspect: InspectorHandler<S, E>;
}

/**
 * Inspector service tag - optional service for machine introspection
 * Uses `any` types to allow variance flexibility when providing the service
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export class Inspector extends Context.Service<Inspector, InspectorService<any, any>>()(
  "@humanlayer/effect-machine/inspection/Inspector",
) {}

/**
 * Create an inspector from a callback function.
 *
 * Type params accept either raw tagged types or Schema constructors:
 * - `makeInspector(cb)` — defaults to `AnyInspectionEvent`
 * - `makeInspector<MyState, MyEvent>(cb)` — explicit tagged types
 * - `makeInspector<typeof MyState, typeof MyEvent>(cb)` — schema constructors (auto-extracts `.Type`)
 */
export const makeInspector = <S = { readonly _tag: string }, E = { readonly _tag: string }>(
  onInspect: InspectorHandler<ResolveType<S>, ResolveType<E>>,
): InspectorService<ResolveType<S>, ResolveType<E>> => ({ onInspect });

export const makeInspectorEffect = <S = { readonly _tag: string }, E = { readonly _tag: string }>(
  onInspect: (event: InspectionEvent<ResolveType<S>, ResolveType<E>>) => Effect.Effect<void>,
): InspectorService<ResolveType<S>, ResolveType<E>> => ({ onInspect });

const inspectionEffect = <S, E>(
  inspector: InspectorService<S, E>,
  event: InspectionEvent<S, E>,
): Effect.Effect<void> => {
  const result = inspector.onInspect(event);
  return Effect.isEffect(result) ? result : Effect.void;
};

export const combineInspectors = <S, E>(
  ...inspectors: ReadonlyArray<InspectorService<S, E>>
): InspectorService<S, E> => ({
  onInspect: (event) =>
    Effect.forEach(
      inspectors,
      (inspector) => inspectionEffect(inspector, event).pipe(Effect.catchCause(() => Effect.void)),
      { concurrency: "unbounded", discard: true },
    ),
});

export interface TracingInspectorOptions<S, E> {
  readonly spanName?: string | ((event: InspectionEvent<S, E>) => string);
  readonly attributes?: (
    event: InspectionEvent<S, E>,
  ) => Readonly<Record<string, string | number | boolean>>;
  readonly eventName?: (event: InspectionEvent<S, E>) => string;
}

const inspectionSpanName = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
>(
  event: InspectionEvent<S, E>,
) => {
  switch (event.type) {
    case "@machine.spawn":
      return `Machine.inspect ${event.initialState._tag}`;
    case "@machine.event":
      return `Machine.inspect ${event.event._tag}`;
    case "@machine.transition":
      return `Machine.inspect ${event.fromState._tag}->${event.toState._tag}`;
    case "@machine.effect":
      return `Machine.inspect ${event.effectType}`;
    case "@machine.task":
      return `Machine.inspect task:${event.phase}`;
    case "@machine.error":
      return `Machine.inspect ${event.phase}`;
    case "@machine.stop":
      return `Machine.inspect ${event.finalState._tag}`;
  }
};

const inspectionTraceName = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
>(
  event: InspectionEvent<S, E>,
) => {
  switch (event.type) {
    case "@machine.spawn":
      return `machine.spawn ${event.initialState._tag}`;
    case "@machine.event":
      return `machine.event ${event.event._tag}`;
    case "@machine.transition":
      return `machine.transition ${event.fromState._tag}->${event.toState._tag}`;
    case "@machine.effect":
      return `machine.effect ${event.effectType}`;
    case "@machine.task":
      return `machine.task ${event.phase}${event.taskName === undefined ? "" : ` ${event.taskName}`}`;
    case "@machine.error":
      return `machine.error ${event.phase}`;
    case "@machine.stop":
      return `machine.stop ${event.finalState._tag}`;
  }
};

const inspectionAttributes = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
>(
  event: InspectionEvent<S, E>,
): Record<string, string | number | boolean> => {
  const shared = {
    "machine.actor.id": event.actorId,
    "machine.inspection.type": event.type,
  };

  switch (event.type) {
    case "@machine.spawn":
      return { ...shared, "machine.state.initial": event.initialState._tag };
    case "@machine.event":
      return {
        ...shared,
        "machine.state.current": event.state._tag,
        "machine.event.tag": event.event._tag,
      };
    case "@machine.transition":
      return {
        ...shared,
        "machine.state.from": event.fromState._tag,
        "machine.state.to": event.toState._tag,
        "machine.event.tag": event.event._tag,
      };
    case "@machine.effect":
      return {
        ...shared,
        "machine.state.current": event.state._tag,
        "machine.effect.kind": event.effectType,
      };
    case "@machine.task":
      return {
        ...shared,
        "machine.state.current": event.state._tag,
        "machine.task.phase": event.phase,
        ...(event.taskName === undefined ? {} : { "machine.task.name": event.taskName }),
      };
    case "@machine.error":
      return {
        ...shared,
        "machine.phase": event.phase,
        "machine.state.current": event.state._tag,
      };
    case "@machine.stop":
      return { ...shared, "machine.state.final": event.finalState._tag };
  }
};

export const tracingInspector = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
>(
  options?: TracingInspectorOptions<S, E>,
): InspectorService<S, E> => ({
  onInspect: (event) => {
    const spanName =
      typeof options?.spanName === "function" ? options.spanName(event) : options?.spanName;
    const traceName = options?.eventName?.(event) ?? inspectionTraceName(event);
    const attributes = {
      ...inspectionAttributes(event),
      ...(options?.attributes?.(event) ?? {}),
    };

    return Effect.gen(function* () {
      const currentSpan = yield* Effect.option(Effect.currentSpan);
      if (Option.isSome(currentSpan)) {
        currentSpan.value.event(traceName, BigInt(event.timestamp) * 1_000_000n, {
          actorId: event.actorId,
          inspectionType: event.type,
        });
      }
    }).pipe(Effect.withSpan(spanName ?? inspectionSpanName(event), { attributes }));
  },
});

// ============================================================================
// Built-in Inspectors
// ============================================================================

/**
 * Console inspector that logs events in a readable format
 */
export const consoleInspector = (): InspectorService<
  { readonly _tag: string },
  { readonly _tag: string }
> =>
  makeInspectorEffect((event) => {
    const prefix = `[${event.actorId}]`;
    switch (event.type) {
      case "@machine.spawn":
        return Effect.log(`${prefix} spawned -> ${event.initialState._tag}`);
      case "@machine.event":
        return Effect.log(`${prefix} received ${event.event._tag} in ${event.state._tag}`);
      case "@machine.transition":
        return Effect.log(`${prefix} ${event.fromState._tag} -> ${event.toState._tag}`);
      case "@machine.effect":
        return Effect.log(`${prefix} ${event.effectType} effect in ${event.state._tag}`);
      case "@machine.task":
        return Effect.log(
          `${prefix} task ${event.phase} ${event.taskName ?? "<unnamed>"} in ${event.state._tag}`,
        );
      case "@machine.error":
        return Effect.log(
          `${prefix} error in ${event.phase} ${event.state._tag} - ${String(event.error)}`,
        );
      case "@machine.stop":
        return Effect.log(`${prefix} stopped in ${event.finalState._tag}`);
    }
  });

/**
 * Collecting inspector that stores events in an array for testing
 */
export const collectingInspector = <
  S extends { readonly _tag: string },
  E extends { readonly _tag: string },
>(
  events: InspectionEvent<S, E>[],
): InspectorService<S, E> => ({
  onInspect: (event) => {
    events.push(event);
  },
});
