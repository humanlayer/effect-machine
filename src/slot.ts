/**
 * Slot module — unified, schema-based parameterized slots.
 *
 * Replaces the split Guards/Effects API with a single `Slot.define` + `Slot.fn`.
 * Each slot declares its parameter schema and (optional) return schema.
 * Handlers receive only params — machine context is accessed via `yield* machine.Context`.
 *
 * @example
 * ```ts
 * import { Slot } from "@humanlayer/effect-machine"
 * import { Schema } from "effect"
 *
 * const MySlots = Slot.define({
 *   canRetry: Slot.fn({ max: Schema.Number }, Schema.Boolean),
 *   isValid: Slot.fn({}, Schema.Boolean),
 *   fetchData: Slot.fn({ url: Schema.String }),
 *   notify: Slot.fn({ message: Schema.String }),
 * })
 *
 * // Used in handlers:
 * .on(State.X, Event.Y, ({ slots }) =>
 *   Effect.gen(function* () {
 *     if (yield* slots.canRetry({ max: 3 })) {
 *       yield* slots.fetchData({ url: "/api" })
 *       return State.Next
 *     }
 *     return state
 *   })
 * )
 * ```
 *
 * @module
 */
import { Schema, Context, Effect } from "effect";
import type { ActorSystemService } from "./actor.js";

// ============================================================================
// Type-level utilities
// ============================================================================

/** Schema fields definition (like Schema.Struct.Fields) */
type Fields = Record<string, Schema.Top>;

/** Extract the type from schema fields (used for parameters) */
type FieldsToParams<F extends Fields> = keyof F extends never
  ? void
  : Schema.Schema.Type<Schema.Struct<F>>;

// ============================================================================
// SlotFnDef — individual slot definition
// ============================================================================

/**
 * Definition of a single slot function.
 * Created via `Slot.fn(params, returnSchema?)`.
 *
 * Carries both type-level information and materialized schemas
 * for runtime validation and serialization.
 */
export interface SlotFnDef<F extends Fields = Fields, Return = void> {
  readonly _tag: "SlotFnDef";
  readonly fields: F;
  /** Return schema — undefined means void */
  readonly returnSchema: Schema.Schema<Return> | undefined;
  /** Materialized input schema (Schema.Struct of fields, or Schema.Void for empty) */
  readonly inputSchema: Schema.Codec<FieldsToParams<F>>;
  /** Materialized output schema (returnSchema or Schema.Void) */
  readonly outputSchema: Schema.Codec<Return>;
}

/**
 * Define a single slot function with parameter schema and optional return schema.
 *
 * @example
 * ```ts
 * // Guard-like: returns boolean
 * Slot.fn({ max: Schema.Number }, Schema.Boolean)
 *
 * // Effect-like: returns void (default)
 * Slot.fn({ url: Schema.String })
 *
 * // No params, returns boolean
 * Slot.fn({}, Schema.Boolean)
 * ```
 */
export const fn: {
  <F extends Fields, Return>(fields: F, returnSchema: Schema.Schema<Return>): SlotFnDef<F, Return>;
  <F extends Fields>(fields: F): SlotFnDef<F>;
} = <F extends Fields, Return = void>(
  fields: F,
  returnSchema?: Schema.Schema<Return>,
): SlotFnDef<F, Return> => {
  const hasFields = Object.keys(fields).length > 0;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const inputSchema = hasFields ? Schema.Struct(fields) : (Schema.Void as any);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const outputSchema = returnSchema ?? (Schema.Void as any);
  return {
    _tag: "SlotFnDef",
    fields,
    returnSchema: returnSchema,
    inputSchema,
    outputSchema,
  };
};

// ============================================================================
// SlotsDef — definition record
// ============================================================================

/**
 * Record of slot definitions. Keys are slot names, values are SlotFnDef.
 */
export type SlotsDef = Record<string, SlotFnDef<Fields, unknown>>;

// ============================================================================
// SlotsSchema — returned by Slot.define()
// ============================================================================

/**
 * Slots schema — returned by `Slot.define()`. Passed to `Machine.make({ slots })`.
 */
export interface SlotsSchema<D extends SlotsDef> {
  readonly _tag: "SlotsSchema";
  readonly definitions: D;
  /** Schema for slot requests `{ _tag: "SlotRequest", name, params }`. For RPC request payloads. */
  readonly requestSchema: Schema.Codec<SlotRequest<D>>;
  /** Schema for slot results `{ _tag: "SlotResult", name, result }`. For RPC response payloads. */
  readonly resultSchema: Schema.Codec<SlotResult<D>>;
  /** Schema for slot invocations `{ _tag: "SlotInvocation", name, params, result }`. For persistence/logging. */
  readonly invocationSchema: Schema.Codec<SlotInvocation<D>>;
  /** Create callable slot proxies (used by Machine internally) */
  readonly _createSlots: (
    resolve: <N extends keyof D & string>(
      name: N,
      params: SlotParams<D[N]>,
    ) => Effect.Effect<SlotReturn<D[N]>>,
  ) => SlotCalls<D>;
}

/**
 * A serialized slot request — captures name and params (no result).
 * Used for RPC request payloads.
 */
export type SlotRequest<D extends SlotsDef> = {
  readonly [K in keyof D & string]: {
    readonly _tag: "SlotRequest";
    readonly name: K;
    readonly params: SlotParams<D[K]>;
  };
}[keyof D & string];

/**
 * A serialized slot result — captures name and result (no params).
 * Used for RPC response payloads.
 */
export type SlotResult<D extends SlotsDef> = {
  readonly [K in keyof D & string]: {
    readonly _tag: "SlotResult";
    readonly name: K;
    readonly result: SlotReturn<D[K]>;
  };
}[keyof D & string];

/**
 * A serialized slot invocation — captures name, params, and result.
 * Used for persistence, logging, and audit trails.
 */
export type SlotInvocation<D extends SlotsDef> = {
  readonly [K in keyof D & string]: {
    readonly _tag: "SlotInvocation";
    readonly name: K;
    readonly params: SlotParams<D[K]>;
    readonly result: SlotReturn<D[K]>;
  };
}[keyof D & string];

// ============================================================================
// SlotCalls — callable slot proxies available in handler context
// ============================================================================

/** Extract params type from a SlotFnDef */
type SlotParams<D extends SlotFnDef<Fields, unknown>> =
  D extends SlotFnDef<infer F, unknown> ? FieldsToParams<F> : never;

/** Extract return type from a SlotFnDef */
type SlotReturn<D extends SlotFnDef<Fields, unknown>> =
  D extends SlotFnDef<Fields, infer R> ? R : never;

/**
 * A callable slot — function that takes params and returns Effect<Return>.
 */
export interface SlotCall<Name extends string, Params, Return> {
  readonly _tag: "Slot";
  readonly name: Name;
  (params: Params): Effect.Effect<Return>;
}

/**
 * Convert slot definitions to callable slot proxies.
 */
export type SlotCalls<D extends SlotsDef> = {
  readonly [K in keyof D & string]: SlotCall<K, SlotParams<D[K]>, SlotReturn<D[K]>>;
};

// ============================================================================
// SlotHandler / ProvideSlots — handler implementations at spawn time
// ============================================================================

/**
 * Slot handler implementation.
 * Receives only params — use `yield* machine.Context` for machine context.
 */
export type SlotHandler<Params, Return, R = never> = (
  params: Params,
) => Return | Effect.Effect<Return, never, R>;

/**
 * Handler implementations for all slots in a definition.
 */
export type ProvideSlots<D extends SlotsDef, R = never> = {
  readonly [K in keyof D & string]: SlotHandler<SlotParams<D[K]>, SlotReturn<D[K]>, R>;
};

/** Check if a SlotsDef has any actual keys */
export type HasSlotKeys<SD extends SlotsDef> = [keyof SD] extends [never]
  ? false
  : SD extends Record<string, never>
    ? false
    : true;

// ============================================================================
// Machine Context Tag
// ============================================================================

/**
 * Type for machine context — state, event, and self reference.
 * Shared across all machines via MachineContextTag.
 */
export interface MachineContext<State, Event, Self> {
  readonly actorId: string;
  readonly state: State;
  readonly event: Event;
  readonly self: Self;
  readonly system: ActorSystemService;
}

/**
 * Shared Context tag for all machines.
 * Single module-level tag instead of per-machine allocation.
 * @internal
 */
/* eslint-disable @typescript-eslint/no-explicit-any -- generic context tag */
export class MachineContextTag extends Context.Service<
  MachineContextTag,
  MachineContext<any, any, any>
>()("@humanlayer/effect-machine/slot/MachineContextTag") {}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ============================================================================
// Slot.define — factory
// ============================================================================

/**
 * Define a set of slots with parameter and return schemas.
 *
 * @example
 * ```ts
 * const MySlots = Slot.define({
 *   canRetry: Slot.fn({ max: Schema.Number }, Schema.Boolean),
 *   fetchData: Slot.fn({ url: Schema.String }),
 *   notify: Slot.fn({ message: Schema.String }),
 * })
 * ```
 */
export const define = <D extends SlotsDef>(definitions: D): SlotsSchema<D> => {
  // Build per-slot invocation schemas, then union them
  const names = Object.keys(definitions);
  /* eslint-disable @typescript-eslint/no-explicit-any */
  const requestSchemas: Array<Schema.Schema<any>> = [];
  const resultSchemas: Array<Schema.Schema<any>> = [];
  const invocationSchemas: Array<Schema.Schema<any>> = [];
  for (const name of names) {
    const def = definitions[name];
    if (def === undefined) continue;
    requestSchemas.push(
      Schema.TaggedStruct("SlotRequest", { name: Schema.Literal(name), params: def.inputSchema }),
    );
    resultSchemas.push(
      Schema.TaggedStruct("SlotResult", { name: Schema.Literal(name), result: def.outputSchema }),
    );
    invocationSchemas.push(
      Schema.TaggedStruct("SlotInvocation", {
        name: Schema.Literal(name),
        params: def.inputSchema,
        result: def.outputSchema,
      }),
    );
  }

  const buildUnion = <T>(schemas: Array<Schema.Schema<any>>): Schema.Codec<T> =>
    schemas.length === 0 ? Schema.Never : (Schema.Union(schemas as any) as any);

  const requestSchema = buildUnion<SlotRequest<D>>(requestSchemas);
  const resultSchema = buildUnion<SlotResult<D>>(resultSchemas);
  const invocationSchema = buildUnion<SlotInvocation<D>>(invocationSchemas);
  /* eslint-enable @typescript-eslint/no-explicit-any */

  return {
    _tag: "SlotsSchema",
    definitions,
    requestSchema,
    resultSchema,
    invocationSchema,
    _createSlots: (resolve) => {
      const slots: Record<string, unknown> = {};
      for (const name of names) {
        const slot = (params: unknown) => resolve(name, params as SlotParams<D[typeof name]>);
        Object.defineProperty(slot, "_tag", { value: "Slot", enumerable: true });
        Object.defineProperty(slot, "name", { value: name, enumerable: true });
        slots[name] = slot;
      }
      return slots as SlotCalls<D>;
    },
  };
};

// ============================================================================
// Slot.of — normalize ProvideSlots into SlotCalls
// ============================================================================

/**
 * Convert raw slot handler implementations into the callable `SlotCalls` form.
 *
 * Handlers that return plain values are wrapped in `Effect.succeed`.
 * Handlers that return Effects are called directly inside `Effect.suspend`.
 *
 * @example
 * ```ts
 * const provided = yield* myExtension.slots(ctx)
 * const slots = Slot.of(slotsSchema, provided)
 * // slots.mySlot({ param: 1 }) returns Effect<ReturnType>
 * ```
 */
const of = <D extends SlotsDef>(
  slotsSchema: SlotsSchema<D>,
  provided: ProvideSlots<D>,
): SlotCalls<D> => {
  const slots: Record<string, unknown> = {};
  for (const name of Object.keys(slotsSchema.definitions)) {
    const handler = (provided as Record<string, (params: unknown) => unknown>)[name];
    if (handler === undefined) continue;
    const call = (params: unknown): Effect.Effect<unknown, never> =>
      Effect.suspend((): Effect.Effect<unknown, never> => {
        const result = handler(params);
        return Effect.isEffect(result)
          ? (result as Effect.Effect<unknown, never>)
          : Effect.succeed(result);
      });
    Object.defineProperty(call, "_tag", { value: "Slot", enumerable: true });
    Object.defineProperty(call, "name", { value: name, enumerable: true });
    slots[name] = call;
  }
  return slots as SlotCalls<D>;
};

// ============================================================================
// Slot namespace export
// ============================================================================

/** @deprecated Prefer Effect `Context.Service` dependencies and `Layer` provisioning. */
export const Slot = {
  fn,
  define,
  of,
} as const;
