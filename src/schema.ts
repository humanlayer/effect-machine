/**
 * Schema-first State/Event definitions for effect-machine.
 *
 * MachineSchema provides a single source of truth that combines:
 * - Schema for validation/serialization
 * - Variant constructors (like Data.taggedEnum)
 * - $is and $match helpers for pattern matching
 * - Brand integration for compile-time safety
 *
 * @example
 * ```ts
 * import { State, Event, Machine } from "@humanlayer/effect-machine"
 *
 * // Define schema-first state
 * const OrderState = State({
 *   Pending: { orderId: Schema.String },
 *   Shipped: { trackingId: Schema.String },
 * })
 *
 * // Infer type from schema
 * type OrderState = typeof OrderState.Type
 *
 * // Use constructors
 * const pending = OrderState.Pending({ orderId: "123" })
 *
 * // Pattern match
 * OrderState.$match(state, {
 *   Pending: (s) => `Order ${s.orderId} pending`,
 *   Shipped: (s) => `Shipped: ${s.trackingId}`,
 * })
 *
 * // Use as Schema for persistence/cluster
 * machine.pipe(Machine.persist({ stateSchema: OrderState, ... }))
 * ```
 *
 * @module
 */
import { Schema } from "effect";
import type { FullStateBrand, FullEventBrand, ReplyTypeBrand } from "./internal/brands.js";
import { InvalidSchemaError, MissingMatchHandlerError } from "./errors.js";

// ============================================================================
// Reply Schema Symbol
// ============================================================================

const ReplySchemaSymbol: unique symbol = Symbol.for("@humanlayer/effect-machine/ReplySchema");
export type ReplySchemaSymbol = typeof ReplySchemaSymbol;

/**
 * Fields annotated with a reply schema.
 * Structurally identical to Schema.Struct.Fields at runtime,
 * but carries the reply schema type at compile time.
 */
export type ReplyFields<F extends Schema.Struct.Fields, RS extends Schema.Schema<unknown>> = F & {
  readonly [ReplySchemaSymbol]: RS;
};

/**
 * Payload fields that actually flow through constructors and runtime values.
 * Reply schema metadata is type-only and must not leak into payload shapes.
 */
type PayloadFields<F extends Schema.Struct.Fields> = {
  readonly [K in keyof F as K extends ReplySchemaSymbol ? never : K]: F[K];
};

type TaggedSource = { readonly _tag: string };

// eslint-disable-next-line anti-slop/no-unsafe-dictionary-type -- dynamic schema construction is isolated behind this alias
type DynamicFields = Record<string, unknown>;

// ============================================================================
// Type Helpers
// ============================================================================

/**
 * Extract the TypeScript type from a TaggedStruct schema
 */
type TaggedStructType<Tag extends string, Fields extends Schema.Struct.Fields> = Schema.Schema.Type<
  Schema.TaggedStruct<Tag, PayloadFields<Fields>>
>;

/**
 * Build variant schemas type from definition
 */
type VariantSchemas<D extends Record<string, Schema.Struct.Fields>> = {
  readonly [K in keyof D & string]: Schema.TaggedStruct<K, PayloadFields<D[K]>>;
};

/**
 * Build union type from variant schemas.
 * Reply-bearing variants carry ReplyTypeBrand<R> for ask() inference.
 */
export type VariantsUnion<D extends Record<string, Schema.Struct.Fields>> = {
  [K in keyof D & string]: TaggedStructType<K, D[K]> &
    (D[K] extends { readonly [ReplySchemaSymbol]: Schema.Schema<infer R> }
      ? ReplyTypeBrand<R>
      : unknown);
}[keyof D & string];

/**
 * Check if fields are empty (no required string properties).
 * Symbol keys (like ReplySchemaSymbol) are metadata, not payload fields.
 */
type IsEmptyFields<Fields extends Schema.Struct.Fields> = string & keyof Fields extends never
  ? true
  : false;

/**
 * Resolve the reply brand for a variant's fields.
 * If fields carry ReplySchemaSymbol, adds ReplyTypeBrand<R>.
 */
type VariantReplyBrand<Fields extends Schema.Struct.Fields> = Fields extends {
  readonly [ReplySchemaSymbol]: Schema.Schema<infer R>;
}
  ? ReplyTypeBrand<R>
  : unknown;

/**
 * Constructor functions for each variant.
 * Empty structs: plain values with `_tag`: `State.Idle`
 * Non-empty structs require args: `State.Loading({ url })`
 *
 * Each variant also has a `with` method for constructing from a source object,
 * copying matching fields and overriding with a partial.
 * The source type uses `object` to accept branded state types without index signature issues.
 * Reply-bearing variants carry ReplyTypeBrand<R> for ask() type inference.
 */
type VariantConstructors<D extends Record<string, Schema.Struct.Fields>, Brand> = {
  readonly [K in keyof D & string]: IsEmptyFields<D[K]> extends true
    ? TaggedStructType<K, D[K]> &
        Brand &
        VariantReplyBrand<D[K]> & {
          readonly with: (source: TaggedSource) => TaggedStructType<K, D[K]> & Brand;
        }
    : ((
        args: Schema.Struct.Type<PayloadFields<D[K]>>,
      ) => TaggedStructType<K, D[K]> & Brand & VariantReplyBrand<D[K]>) & {
        readonly with: (
          source: TaggedSource,
          partial?: Partial<Schema.Struct.Type<PayloadFields<D[K]>>>,
        ) => TaggedStructType<K, D[K]> & Brand;
        readonly _tag: K;
      };
};

/**
 * Keys present in ALL variants (intersection of field names).
 * Used by union-level `with` to accept only fields safe to update
 * regardless of which variant the source is.
 */
type SharedKeys<D extends Record<string, Schema.Struct.Fields>> = keyof D[keyof D & string] &
  string;

type SharedFields<D extends Record<string, Schema.Struct.Fields>> = {
  readonly [K in SharedKeys<D>]?: D[keyof D & string][K] extends Schema.Top
    ? Schema.Schema.Type<D[keyof D & string][K]>
    : never;
};

/**
 * Pattern matching cases type
 */
type MatchCases<D extends Record<string, Schema.Struct.Fields>, R> = {
  readonly [K in keyof D & string]: (value: TaggedStructType<K, D[K]>) => R;
};

interface MatchFunction<D extends Record<string, Schema.Struct.Fields>, Brand> {
  <R>(cases: MatchCases<D, R>): (value: VariantsUnion<D> & TaggedSource & Brand) => R;
  <R>(value: VariantsUnion<D> & TaggedSource & Brand, cases: MatchCases<D, R>): R;
}

type RuntimeMatchCases<R> = Record<string, (value: TaggedSource) => R>;

/**
 * Base schema interface with pattern matching helpers
 */
interface MachineSchemaBase<D extends Record<string, Schema.Struct.Fields>, Brand> {
  /**
   * Raw definition record for introspection
   */
  readonly _definition: D;

  /**
   * Per-variant schemas for fine-grained operations
   */
  readonly variants: VariantSchemas<D>;

  /**
   * Type guard: `OrderState.$is("Pending")(value)`
   */
  readonly $is: <Tag extends keyof D & string>(
    tag: Tag,
  ) => (u: unknown) => u is TaggedStructType<Tag, D[Tag]> & Brand;

  /**
   * Pattern matching (curried and uncurried)
   */
  readonly $match: MatchFunction<D, Brand>;

  /**
   * Copy fields from `source` into the same variant, overriding with `partial`.
   * Preserves the specific variant subtype in the return.
   *
   * The partial accepts fields common to all variants, so it works safely
   * when `S` is a generic type parameter (e.g., `<S extends MyState>`).
   *
   * @example
   * ```ts
   * // Per-variant field update — partial accepts that variant's fields
   * const next = MyState.Streaming.with(state, { draft: newDraft })
   *
   * // Cross-variant shared field — works with generic state
   * const updateQueue = <S extends MyState>(state: S, queue: Queue): S =>
   *   MyState.with(state, { queue })
   * ```
   */
  readonly with: <S extends VariantsUnion<D> & Brand>(source: S, partial?: SharedFields<D>) => S;

  /**
   * Reply schemas per variant tag. Only populated for event schemas
   * with variants defined via `Event.reply()`.
   */
  readonly _replySchemas: ReadonlyMap<string, Schema.Decoder<unknown>>;
}

// ============================================================================
// MachineStateSchema Type
// ============================================================================

/**
 * Schema-first state definition that provides:
 * - Schema for encode/decode/validate
 * - Variant constructors: `OrderState.Pending({ orderId: "x" })`
 * - Pattern matching: `$is`, `$match`
 * - Type inference: `typeof OrderState.Type`
 *
 * The D type parameter captures the definition, creating a unique brand
 * per distinct schema definition shape.
 */
export type MachineStateSchema<D extends Record<string, Schema.Struct.Fields>> = Schema.Codec<
  VariantsUnion<D> & FullStateBrand<D>,
  unknown
> &
  MachineSchemaBase<D, FullStateBrand<D>> &
  VariantConstructors<D, FullStateBrand<D>> & {
    /** Schema for persistence, config, and registration. */
    readonly schema: Schema.Schema<VariantsUnion<D> & FullStateBrand<D>>;
  };

/**
 * Schema-first event definition (same structure as state, different brand)
 *
 * The D type parameter captures the definition, creating a unique brand
 * per distinct schema definition shape.
 */
export type MachineEventSchema<D extends Record<string, Schema.Struct.Fields>> = Schema.Codec<
  VariantsUnion<D> & FullEventBrand<D>,
  unknown
> &
  MachineSchemaBase<D, FullEventBrand<D>> &
  VariantConstructors<D, FullEventBrand<D>>;

// ============================================================================
// Implementation
// ============================================================================

/**
 * Build a schema-first definition from a record of tag -> fields
 */
const RESERVED_DERIVE_KEYS = new Set(["_tag"]);

/* eslint-disable anti-slop/no-unsafe-dictionary-type -- isolated erased schema-construction boundary */
type RuntimeWith = (source: TaggedSource, partial?: DynamicFields) => DynamicFields;

type RuntimeConstructor =
  | (((args: DynamicFields) => DynamicFields) & { readonly _tag: string; with: RuntimeWith })
  | (TaggedSource & { with: RuntimeWith });
/* eslint-enable anti-slop/no-unsafe-dictionary-type */

interface BuiltMachineSchema<D extends Record<string, Schema.Struct.Fields>> {
  readonly schema: Schema.Schema<VariantsUnion<D>>;
  readonly variants: VariantSchemas<D>;
  readonly constructors: Record<string, RuntimeConstructor>;
  readonly _definition: D;
  readonly replySchemas: Map<string, Schema.Decoder<unknown>>;
  readonly $is: MachineSchemaBase<D, unknown>["$is"];
  readonly $match: MatchFunction<D, unknown>;
}

const hasTag = (value: unknown): value is TaggedSource =>
  typeof value === "object" && value !== null && "_tag" in value;

const readDynamicField = (source: TaggedSource, key: string) => {
  // SAFETY: schema-derived state values are records whose enumerable payload fields are keyed by strings.
  return (source as DynamicFields)[key];
};

const buildMachineSchema = <D extends Record<string, Schema.Struct.Fields>>(
  definition: D,
): BuiltMachineSchema<D> => {
  // Build variant schemas
  // SAFETY: every key is populated from definition before the schema is exposed.
  const variants = {} as Record<string, Schema.TaggedStruct<string, Schema.Struct.Fields>>;
  const constructors: Record<string, RuntimeConstructor> = {};
  const replySchemas = new Map<string, Schema.Decoder<unknown>>();

  for (const tag of Object.keys(definition)) {
    const fields = definition[tag];
    if (fields === undefined) continue;

    // Detect reply schema before passing to TaggedStruct
    if (ReplySchemaSymbol in fields) {
      // SAFETY: ReplySchemaSymbol membership establishes the hidden decoder metadata property.
      const rs = (fields as Record<symbol, Schema.Decoder<unknown>>)[ReplySchemaSymbol];
      if (rs !== undefined) replySchemas.set(tag, rs);
    }

    const variantSchema = Schema.TaggedStruct(tag, fields);
    variants[tag] = variantSchema;

    // Create constructor that builds tagged struct directly
    // Like Data.taggedEnum, this doesn't validate at construction time
    // Use Schema.decode for validation when needed
    const fieldNames = new Set(Object.keys(fields));
    const hasFields = fieldNames.size > 0;

    if (hasFields) {
      // Non-empty: constructor function requiring args
      const constructor = (args: DynamicFields) => ({ ...args, _tag: tag });
      constructor._tag = tag;
      constructor.with = (source: TaggedSource, partial?: DynamicFields) => {
        // eslint-disable-next-line anti-slop/no-known-value-widening -- payload fields are selected dynamically from schema keys
        const result: DynamicFields = { _tag: tag };
        for (const key of fieldNames) {
          if (key in source) result[key] = readDynamicField(source, key);
        }
        if (partial !== undefined) {
          for (const [key, value] of Object.entries(partial)) {
            if (RESERVED_DERIVE_KEYS.has(key)) continue;
            if (!fieldNames.has(key)) continue;
            result[key] = value;
          }
        }
        return result;
      };
      constructors[tag] = constructor;
    } else {
      // Empty: plain value, not callable
      // SAFETY: empty variants require no payload and expose only the common runtime with contract.
      constructors[tag] = { _tag: tag, with: () => ({ _tag: tag }) } as never;
    }
  }

  // Build union schema from all variants
  const variantArray = Object.values(variants);
  if (variantArray.length === 0) {
    throw new InvalidSchemaError({ message: "Schema must have at least one variant" });
  }

  // Schema.Union requires at least 2 members, handle single variant case
  const unionSchema =
    variantArray.length === 1
      ? // eslint-disable-next-line @typescript-eslint/no-non-null-assertion -- checked length above
        variantArray[0]!
      : // SAFETY: the length check establishes the non-empty tuple required by Schema.Union.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any -- dynamic schema union
        Schema.Union(variantArray as any);

  // Type guard
  const $is =
    <Tag extends keyof D & string>(tag: Tag) =>
    (value: unknown): value is TaggedStructType<Tag, D[Tag]> =>
      hasTag(value) && value._tag === tag;

  // Pattern matching
  function $match<R>(cases: MatchCases<D, R>): (value: VariantsUnion<D> & TaggedSource) => R;
  function $match<R>(value: VariantsUnion<D> & TaggedSource, cases: MatchCases<D, R>): R;
  function $match<R>(
    valueOrCases: (VariantsUnion<D> & TaggedSource) | MatchCases<D, R>,
    maybeCases?: MatchCases<D, R>,
  ): R | ((value: VariantsUnion<D> & TaggedSource) => R) {
    if (maybeCases !== undefined) {
      // Uncurried: $match(value, cases)
      // SAFETY: the two-argument overload requires a tagged value as its first argument.
      const value = valueOrCases as TaggedSource;
      // SAFETY: every handler receives a variant selected by that value's matching tag.
      const cases = maybeCases as RuntimeMatchCases<R>;
      const handler = cases[value._tag];
      if (handler === undefined) {
        throw new MissingMatchHandlerError({ tag: value._tag });
      }
      return handler(value);
    }
    // Curried: $match(cases) -> (value) => result
    // SAFETY: the one-argument overload requires the complete case map.
    const cases = valueOrCases as RuntimeMatchCases<R>;
    return (value: VariantsUnion<D> & TaggedSource): R => {
      const handler = cases[value._tag];
      if (handler === undefined) {
        throw new MissingMatchHandlerError({ tag: value._tag });
      }
      return handler(value);
    };
  }

  // SAFETY: the union and variants are assembled exclusively from the same definition D.
  return {
    // eslint-disable-next-line anti-slop/no-chained-type-assertions -- Effect's dynamic union loses D's static relationship
    schema: unionSchema as unknown as Schema.Schema<VariantsUnion<D>>,
    // eslint-disable-next-line anti-slop/no-known-value-widening -- keys were populated from definition D above
    variants: variants as VariantSchemas<D>,
    constructors,
    _definition: definition,
    replySchemas,
    $is,
    $match,
  };
};

/**
 * Internal helper to create a machine schema (shared by State and Event).
 * Builds the schema object with variants, constructors, $is, and $match.
 */
const createMachineSchema = <D extends Record<string, Schema.Struct.Fields>>(definition: D) => {
  const { schema, variants, constructors, _definition, replySchemas, $is, $match } =
    buildMachineSchema(definition);
  // Union-level with: dispatch to per-variant with based on _tag
  const withFn = (source: TaggedSource, partial?: DynamicFields) => {
    const ctor = constructors[source._tag];
    if (ctor === undefined) {
      throw new MissingMatchHandlerError({ tag: source._tag });
    }
    return ctor.with(source, partial);
  };

  return Object.assign(Object.create(schema), {
    variants,
    _definition,
    _replySchemas: replySchemas,
    schema,
    $is,
    $match,
    with: withFn,
    ...constructors,
  });
};

/**
 * Create a schema-first State definition.
 *
 * The schema's definition type D creates a unique brand, preventing
 * accidental use of constructors from different state schemas
 * (unless they have identical definitions).
 *
 * @example
 * ```ts
 * const OrderState = MachineSchema.State({
 *   Pending: { orderId: Schema.String },
 *   Shipped: { trackingId: Schema.String },
 * })
 *
 * type OrderState = typeof OrderState.Type
 *
 * // Construct
 * const s = OrderState.Pending({ orderId: "123" })
 *
 * // Pattern match
 * OrderState.$match(s, {
 *   Pending: (v) => v.orderId,
 *   Shipped: (v) => v.trackingId,
 * })
 *
 * // Validate
 * Schema.decodeUnknownSync(OrderState)(rawJson)
 * ```
 */
export const State = <const D extends Record<string, Schema.Struct.Fields>>(
  definition: D,
): MachineStateSchema<D> => {
  // SAFETY: createMachineSchema builds every constructor and schema member from definition D.
  return createMachineSchema(definition) as MachineStateSchema<D>;
};

/**
 * Create a schema-first Event definition.
 *
 * The schema's definition type D creates a unique brand, preventing
 * accidental use of constructors from different event schemas
 * (unless they have identical definitions).
 *
 * Use `Event.reply(fields, replySchema)` to define events that support
 * typed `ask()` replies.
 *
 * @example
 * ```ts
 * const OrderEvent = Event({
 *   Ship: { trackingId: Schema.String },
 *   Cancel: {},
 *   GetTotal: Event.reply({}, Schema.Number),
 * })
 *
 * type OrderEvent = typeof OrderEvent.Type
 *
 * // Construct
 * const e = OrderEvent.Ship({ trackingId: "abc" })
 *
 * // Typed ask
 * const total = yield* actor.ask(OrderEvent.GetTotal) // number
 * ```
 */
const EventImpl = <const D extends Record<string, Schema.Struct.Fields>>(
  definition: D,
): MachineEventSchema<D> => {
  // SAFETY: createMachineSchema builds every constructor and schema member from definition D.
  return createMachineSchema(definition) as MachineEventSchema<D>;
};

/**
 * Annotate event fields with a reply schema.
 * Events defined with `Event.reply(fields, replySchema)` enable typed `ask()`.
 */
const replyFieldsFn = <F extends Schema.Struct.Fields, RS extends Schema.Schema<unknown>>(
  fields: F,
  replySchema: RS,
): ReplyFields<F, RS> => {
  // SAFETY: Object.defineProperty below installs the non-enumerable metadata property on this copy.
  const annotated = { ...fields } as ReplyFields<F, RS>;
  Object.defineProperty(annotated, ReplySchemaSymbol, {
    value: replySchema,
    enumerable: false,
    writable: false,
  });
  return annotated;
};

export const Event = Object.assign(EventImpl, { reply: replyFieldsFn });
