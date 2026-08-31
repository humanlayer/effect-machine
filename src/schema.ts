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
// Reply metadata
// ============================================================================

/**
 * Explicit event-variant metadata created by `Event.reply`.
 */
export interface ReplyVariant<
  F extends Schema.Struct.Fields,
  RS extends Schema.Codec<unknown, unknown>,
> {
  readonly _kind: "ReplyVariant";
  readonly fields: F;
  readonly replySchema: RS;
}

/** @deprecated Use `ReplyVariant`. Retained as a source-compatible type alias. */
export type ReplyFields<
  F extends Schema.Struct.Fields,
  RS extends Schema.Codec<unknown, unknown>,
> = ReplyVariant<F, RS>;

type VariantDefinition =
  | Schema.Struct.Fields
  | ReplyVariant<Schema.Struct.Fields, Schema.Codec<unknown, unknown>>;

export type MachineSchemaDefinition = Record<string, VariantDefinition>;

type FieldsOf<Definition> =
  Definition extends ReplyVariant<infer F, Schema.Codec<unknown, unknown>>
    ? F
    : Definition extends Schema.Struct.Fields
      ? Definition
      : never;

type TaggedSource = { readonly _tag: string };

// eslint-disable-next-line anti-slop/no-unsafe-dictionary-type -- dynamic schema construction is isolated behind this alias
type DynamicFields = Record<string, unknown>;

// ============================================================================
// Type Helpers
// ============================================================================

/**
 * Extract the TypeScript type from a TaggedStruct schema
 */
type TaggedStructType<Tag extends string, Definition> = Schema.Schema.Type<
  Schema.TaggedStruct<Tag, FieldsOf<Definition>>
>;

/**
 * Build variant schemas type from definition
 */
type VariantSchemas<D extends MachineSchemaDefinition> = {
  readonly [K in keyof D & string]: Schema.TaggedStruct<K, FieldsOf<D[K]>>;
};

/**
 * Build union type from variant schemas.
 * Reply-bearing variants carry ReplyTypeBrand<R> for ask() inference.
 */
export type VariantsUnion<D extends MachineSchemaDefinition> = {
  [K in keyof D & string]: TaggedStructType<K, D[K]> &
    (D[K] extends ReplyVariant<Schema.Struct.Fields, infer RS>
      ? ReplyTypeBrand<Schema.Schema.Type<RS>>
      : unknown);
}[keyof D & string] &
  TaggedSource;

/**
 * Check if fields are empty (no required string properties).
 * Symbol keys (like ReplySchemaSymbol) are metadata, not payload fields.
 */
type IsEmptyFields<Definition> = string & keyof FieldsOf<Definition> extends never ? true : false;

/**
 * Resolve the reply brand for a variant's fields.
 * If fields carry ReplySchemaSymbol, adds ReplyTypeBrand<R>.
 */
type VariantReplyBrand<Definition> =
  Definition extends ReplyVariant<Schema.Struct.Fields, infer RS>
    ? ReplyTypeBrand<Schema.Schema.Type<RS>>
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
type VariantConstructors<D extends MachineSchemaDefinition, Brand> = {
  readonly [K in keyof D & string]: IsEmptyFields<D[K]> extends true
    ? TaggedStructType<K, D[K]> &
        Brand &
        VariantReplyBrand<D[K]> & {
          readonly with: (source: TaggedSource) => TaggedStructType<K, D[K]> & Brand;
        }
    : ((
        args: Schema.Struct.Type<FieldsOf<D[K]>>,
      ) => TaggedStructType<K, D[K]> & Brand & VariantReplyBrand<D[K]>) & {
        readonly with: (
          source: TaggedSource,
          partial?: Partial<Schema.Struct.Type<FieldsOf<D[K]>>>,
        ) => TaggedStructType<K, D[K]> & Brand;
        readonly _tag: K;
      };
};

/**
 * Keys present in ALL variants (intersection of field names).
 * Used by union-level `with` to accept only fields safe to update
 * regardless of which variant the source is.
 */
type SharedKeys<D extends MachineSchemaDefinition> = keyof FieldsOf<D[keyof D & string]> & string;

type SharedFields<D extends MachineSchemaDefinition> = {
  readonly [K in SharedKeys<D>]?: FieldsOf<D[keyof D & string]>[K] extends Schema.Top
    ? Schema.Schema.Type<FieldsOf<D[keyof D & string]>[K]>
    : never;
};

/**
 * Pattern matching cases type
 */
type MatchCases<D extends MachineSchemaDefinition, R> = {
  readonly [K in keyof D & string]: (value: TaggedStructType<K, D[K]>) => R;
};

interface MatchFunction<D extends MachineSchemaDefinition, Brand> {
  <R>(cases: MatchCases<D, R>): (value: VariantsUnion<D> & TaggedSource & Brand) => R;
  <R>(value: VariantsUnion<D> & TaggedSource & Brand, cases: MatchCases<D, R>): R;
}

/**
 * Base schema interface with pattern matching helpers
 */
interface MachineSchemaBase<D extends MachineSchemaDefinition, Brand> {
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
  readonly _replySchemas: ReadonlyMap<string, Schema.Codec<unknown, unknown>>;
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
export type MachineEventSchema<D extends MachineSchemaDefinition> = Schema.Codec<
  VariantsUnion<D> & FullEventBrand<D>,
  unknown
> &
  MachineSchemaBase<D, FullEventBrand<D>> &
  VariantConstructors<D, FullEventBrand<D>> & {
    /** Schema for persistence, config, and registration. */
    readonly schema: Schema.Schema<VariantsUnion<D> & FullEventBrand<D>>;
  };

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

type RuntimeMatchCases<R> = Record<string, (value: TaggedSource) => R>;

type MachineSchemaOwner<D extends MachineSchemaDefinition, Brand> = Schema.Codec<
  VariantsUnion<D> & Brand,
  unknown
> &
  MachineSchemaBase<D, Brand> &
  VariantConstructors<D, Brand> & {
    readonly schema: Schema.Schema<VariantsUnion<D> & Brand>;
  };

const hasTag = (value: unknown): value is TaggedSource =>
  typeof value === "object" && value !== null && "_tag" in value;

const readDynamicField = (source: TaggedSource, key: string) =>
  // SAFETY: schema-derived state values are records whose enumerable payload fields are keyed by strings.
  (source as DynamicFields)[key];

const isReplyVariant = (
  definition: VariantDefinition,
): definition is ReplyVariant<Schema.Struct.Fields, Schema.Codec<unknown, unknown>> =>
  "_kind" in definition && definition._kind === "ReplyVariant";

const invokeMatch = <R>(value: TaggedSource, cases: RuntimeMatchCases<R>): R => {
  const handler = cases[value._tag];
  if (handler === undefined) {
    throw new MissingMatchHandlerError({ tag: value._tag });
  }
  return handler(value);
};

const buildMachineSchema = <D extends MachineSchemaDefinition, Brand>(
  definition: D,
): MachineSchemaOwner<D, Brand> => {
  const tags = Object.keys(definition);
  if (tags.length === 0) {
    throw new InvalidSchemaError({ message: "Schema must have at least one variant" });
  }

  const fieldsByTag: Record<string, Schema.Struct.Fields> = {};
  const constructors: Record<string, RuntimeConstructor> = {};
  const replySchemas = new Map<string, Schema.Codec<unknown, unknown>>();

  for (const tag of tags) {
    const variantDefinition = definition[tag];
    if (variantDefinition === undefined) continue;

    const fields = isReplyVariant(variantDefinition) ? variantDefinition.fields : variantDefinition;
    fieldsByTag[tag] = fields;
    if (isReplyVariant(variantDefinition)) {
      replySchemas.set(tag, variantDefinition.replySchema);
    }

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
      // Empty variants use the tagged-value arm of RuntimeConstructor.
      constructors[tag] = { _tag: tag, with: () => ({ _tag: tag }) };
    }
  }

  const tagged = Schema.TaggedUnion(fieldsByTag);

  // Type guard
  const $is =
    <Tag extends keyof D & string>(tag: Tag) =>
    (value: unknown): value is TaggedStructType<Tag, D[Tag]> =>
      hasTag(value) && value._tag === tag;

  const $match = <R>(
    ...args:
      | readonly [cases: RuntimeMatchCases<R>]
      | readonly [value: TaggedSource, cases: RuntimeMatchCases<R>]
  ): R | ((value: TaggedSource) => R) => {
    if (args.length === 2) {
      return invokeMatch(args[0], args[1]);
    }
    const cases = args[0];
    return (value: TaggedSource) => invokeMatch(value, cases);
  };

  const schema = Schema.make<Schema.Codec<VariantsUnion<D> & Brand, unknown>>(tagged.ast);

  const withFn = (source: TaggedSource, partial?: DynamicFields) => {
    const ctor = constructors[source._tag];
    if (ctor === undefined) {
      throw new MissingMatchHandlerError({ tag: source._tag });
    }
    return ctor.with(source, partial);
  };

  Object.assign(
    schema,
    {
      variants: tagged.cases,
      _definition: definition,
      _replySchemas: replySchemas,
      schema,
      $is,
      $match,
      with: withFn,
    },
    constructors,
  );

  const complete = tags.every(
    (tag) => Object.hasOwn(tagged.cases, tag) && Object.hasOwn(constructors, tag),
  );
  if (!complete) {
    throw new InvalidSchemaError({ message: "Schema owner construction was incomplete" });
  }

  // SAFETY: every definition tag is runtime-checked above to own both its schema case and
  // constructor; all helpers close over those same records, and brands are type-only.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion
  return schema as MachineSchemaOwner<D, Brand>;
};

/**
 * Internal helper to create a machine schema (shared by State and Event).
 */
const createMachineSchema = <D extends MachineSchemaDefinition, Brand>(definition: D) =>
  buildMachineSchema<D, Brand>(definition);

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
): MachineStateSchema<D> => createMachineSchema<D, FullStateBrand<D>>(definition);

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
const EventImpl = <const D extends MachineSchemaDefinition>(definition: D): MachineEventSchema<D> =>
  createMachineSchema<D, FullEventBrand<D>>(definition);

/**
 * Annotate event fields with a reply schema.
 * Events defined with `Event.reply(fields, replySchema)` enable typed `ask()`.
 */
const replyFieldsFn = <F extends Schema.Struct.Fields, RS extends Schema.Codec<unknown, unknown>>(
  fields: F,
  replySchema: RS,
): ReplyVariant<F, RS> => ({ _kind: "ReplyVariant", fields, replySchema });

export const Event = Object.assign(EventImpl, { reply: replyFieldsFn });
