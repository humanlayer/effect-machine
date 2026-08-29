// eslint-disable-next-line eslint-plugin-import/namespace -- false positive: Brand is a type namespace in effect
import type { Brand } from "effect";

// String-based type IDs for branding (v4 Brand requires string keys)
export type StateTypeId = "@humanlayer/effect-machine/StateTypeId";
export type EventTypeId = "@humanlayer/effect-machine/EventTypeId";

// Brand interfaces
export interface StateBrand extends Brand.Brand<StateTypeId> {}
export interface EventBrand extends Brand.Brand<EventTypeId> {}

// Shared branded type constraints used across all combinators
export type BrandedState = { readonly _tag: string } & StateBrand;
export type BrandedEvent = { readonly _tag: string } & EventBrand;

// String-based schema branding (ties brand to specific schema definition)
type SchemaIdTypeId = "@humanlayer/effect-machine/SchemaIdTypeId";

/**
 * Brand that captures the schema definition type D.
 * Two schemas with identical definition shapes will have compatible brands.
 * Different definitions = incompatible brands.
 */
export interface SchemaIdBrand<
  _D extends Record<string, unknown>,
> extends Brand.Brand<SchemaIdTypeId> {}

/**
 * Full state brand: combines base state brand with schema-specific brand
 */
export type FullStateBrand<D extends Record<string, unknown>> = StateBrand & SchemaIdBrand<D>;

/**
 * Full event brand: combines base event brand with schema-specific brand
 */
export type FullEventBrand<D extends Record<string, unknown>> = EventBrand & SchemaIdBrand<D>;

/**
 * Brand that carries the reply type for an event variant.
 * Present only on events defined with Event.reply().
 */
export type ReplyTypeId = "@humanlayer/effect-machine/ReplyTypeId";
export interface ReplyTypeBrand<R> extends Brand.Brand<ReplyTypeId> {
  readonly _ReplyType: R;
}

/**
 * Extract the reply type from a branded event value.
 * Returns `never` if the event has no reply schema.
 */
export type ExtractReply<E> = E extends ReplyTypeBrand<infer R> ? R : never;

/**
 * Value or constructor for a tagged type.
 * Accepts both plain values (empty structs) and constructor functions (non-empty structs).
 */
export type TaggedOrConstructor<T extends { readonly _tag: string }> =
  | T
  | ((...args: never[]) => T);
