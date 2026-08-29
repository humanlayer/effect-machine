/**
 * Internal utilities for effect-machine.
 * @internal
 */
import { Effect, Stream } from "effect";
import type { ActorSystemService } from "../actor.js";

// ============================================================================
// Type Helpers
// ============================================================================

/**
 * Extracts _tag from a tagged union member
 */
export type TagOf<T> = T extends { readonly _tag: infer Tag } ? Tag : never;

/**
 * Extracts args type from a Data.taggedEnum constructor
 */
export type ArgsOf<C> = C extends (args: infer A) => unknown ? A : never;

/**
 * Extracts return type from a Data.taggedEnum constructor
 * @internal
 */
export type InstanceOf<C> = C extends (...args: unknown[]) => infer R ? R : never;

/**
 * A tagged union constructor (from Data.taggedEnum)
 */
export type TaggedConstructor<T extends { readonly _tag: string }> = (args: Omit<T, "_tag">) => T;

// ============================================================================
// Reply Result (branded replacement for duck-typed { state, reply })
// ============================================================================

const ReplyResultSymbol: unique symbol = Symbol.for("@humanlayer/effect-machine/ReplyResult");
export type ReplyResultSymbol = typeof ReplyResultSymbol;

/**
 * Branded reply result from a transition handler.
 * Created via `Machine.reply(state, value)`.
 */
export interface ReplyResult<State, Reply> {
  readonly state: State;
  readonly reply: Reply;
  readonly [ReplyResultSymbol]: true;
}

/**
 * Create a reply result for ask-bearing event handlers.
 */
export const makeReply = <State, Reply>(state: State, reply: Reply): ReplyResult<State, Reply> => ({
  state,
  reply,
  [ReplyResultSymbol]: true as const,
});

/**
 * Type guard for ReplyResult (symbol-based, replaces duck-typing).
 */
export const isReplyResult = (value: unknown): value is ReplyResult<unknown, unknown> =>
  value !== null && typeof value === "object" && ReplyResultSymbol in value;

// ============================================================================
// DeferReplyResult — signal that spawn handler will reply later
// ============================================================================

const DeferReplySymbol: unique symbol = Symbol.for("@humanlayer/effect-machine/DeferReply");
export type DeferReplySymbol = typeof DeferReplySymbol;

/**
 * Branded deferred reply result from a transition handler.
 * Signals that the reply will be settled later by `self.reply()` in a spawn handler.
 * Created via `Machine.deferReply(state)`.
 */
export interface DeferReplyResult<State> {
  readonly state: State;
  readonly [DeferReplySymbol]: true;
}

/**
 * Create a deferred reply result. Handler returns this to signal
 * "spawn handler will call self.reply(value) later".
 */
export const makeDeferReply = <State>(state: State): DeferReplyResult<State> => ({
  state,
  [DeferReplySymbol]: true as const,
});

/**
 * Type guard for DeferReplyResult.
 */
export const isDeferReplyResult = (value: unknown): value is DeferReplyResult<unknown> =>
  value !== null && typeof value === "object" && DeferReplySymbol in value;

/**
 * Transition handler result.
 * - When Reply is `never`: handler returns plain State (no reply allowed)
 * - When Reply is concrete: handler must return ReplyResult via Machine.reply()
 */
export type TransitionResult<State, R, Reply = never> = [Reply] extends [never]
  ? State | Effect.Effect<State, never, R>
  :
      | ReplyResult<State, Reply>
      | DeferReplyResult<State>
      | Effect.Effect<ReplyResult<State, Reply> | DeferReplyResult<State>, never, R>;

// ============================================================================
// Constants
// ============================================================================

/**
 * Internal event tags used for lifecycle effect contexts.
 * Prefixed with $ to distinguish from user events.
 * @internal
 */
export const INTERNAL_INIT_EVENT = "$init" as const;
export const INTERNAL_ENTER_EVENT = "$enter" as const;

// ============================================================================
// Runtime Utilities
// ============================================================================

/**
 * Extract _tag from a tagged value or constructor.
 *
 * Supports:
 * - Plain values with `_tag` (MachineSchema empty structs)
 * - Constructors with static `_tag` (MachineSchema non-empty structs)
 * - Data.taggedEnum constructors (fallback via instantiation)
 */
export const getTag = (
  constructorOrValue: { _tag: string } | ((...args: never[]) => { _tag: string }),
): string => {
  // Direct _tag property (values or static on constructors)
  if ("_tag" in constructorOrValue) {
    return constructorOrValue._tag;
  }
  // Fallback: instantiate (Data.taggedEnum compatibility)
  // Try zero-arg first, then empty object for record constructors
  try {
    // The _tag check leaves only the callable constructor branch.
    return constructorOrValue()._tag;
  } catch {
    // SAFETY: Data tagged record constructors accept an empty record when zero-argument invocation fails.
    return (constructorOrValue as (args: Record<string, never>) => { _tag: string })({})._tag;
  }
};

/** Check if a value is an Effect */
export const isEffect: (value: unknown) => value is Effect.Effect<unknown, unknown, unknown> =
  Effect.isEffect;

// ============================================================================
// Stub System
// ============================================================================

/**
 * Stub ActorSystemService that dies on any method call.
 * Used in contexts where spawning/system access isn't supported
 * (testing simulation, persistent actor replay).
 * @internal
 */
export const stubSystem: ActorSystemService = {
  spawn: () => Effect.die("spawn not supported in stub system"),
  get: () => Effect.die("get not supported in stub system"),
  stop: () => Effect.die("stop not supported in stub system"),
  events: Stream.empty,
  get actors() {
    return new Map();
  },
  subscribe: () => () => {},
};
