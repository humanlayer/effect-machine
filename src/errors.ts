/**
 * Typed error classes for effect-machine.
 *
 * All errors extend Schema.TaggedError for:
 * - Type-safe catching via Effect.catchTag
 * - Serialization support
 * - Composable error handling
 *
 * @module
 */
import { Schema } from "effect";

/** Attempted to spawn/restore actor with ID already in use */
export class DuplicateActorError extends Schema.TaggedError<DuplicateActorError>()(
  "DuplicateActorError",
  { actorId: Schema.String },
) {}

/** Operation requires schemas attached to machine */
export class MissingSchemaError extends Schema.TaggedError<MissingSchemaError>()(
  "MissingSchemaError",
  { operation: Schema.String },
) {}

/** State/Event schema has no variants */
export class InvalidSchemaError extends Schema.TaggedError<InvalidSchemaError>()(
  "InvalidSchemaError",
  { message: Schema.String },
) {}

/** $match called with missing handler for tag */
export class MissingMatchHandlerError extends Schema.TaggedError<MissingMatchHandlerError>()(
  "MissingMatchHandlerError",
  { tag: Schema.String },
) {}

/** Slot handler not found at runtime (internal error) */
export class SlotProvisionError extends Schema.TaggedError<SlotProvisionError>()(
  "SlotProvisionError",
  {
    slotName: Schema.String,
    slotType: Schema.Literal("slot"),
  },
) {}

/** Slot provision validation failed — missing or extra handlers */
export class ProvisionValidationError extends Schema.TaggedError<ProvisionValidationError>()(
  "ProvisionValidationError",
  {
    missing: Schema.Array(Schema.String),
    extra: Schema.Array(Schema.String),
  },
) {}

/** Assertion failed in testing utilities */
export class AssertionError extends Schema.TaggedError<AssertionError>()("AssertionError", {
  message: Schema.String,
}) {}

/** Actor was stopped while a call/ask was pending */
export class ActorStoppedError extends Schema.TaggedError<ActorStoppedError>()(
  "ActorStoppedError",
  { actorId: Schema.String },
) {}

/** ask() was used but the transition handler did not call reply */
export class NoReplyError extends Schema.TaggedError<NoReplyError>()("NoReplyError", {
  actorId: Schema.String,
  eventTag: Schema.String,
}) {}

/** Persistence adapter operation failed */
export class PersistenceError extends Schema.TaggedError<PersistenceError>()("PersistenceError", {
  message: Schema.String,
}) {}

/** Slot input/output schema validation failed */
export class SlotCodecError extends Schema.TaggedError<SlotCodecError>()("SlotCodecError", {
  slotName: Schema.String,
  phase: Schema.Literals(["input", "output"]),
  message: Schema.String,
}) {}

/** Optimistic locking failure — stored version doesn't match expected */
export class VersionConflictError extends Schema.TaggedError<VersionConflictError>()(
  "VersionConflictError",
  { expected: Schema.Number, actual: Schema.Number },
) {}
