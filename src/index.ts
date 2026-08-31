// Machine namespace (Effect-style)
export * as Machine from "./machine.js";

// Errors
export {
  ActorStoppedError,
  AssertionError,
  DuplicateActorError,
  InvalidSchemaError,
  MissingMatchHandlerError,
  MissingSchemaError,
  NoReplyError,
  PersistenceError,
  VersionConflictError,
} from "./errors.js";

// Schema-first State/Event definitions
export { State, Event } from "./schema.js";
export type {
  MachineStateSchema,
  MachineEventSchema,
  ReplyFields,
  ReplyVariant,
} from "./schema.js";

// Core machine types (for advanced use)
export type {
  Machine as MachineType,
  MachineRef,
  MakeConfig,
  Transition,
  SpawnEffect,
  BackgroundEffect,
  HandlerContext,
  StateHandlerContext,
  TaskOptions,
  TimeoutConfig,
  ReplyResult,
  DeferReplyResult,
  Recovery,
  RecoveryContext,
  Durability,
  DurabilityCommit,
  Lifecycle,
  LifecycleEvent,
} from "./machine.js";

// Actor types and system
export type {
  ActorRef,
  ActorRefSync,
  ActorHandle,
  ActorSystemService as ActorSystem,
  ProcessEventResult,
  SystemEvent,
  SystemEventListener,
  TransitionInfo,
} from "./actor.js";
export {
  ActorSystem as ActorSystemService,
  ActorScope,
  Default as ActorSystemDefault,
} from "./actor.js";

// Supervision
export { ActorExit, Supervision } from "./supervision.js";
export type { DefectPhase, CellPhase } from "./supervision.js";

// Testing utilities
export {
  assertNeverReaches,
  assertPath,
  assertReaches,
  createTestHarness,
  simulate,
} from "./testing.js";
export type { SimulationResult, TestHarness, TestHarnessOptions } from "./testing.js";

// Inspection
export type {
  AnyInspectionEvent,
  EffectEvent,
  ErrorEvent,
  EventReceivedEvent,
  InspectionEvent,
  InspectorService as Inspector,
  InspectorHandler,
  SpawnEvent,
  StopEvent,
  TaskEvent,
  TracingInspectorOptions,
  TransitionEvent,
} from "./inspection.js";
export {
  combineInspectors,
  collectingInspector,
  consoleInspector,
  Inspector as InspectorService,
  makeInspector,
  makeInspectorEffect,
  tracingInspector,
} from "./inspection.js";
