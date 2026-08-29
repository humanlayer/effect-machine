/**
 * Supervision types for actor lifecycle management.
 *
 * Core concepts:
 * - `ActorExit<S>` — why an actor stopped (final, explicit stop, or defect)
 * - `DefectPhase` — where in the lifecycle a defect occurred
 * - `Supervision.Policy` — Schedule-based restart policy
 * - `CellPhase<S>` — internal phase machine for serializing stop/restart/drain
 *
 * @module
 */
import { type Cause, type Duration, Schedule } from "effect";

// ============================================================================
// ActorExit — why an actor stopped
// ============================================================================

/**
 * Where in the actor lifecycle a defect occurred.
 *
 * - `transition` — during event handler execution
 * - `spawn` — during state spawn effect execution
 * - `background` — in a background effect fiber
 * - `initial-spawn` — during initial state spawn effects (before event loop)
 */
export type DefectPhase = "transition" | "spawn" | "background" | "initial-spawn";

/**
 * Terminal exit reason for an actor generation.
 *
 * - `Final` — machine reached a final state normally
 * - `Stopped` — explicit `actor.stop` or `actor.drain`
 * - `Defect` — unhandled error in the runtime
 */
export type ActorExit<S> =
  | { readonly _tag: "Final"; readonly state: S }
  | { readonly _tag: "Stopped" }
  | { readonly _tag: "Defect"; readonly cause: Cause.Cause<unknown>; readonly phase: DefectPhase };

/** Constructors for ActorExit */
export const ActorExit = {
  Final: <S>(state: S): ActorExit<S> => ({ _tag: "Final", state }),
  Stopped: { _tag: "Stopped" } as ActorExit<never>,
  Defect: <S = never>(cause: Cause.Cause<unknown>, phase: DefectPhase): ActorExit<S> => ({
    _tag: "Defect",
    cause,
    phase,
  }),
} as const;

// ============================================================================
// CellPhase — internal phase machine for supervised actors
// ============================================================================

/**
 * Phase state for supervised actors. Serializes concurrent stop/restart/drain.
 *
 * Transitions:
 * - `Running` → crash → `Restarting` → new runtime → `Running`
 * - `Running` → explicit stop/drain → `Stopping` → `Terminated`
 * - `Restarting` → explicit stop → `Stopping` → `Terminated`
 *
 * @internal
 */
export type CellPhase<S> =
  | { readonly _tag: "Running"; readonly generation: number }
  | { readonly _tag: "Restarting"; readonly generation: number }
  | { readonly _tag: "Stopping" }
  | { readonly _tag: "Terminated"; readonly exit: ActorExit<S> };

// ============================================================================
// Supervision Policy
// ============================================================================

export namespace Supervision {
  /**
   * Supervision policy for actor restart behavior.
   *
   * `schedule` controls restart timing and budget — schedule exhaustion means terminal stop.
   * `shouldRestart` optionally classifies defects — return `false` to stop immediately
   * without consuming the schedule.
   */
  export interface Policy {
    /** Schedule that controls restart timing. Exhaustion = terminal stop. */
    readonly schedule: Schedule.Schedule<unknown>;
    /**
     * Optional classifier: given a defect exit, decide whether to restart or stop immediately.
     * Default: always restart (let schedule handle budget).
     */
    readonly shouldRestart?: (
      exit: Extract<ActorExit<unknown>, { readonly _tag: "Defect" }>,
    ) => boolean;
  }

  /** No supervision — crashes are terminal. */
  export const none: Policy = {
    schedule: Schedule.recurs(0),
  };

  /**
   * Restart on defect with max restarts within a window, optional backoff.
   *
   * @example
   * ```ts
   * Supervision.restart() // unlimited restarts, no backoff
   * Supervision.restart({ maxRestarts: 3 }) // 3 restarts then terminal
   * Supervision.restart({ maxRestarts: 3, within: "1 minute" }) // 3 within 1 min
   * Supervision.restart({ backoff: Schedule.exponential("100 millis") })
   * ```
   */
  export const restart = (options?: {
    readonly maxRestarts?: number;
    readonly within?: Duration.Input;
    readonly backoff?: Schedule.Schedule<unknown>;
  }): Policy => {
    let schedule: Schedule.Schedule<unknown> = Schedule.forever;

    if (options?.maxRestarts !== undefined) {
      const recurs = Schedule.recurs(options.maxRestarts);
      if (options.within !== undefined) {
        // Reset the counter within the time window
        schedule = Schedule.max([recurs, Schedule.windowed(options.within)]);
      } else {
        schedule = recurs;
      }
    }

    if (options?.backoff !== undefined) {
      schedule = Schedule.max([schedule, options.backoff]);
    }

    return { schedule };
  };
}
