/**
 * EntityActorRef — typed client wrapper for remote entity machines.
 *
 * Provides an ActorRef-like API over the cluster RPC protocol:
 * - send (fire-and-forget, returns new state)
 * - ask (typed domain reply)
 * - snapshot (current state)
 * - watch (streaming state observation)
 * - waitFor (wait for specific state)
 *
 * @module
 */
import type { RpcClient } from "effect/unstable/rpc";
import { Effect, Option, Stream } from "effect";

import type { ExtractReply, ReplyTypeBrand } from "../internal/brands.js";
import type { NoReplyError } from "../errors.js";
import { ActorStoppedError } from "../errors.js";
import type { EntityRpcs } from "./to-entity.js";

/**
 * Typed client wrapper for remote entity machines.
 *
 * Unlike local `ActorRef`, this communicates over cluster RPCs.
 * Only operations that make sense over the network are exposed.
 *
 * @example
 * ```ts
 * const ref = makeEntityActorRef(client, "order-123")
 * yield* ref.send(OrderEvent.Ship({ trackingId: "abc" }))
 * const state = yield* ref.snapshot
 * yield* ref.waitFor((s) => s._tag === "Shipped")
 * ```
 */
export interface EntityActorRef<
  State extends { readonly _tag: string },
  Event extends { readonly _tag: string },
> {
  readonly entityId: string;

  /** Send event. Returns new state after processing. */
  readonly send: (event: Event) => Effect.Effect<State>;

  /** Send event and get typed domain reply (via Event.reply() schema). */
  readonly ask: <E extends Event & ReplyTypeBrand<unknown>>(
    event: E,
  ) => Effect.Effect<ExtractReply<E>, NoReplyError>;

  /** Get current state. */
  readonly snapshot: Effect.Effect<State>;

  /** Stream of state changes (via WatchState streaming RPC). */
  readonly watch: Stream.Stream<State>;

  /** Wait for a state matching the predicate. Snapshots first, then watches stream. */
  readonly waitFor: (
    predicate: (state: State) => boolean,
  ) => Effect.Effect<State, ActorStoppedError>;
}

interface TypedEntityClient<
  State extends { readonly _tag: string },
  Event extends { readonly _tag: string },
> {
  readonly Send: (request: { readonly event: Event }) => Effect.Effect<State>;
  readonly Ask: <E extends Event & ReplyTypeBrand<unknown>>(request: {
    readonly event: E;
  }) => Effect.Effect<ExtractReply<E>, NoReplyError>;
  readonly GetState: () => Effect.Effect<State>;
  readonly WatchState: () => Stream.Stream<State>;
}

/**
 * Create an EntityActorRef from a RPC client.
 *
 * @example
 * ```ts
 * const makeClient = yield* Entity.makeTestClient(entity, entityLayer)
 * const client = yield* makeClient("order-123")
 * const ref = makeEntityActorRef(client, "order-123")
 * yield* ref.send(OrderEvent.Process)
 * ```
 */
export const makeEntityActorRef = <
  State extends { readonly _tag: string },
  Event extends { readonly _tag: string },
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Schema types need wide acceptance
  Rpcs extends EntityRpcs<any, any>[number],
>(
  client: RpcClient.RpcClient<Rpcs>,
  entityId: string,
): EntityActorRef<State, Event> => {
  // SAFETY: EntityRpcs guarantees the generated client exposes Send, Ask, GetState, and WatchState.
  // eslint-disable-next-line anti-slop/no-chained-type-assertions -- RpcClient does not preserve method correlations
  const c = client as unknown as TypedEntityClient<State, Event>;

  return {
    entityId,
    send: (event: Event) => c.Send({ event }),
    ask: (event) => c.Ask({ event }),
    snapshot: c.GetState(),
    watch: c.WatchState(),
    waitFor: (predicate: (state: State) => boolean) =>
      Effect.gen(function* () {
        // Snapshot first — if current state already matches, return immediately
        const current = yield* c.GetState();
        if (predicate(current)) return current;
        // Fall through to streaming observation
        const result = yield* c
          .WatchState()
          .pipe(Stream.filter(predicate), Stream.take(1), Stream.runHead);
        if (Option.isSome(result)) return result.value;
        return yield* new ActorStoppedError({ actorId: entityId });
      }),
  };
};
