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
import { Effect, Option, Schema, Stream } from "effect";

import type { ExtractReply, ReplyTypeBrand } from "../internal/brands.js";
import { ActorStoppedError, NoReplyError } from "../errors.js";
import type { EntityRpcs, MachineEntity } from "./to-entity.js";

/**
 * Typed client wrapper for remote entity machines.
 *
 * Unlike local `ActorRef`, this communicates over cluster RPCs.
 * Only operations that make sense over the network are exposed.
 *
 * @example
 * ```ts
 * const ref = makeEntityActorRef(entity, client, "order-123")
 * yield* ref.send(OrderEvent.Ship({ trackingId: "abc" }))
 * const state = yield* ref.snapshot
 * yield* ref.waitFor((s) => s._tag === "Shipped")
 * ```
 */
export interface EntityActorRef<
  State extends { readonly _tag: string },
  Event extends { readonly _tag: string },
  ClientError = never,
> {
  readonly entityId: string;

  /** Send event. Returns new state after processing. */
  readonly send: (event: Event) => Effect.Effect<State, ClientError>;

  /** Send event and get typed domain reply (via Event.reply() schema). */
  readonly ask: <E extends Event & ReplyTypeBrand<unknown>>(
    event: E,
  ) => Effect.Effect<ExtractReply<E>, ClientError | NoReplyError | Schema.SchemaError>;

  /** Get current state. */
  readonly snapshot: Effect.Effect<State, ClientError>;

  /** Stream of state changes (via WatchState streaming RPC). */
  readonly watch: Stream.Stream<State, ClientError>;

  /** Wait for a state matching the predicate. Snapshots first, then watches stream. */
  readonly waitFor: (
    predicate: (state: State) => boolean,
  ) => Effect.Effect<State, ClientError | ActorStoppedError>;
}

/**
 * Create an EntityActorRef from a RPC client.
 *
 * @example
 * ```ts
 * const makeClient = yield* Entity.makeTestClient(entity, entityLayer)
 * const client = yield* makeClient("order-123")
 * const ref = makeEntityActorRef(entity, client, "order-123")
 * yield* ref.send(OrderEvent.Process)
 * ```
 */
export const makeEntityActorRef = <
  State extends { readonly _tag: string },
  Event extends { readonly _tag: string },
  R,
  EntityType extends string,
  ClientError,
>(
  entity: MachineEntity<State, Event, R, EntityType>,
  client: RpcClient.RpcClient<
    EntityRpcs<Schema.Codec<State>, Schema.Codec<Event>>[number],
    ClientError
  >,
  entityId: string,
): EntityActorRef<State, Event, ClientError> => {
  const ask = <ReplyEvent extends Event & ReplyTypeBrand<unknown>>(event: ReplyEvent) => {
    const replySchema = entity.machine.replySchemas.get(event._tag);
    if (replySchema === undefined) {
      return Effect.fail(new NoReplyError({ actorId: entityId, eventTag: event._tag }));
    }
    const typedReplySchema = Schema.make<Schema.Decoder<ExtractReply<ReplyEvent>>>(replySchema.ast);
    return client
      .Ask({ event })
      .pipe(Effect.flatMap((reply) => Schema.decodeUnknownEffect(typedReplySchema)(reply)));
  };
  return {
    entityId,
    send: (event: Event) => client.Send({ event }),
    ask,
    snapshot: client.GetState(),
    watch: client.WatchState(),
    waitFor: (predicate: (state: State) => boolean) =>
      Effect.gen(function* () {
        // Snapshot first — if current state already matches, return immediately
        const current = yield* client.GetState();
        if (predicate(current)) return current;
        // Fall through to streaming observation
        const result = yield* client
          .WatchState()
          .pipe(Stream.filter(predicate), Stream.take(1), Stream.runHead);
        if (Option.isSome(result)) return result.value;
        return yield* new ActorStoppedError({ actorId: entityId });
      }),
  };
};
