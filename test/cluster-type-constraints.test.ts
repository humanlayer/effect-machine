import type { Sharding } from "effect/unstable/cluster";
import type { RpcClient } from "effect/unstable/rpc";
import { Context, Effect, type Layer, Schema } from "effect";

import { Event, Machine, State } from "../src/index.js";
import type { NoReplyError } from "../src/errors.js";
import {
  EntityMachine,
  type EntityMachineOptions,
  makeEntityActorRef,
  type PersistenceAdapter,
  toEntity,
} from "../src/cluster/index.js";

const ClusterState = State({ Active: { count: Schema.Number } });
const ClusterEvent = Event({
  GetCount: Event.reply({}, Schema.Number),
  Increment: {},
});

class ClusterService extends Context.Service<
  ClusterService,
  { readonly run: Effect.Effect<void> }
>()("@humanlayer/effect-machine/test/cluster-type-constraints.test/ClusterService") {}

const clusterMachine = Machine.make({
  state: ClusterState,
  event: ClusterEvent,
  initial: ClusterState.Active({ count: 0 }),
})
  .on(ClusterState.Active, ClusterEvent.GetCount, ({ state }) => Machine.reply(state, state.count))
  .background(() => ClusterService.pipe(Effect.andThen((service) => service.run)));

const ClusterEntity = toEntity(clusterMachine, { type: "TypeConstraints" });
const withoutPersistence = EntityMachine.layer(ClusterEntity);
const withPersistence = EntityMachine.layer(ClusterEntity, {
  persistence: { strategy: "journal" },
});
const optionsVariable: EntityMachineOptions<typeof ClusterState.Type, typeof ClusterEvent.Type> =
  {};
const withConservativeOptions = EntityMachine.layer(ClusterEntity, optionsVariable);

type Requirements<Value> = Value extends Layer.Layer<never, never, infer R> ? R : never;
type EffectError<Value> = Value extends Effect.Effect<infer _A, infer E, infer _R> ? E : never;
type Assert<Condition extends true> = Condition;

type _LayerRequiresMachineService = Assert<
  ClusterService extends Requirements<typeof withoutPersistence> ? true : false
>;
type _LayerRequiresSharding = Assert<
  Sharding.Sharding extends Requirements<typeof withoutPersistence> ? true : false
>;
type _LayerDoesNotRequireDisabledPersistence = Assert<
  PersistenceAdapter extends Requirements<typeof withoutPersistence> ? false : true
>;
type _PersistentLayerRequiresAdapter = Assert<
  PersistenceAdapter extends Requirements<typeof withPersistence> ? true : false
>;
type _ConservativeOptionsLayerRequiresAdapter = Assert<
  PersistenceAdapter extends Requirements<typeof withConservativeOptions> ? true : false
>;

interface TransportError {
  readonly _tag: "TransportError";
}

const _entityActorRefPreservesClientErrors = (
  client: RpcClient.RpcClient<(typeof ClusterEntity.rpcs)[number], TransportError>,
) => {
  const ref = makeEntityActorRef(ClusterEntity, client, "entity-1");
  const send = ref.send(ClusterEvent.Increment);
  const ask = ref.ask(ClusterEvent.GetCount);
  type _SendPreservesClientError = Assert<
    TransportError extends EffectError<typeof send> ? true : false
  >;
  type _AskPreservesClientError = Assert<
    TransportError extends EffectError<typeof ask> ? true : false
  >;
  type _AskPreservesDomainAndDecodeErrors = Assert<
    NoReplyError | Schema.SchemaError extends EffectError<typeof ask> ? true : false
  >;
  return { send, ask };
};

export {};
