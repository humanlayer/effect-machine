import { Context, Effect, Layer, Schema } from "effect";
import { describe, expect, it } from "effect-bun-test";

import { ActorSystemDefault, ActorSystemService, Event, Machine, State } from "../src/index.js";

class GreetingService extends Context.Service<
  GreetingService,
  { readonly greet: (name: string) => Effect.Effect<string> }
>()("@humanlayer/effect-machine/test/GreetingService") {}

const GreetingState = State({
  Idle: {},
  Loading: { name: Schema.String },
  Done: { message: Schema.String },
});

const GreetingEvent = Event({
  Start: { name: Schema.String },
  Loaded: { message: Schema.String },
});

const greetingMachine = Machine.make({
  state: GreetingState,
  event: GreetingEvent,
  initial: GreetingState.Idle,
})
  .on(GreetingState.Idle, GreetingEvent.Start, ({ event }) =>
    GreetingState.Loading({ name: event.name }),
  )
  .task(
    GreetingState.Loading,
    ({ state }) =>
      Effect.gen(function* () {
        const greetings = yield* GreetingService;
        return yield* greetings.greet(state.name);
      }),
    { onSuccess: (message) => GreetingEvent.Loaded({ message }) },
  )
  .on(GreetingState.Loading, GreetingEvent.Loaded, ({ event }) =>
    GreetingState.Done({ message: event.message }),
  )
  .final(GreetingState.Done);

const StreamState = State({
  Idle: {},
  Streaming: {},
  Done: { message: Schema.String },
});

const StreamEvent = Event({
  Start: {},
  Received: { message: Schema.String },
});

const streamMachine = Machine.make({
  state: StreamState,
  event: StreamEvent,
  initial: StreamState.Idle,
})
  .on(StreamState.Idle, StreamEvent.Start, () => StreamState.Streaming)
  .spawn(StreamState.Streaming, ({ self }) =>
    Effect.gen(function* () {
      const greetings = yield* GreetingService;
      const message = yield* greetings.greet("Grace");
      yield* self.send(StreamEvent.Received({ message }));
    }),
  )
  .on(StreamState.Streaming, StreamEvent.Received, ({ event }) =>
    StreamState.Done({ message: event.message }),
  )
  .final(StreamState.Done);

describe("service requirements", () => {
  it.scopedLive("runs state tasks with services supplied by a Layer", () =>
    Effect.gen(function* () {
      const actor = yield* Machine.spawn(greetingMachine);
      yield* actor.start;
      yield* actor.send(GreetingEvent.Start({ name: "Ada" }));

      const state = yield* actor.awaitFinal;
      expect(state).toEqual(GreetingState.Done({ message: "Hello, Ada!" }));
    }).pipe(
      Effect.provide(
        Layer.succeed(GreetingService, {
          greet: (name) => Effect.succeed(`Hello, ${name}!`),
        }),
      ),
    ),
  );

  it.scopedLive("runs state-scoped effects with services supplied by a Layer", () =>
    Effect.gen(function* () {
      const actor = yield* Machine.spawn(streamMachine);
      yield* actor.start;
      yield* actor.send(StreamEvent.Start);

      const state = yield* actor.awaitFinal;
      expect(state).toEqual(StreamState.Done({ message: "Hello, Grace!" }));
    }).pipe(
      Effect.provide(
        Layer.succeed(GreetingService, {
          greet: (name) => Effect.succeed(`Hello, ${name}!`),
        }),
      ),
    ),
  );

  it.scopedLive("propagates service requirements through ActorSystem.spawn", () =>
    Effect.gen(function* () {
      const system = yield* ActorSystemService;
      const actor = yield* system.spawn("greeting-service-test", greetingMachine);
      yield* actor.send(GreetingEvent.Start({ name: "Lin" }));

      const state = yield* actor.awaitFinal;
      expect(state).toEqual(GreetingState.Done({ message: "Hello, Lin!" }));
    }).pipe(
      Effect.provide(ActorSystemDefault),
      Effect.provide(
        Layer.succeed(GreetingService, {
          greet: (name) => Effect.succeed(`Hello, ${name}!`),
        }),
      ),
    ),
  );
});
