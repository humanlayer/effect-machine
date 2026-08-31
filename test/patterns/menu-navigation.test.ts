import { Effect, Schema } from "effect";
import { describe, expect, test } from "bun:test";

import {
  assertNeverReaches,
  assertPath,
  Event,
  Machine,
  simulate,
  State,
} from "../../src/index.js";

/**
 * Menu navigation pattern tests based on bite menu.machine.ts
 * Tests: page navigation with guards, section scrolling, item selection
 */
describe("Menu Navigation Pattern", () => {
  type Page = { id: string; sections: Section[] };
  type Section = { id: string; items: Item[] };
  type Item = { id: string; name: string; available: boolean };

  const MenuState = State({
    Browsing: {
      pageId: Schema.String,
      sectionIndex: Schema.Number,
      itemIndex: Schema.NullOr(Schema.Number),
    },
    ItemSelected: { pageId: Schema.String, sectionIndex: Schema.Number, itemId: Schema.String },
    Checkout: { items: Schema.Array(Schema.String) },
    Closed: {},
  });
  type MenuState = typeof MenuState.Type;

  const MenuEvent = Event({
    NavigateToPage: { pageId: Schema.String },
    ScrollToSection: { sectionIndex: Schema.Number },
    SelectItem: { itemId: Schema.String },
    AddToCart: {},
    GoToCheckout: {},
    Close: {},
  });
  type MenuEvent = typeof MenuEvent.Type;

  // State/Event type aliases for guards
  type BrowsingState = MenuState & { _tag: "Browsing" };

  // Mock data
  const pages: Page[] = [
    {
      id: "food",
      sections: [
        { id: "appetizers", items: [{ id: "fries", name: "Fries", available: true }] },
        { id: "mains", items: [{ id: "burger", name: "Burger", available: true }] },
      ],
    },
    {
      id: "drinks",
      sections: [
        { id: "soft", items: [{ id: "cola", name: "Cola", available: true }] },
        { id: "alcohol", items: [{ id: "beer", name: "Beer", available: false }] },
      ],
    },
  ];

  const cart: string[] = [];

  const menuMachine = Machine.make({
    state: MenuState,
    event: MenuEvent,
    initial: MenuState.Browsing({ pageId: "food", sectionIndex: 0, itemIndex: null }),
  })
    // Browsing handlers
    // Navigate to different page (reset section)
    .on(MenuState.Browsing, MenuEvent.NavigateToPage, ({ state, event }) =>
      state.pageId !== event.pageId && pages.some((page) => page.id === event.pageId)
        ? MenuState.Browsing({ pageId: event.pageId, sectionIndex: 0, itemIndex: null })
        : state,
    )
    // Scroll to section
    .on(MenuState.Browsing, MenuEvent.ScrollToSection, ({ state, event }) => {
      const page = pages.find((candidate) => candidate.id === state.pageId);
      return page !== undefined &&
        event.sectionIndex >= 0 &&
        event.sectionIndex < page.sections.length
        ? MenuState.Browsing({
            ...state,
            sectionIndex: event.sectionIndex,
            itemIndex: null,
          })
        : state;
    })
    // Select item
    .on(MenuState.Browsing, MenuEvent.SelectItem, ({ state, event }) =>
      MenuState.ItemSelected({
        pageId: state.pageId,
        sectionIndex: state.sectionIndex,
        itemId: event.itemId,
      }),
    )
    // Go to checkout
    .on(MenuState.Browsing, MenuEvent.GoToCheckout, () => MenuState.Checkout({ items: [...cart] }))
    // Close menu
    .on(MenuState.Browsing, MenuEvent.Close, () => MenuState.Closed)
    // ItemSelected handlers
    // Add to cart and return to browsing
    .on(MenuState.ItemSelected, MenuEvent.AddToCart, ({ state }) => {
      cart.push(state.itemId);
      return MenuState.Browsing({
        pageId: state.pageId,
        sectionIndex: state.sectionIndex,
        itemIndex: null,
      });
    })
    // Cancel selection - return to browsing
    .on(MenuState.ItemSelected, MenuEvent.Close, ({ state }) =>
      MenuState.Browsing({
        pageId: state.pageId,
        sectionIndex: state.sectionIndex,
        itemIndex: null,
      }),
    )
    // Checkout handlers
    .on(MenuState.Checkout, MenuEvent.Close, () => MenuState.Closed)
    .final(MenuState.Closed);

  test("page navigation with valid page", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const result = yield* simulate(menuMachine, [
          MenuEvent.NavigateToPage({ pageId: "drinks" }),
        ]);

        expect(result.finalState._tag).toBe("Browsing");
        expect((result.finalState as BrowsingState).pageId).toBe("drinks");
        expect((result.finalState as BrowsingState).sectionIndex).toBe(0);
      }),
    );
  });

  test("page navigation to same page is no-op", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const result = yield* simulate(menuMachine, [
          MenuEvent.ScrollToSection({ sectionIndex: 1 }),
          MenuEvent.NavigateToPage({ pageId: "food" }), // Same page
        ]);

        // Section should still be 1 (internal transition preserved state)
        expect((result.finalState as BrowsingState).sectionIndex).toBe(1);
      }),
    );
  });

  test("page navigation to invalid page blocked", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const result = yield* simulate(menuMachine, [
          MenuEvent.NavigateToPage({ pageId: "nonexistent" }),
        ]);

        // Should stay on food (initial page)
        expect((result.finalState as BrowsingState).pageId).toBe("food");
      }),
    );
  });

  test("section scrolling with valid index", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const result = yield* simulate(menuMachine, [
          MenuEvent.ScrollToSection({ sectionIndex: 1 }),
        ]);

        expect((result.finalState as BrowsingState).sectionIndex).toBe(1);
      }),
    );
  });

  test("section scrolling with invalid index blocked", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const result = yield* simulate(menuMachine, [
          MenuEvent.ScrollToSection({ sectionIndex: 99 }), // Invalid
        ]);

        expect((result.finalState as BrowsingState).sectionIndex).toBe(0);
      }),
    );
  });

  test("item selection flow", async () => {
    await Effect.runPromise(
      assertPath(
        menuMachine,
        [MenuEvent.SelectItem({ itemId: "burger" }), MenuEvent.AddToCart],
        ["Browsing", "ItemSelected", "Browsing"],
      ),
    );
  });

  test("cancel selection returns to browsing", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const result = yield* simulate(menuMachine, [
          MenuEvent.ScrollToSection({ sectionIndex: 1 }),
          MenuEvent.SelectItem({ itemId: "burger" }),
          MenuEvent.Close,
        ]);

        expect(result.finalState._tag).toBe("Browsing");
        // Preserves section from before selection
        expect((result.finalState as BrowsingState).sectionIndex).toBe(1);
      }),
    );
  });

  test("checkout flow", async () => {
    await Effect.runPromise(
      assertPath(
        menuMachine,
        [MenuEvent.SelectItem({ itemId: "fries" }), MenuEvent.AddToCart, MenuEvent.GoToCheckout],
        ["Browsing", "ItemSelected", "Browsing", "Checkout"],
      ),
    );
  });

  test("close menu from browsing", async () => {
    await Effect.runPromise(assertPath(menuMachine, [MenuEvent.Close], ["Browsing", "Closed"]));
  });

  test("navigation never reaches checkout without explicit action", async () => {
    await Effect.runPromise(
      assertNeverReaches(
        menuMachine,
        [
          MenuEvent.NavigateToPage({ pageId: "drinks" }),
          MenuEvent.ScrollToSection({ sectionIndex: 1 }),
          MenuEvent.NavigateToPage({ pageId: "food" }),
        ],
        "Checkout",
      ),
    );
  });

  test("complex navigation flow", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const result = yield* simulate(menuMachine, [
          MenuEvent.NavigateToPage({ pageId: "drinks" }),
          MenuEvent.ScrollToSection({ sectionIndex: 1 }),
          MenuEvent.SelectItem({ itemId: "beer" }),
          MenuEvent.Close, // Cancel, back to browsing
          MenuEvent.NavigateToPage({ pageId: "food" }),
          MenuEvent.SelectItem({ itemId: "burger" }),
          MenuEvent.AddToCart,
          MenuEvent.GoToCheckout,
        ]);

        expect(result.finalState._tag).toBe("Checkout");
      }),
    );
  });
});
