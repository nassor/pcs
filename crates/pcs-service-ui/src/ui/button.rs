//! `Button`, outline variant, the only one the dashboard uses.

use leptos::prelude::*;

/// shadcn `Button`, `variant="outline" size="sm"`.
///
/// The dashboard is read-only, so there is no destructive or primary action to
/// style: every button here re-fetches something.
#[component]
pub fn Button(
    /// Click handler.
    #[prop(into)]
    on_click: Callback<()>,
    children: Children,
) -> impl IntoView {
    view! {
        <button
            data-slot="button"
            type="button"
            class="inline-flex h-8 shrink-0 items-center justify-center gap-1.5 rounded-md border \
                   border-border bg-background px-3 text-xs font-medium whitespace-nowrap \
                   transition-colors outline-none hover:bg-accent hover:text-accent-foreground \
                   focus-visible:ring-[3px] focus-visible:ring-ring/50 disabled:opacity-50"
            on:click=move |_| on_click.run(())
        >
            {children()}
        </button>
    }
}
