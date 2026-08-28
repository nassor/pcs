//! `Tooltip`, hover-only.
//!
//! shadcn's tooltip is Radix-positioned. There is no Radix here, so this uses
//! the same markup and classes with CSS positioning: the trigger is the
//! positioning context and the content sits above it. That covers the one thing
//! the dashboard needs — hover a graph node, read its numbers — without
//! porting a collision-aware positioner.

use leptos::prelude::*;

/// shadcn `Tooltip`: `children` is the trigger, `content` the bubble.
#[component]
pub fn Tooltip(
    /// The bubble's text, evaluated on every render.
    #[prop(into)]
    content: Signal<String>,
    children: Children,
) -> impl IntoView {
    let (open, set_open) = signal(false);
    view! {
        <span
            data-slot="tooltip-trigger"
            class="relative inline-flex"
            on:pointerenter=move |_| set_open.set(true)
            on:pointerleave=move |_| set_open.set(false)
        >
            {children()}
            <Show when=move || open.get()>
                <span
                    data-slot="tooltip-content"
                    role="tooltip"
                    class="pointer-events-none absolute bottom-full left-1/2 z-50 mb-1 \
                           -translate-x-1/2 rounded-md bg-foreground px-2 py-1 text-xs \
                           text-background shadow-md whitespace-pre"
                >
                    {move || content.get()}
                </span>
            </Show>
        </span>
    }
}
