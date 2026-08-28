+++
title = "Quick Start"
description = "Put the binary on your PATH, then run a live NATS stream through a Go processor and a C# processor into PostgreSQL."
template = "section.html"
sort_by = "weight"

[extra]
kicker = "Quick Start"
+++

## Two pages

[Installation](@/quickstart/installation.md) installs `pcs-service` and the three
toolchains the tutorial needs. One `cargo install`, no feature flags.

[Running it!](@/quickstart/running-it.md) runs the stack: NATS in, two
WebAssembly processors in two languages, PostgreSQL out, with a live dashboard on
each service.
