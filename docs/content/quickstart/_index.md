+++
title = "Quick Start"
description = "Install the binary, then run a real pipeline in 15 minutes."
template = "section.html"
sort_by = "weight"

[extra]
kicker = "Quick Start"
+++

## Two pages

[Installation](@/quickstart/installation.md) installs `pcs-service` and the
toolchains the tutorial needs. One `cargo install` from a checkout, no feature
flags.

[Running it!](@/quickstart/running-it.md) builds a Rust WebAssembly processor,
runs it through `pcs-service` with a minimal config, and reads the result. It
takes about 15 minutes, end to end.
