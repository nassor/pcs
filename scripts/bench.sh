#!/usr/bin/env bash
#
# PCS benchmark harness.
#
# Reproducible criterion runs: fixed compiler flags, fixed sample size, optional
# CPU-affinity pinning, and A/B comparison against a saved baseline.
#
# Usage
# -----
#   scripts/bench.sh <bench> [options] [-- <extra criterion args>]
#
#   <bench>              one of: tpch_q6 tpch_q1 parallelism_compute parallelism
#                        pipeline batch_vs_stream ipc_checkpoint vs_datafusion_q6
#   --save NAME          store this run as criterion baseline NAME
#   --baseline NAME      compare against baseline NAME; prints % change and p-value
#   --affinity MASK      hex CPU affinity mask, e.g. FFFF for logical CPUs 0-15.
#                        Omit for all CPUs. Inherited by rayon/tokio workers.
#   --filter REGEX       only run benchmarks whose id matches REGEX
#   --samples N          criterion sample size (default 10)
#   --threads N          size the rayon/tokio pools to N. Pair with --affinity:
#                        num_cpus reports the machine, not the affinity mask.
#   --features LIST      extra cargo features, comma-separated
#   --build-only         compile and exit; always do this before a timing run
#
# Pinning matters on a dual-CCD part such as the Ryzen 9 9950X3D, where only one
# die carries 3D V-cache. The scheduler can migrate workers across dies and
# change L3 residency wholesale between runs. Pin for A/B comparison; take
# published figures unpinned, since that is what a deployment gets.
#
set -euo pipefail

usage() { sed -n '3,28p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-1}"; }

[ $# -eq 0 ] && usage 1
BENCH="$1"; shift

SAVE=""; BASELINE=""; AFFINITY=""; FILTER=""; SAMPLES="10"; EXTRA_FEATURES=""
THREADS=""; BUILD_ONLY=0; EXTRA=()

while [ $# -gt 0 ]; do
  case "$1" in
    --save)       SAVE="$2"; shift 2 ;;
    --baseline)   BASELINE="$2"; shift 2 ;;
    --affinity)   AFFINITY="$2"; shift 2 ;;
    --filter)     FILTER="$2"; shift 2 ;;
    --samples)    SAMPLES="$2"; shift 2 ;;
    --features)   EXTRA_FEATURES="$2"; shift 2 ;;
    --threads)    THREADS="$2"; shift 2 ;;
    --build-only) BUILD_ONLY=1; shift ;;
    -h|--help)    usage 0 ;;
    --)           shift; EXTRA=("$@"); break ;;
    *)            echo "unknown option: $1" >&2; usage 1 ;;
  esac
done

case "$BENCH" in
  tpch_q6|tpch_q1|parallelism|parallelism_compute|pipeline|ipc_checkpoint)
                     PKG="pcs-core";    FEATURES="" ;;
  batch_vs_stream)   PKG="pcs-core";    FEATURES="io" ;;
  vs_datafusion_q6)  PKG="pcs-service"; FEATURES="datafusion" ;;
  *) echo "unknown bench: $BENCH" >&2; usage 1 ;;
esac

if [ -n "$EXTRA_FEATURES" ]; then
  FEATURES="${FEATURES:+$FEATURES,}$EXTRA_FEATURES"
fi

# The published numbers are taken with these flags; changing them invalidates
# existing comparisons.
export RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native -C opt-level=3 -C codegen-units=1}"

FEATURE_ARGS=()
[ -n "$FEATURES" ] && FEATURE_ARGS=(--features "$FEATURES")

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Compile as its own step, always. A criterion run that shares the machine with
# rustc measures rustc.
if ! BUILD_LOG="$(cargo bench -p "$PKG" --bench "$BENCH" "${FEATURE_ARGS[@]}" --no-run 2>&1)"; then
  printf '%s\n' "$BUILD_LOG"
  exit 1
fi
printf '%s\n' "$BUILD_LOG"
[ "$BUILD_ONLY" -eq 1 ] && exit 0

# Run the harness directly rather than through cargo, so an affinity mask
# applies to the benchmark process itself instead of to a cargo parent that
# would spawn it unpinned.
#
# The path comes from cargo's own `Executable benches/<name>.rs (<path>)` line,
# printed on every --no-run invocation. Globbing `deps/<bench>-*` for the newest
# mtime is unsafe: cargo's metadata hash encodes profile, feature and RUSTFLAGS
# settings, so binaries built under different configurations coexist in that
# directory, and a build cargo considers fresh does not touch the artefact's
# mtime. Fail loudly rather than guess.
EXE="$(printf '%s\n' "$BUILD_LOG" \
  | grep -F "$BENCH.rs (" \
  | sed -n 's/.*(\(.*\))[[:space:]]*$/\1/p' \
  | tail -1)"
EXE="${EXE//\\//}"

if [ -z "$EXE" ] || [ ! -f "$EXE" ]; then
  echo "could not read an Executable path for bench '$BENCH' out of cargo's output" >&2
  echo "refusing to guess: a stale binary would produce a silently wrong measurement" >&2
  exit 1
fi

ARGS=(--bench --sample-size "$SAMPLES" --noplot)
[ -n "$SAVE" ]     && ARGS+=(--save-baseline "$SAVE")
[ -n "$BASELINE" ] && ARGS+=(--baseline "$BASELINE")
[ -n "$FILTER" ]   && ARGS+=("$FILTER")
[ ${#EXTRA[@]} -gt 0 ] && ARGS+=("${EXTRA[@]}")

# `num_cpus::get()` reports the machine, not the process affinity mask, so a
# pinned run would otherwise spin up one rayon worker per machine CPU and
# oversubscribe the pinned set. Size the pool to the CPUs available.
if [ -n "$THREADS" ]; then
  export RAYON_NUM_THREADS="$THREADS"
  export TOKIO_WORKER_THREADS="$THREADS"
fi

echo "== $BENCH  exe=$EXE  samples=$SAMPLES  affinity=${AFFINITY:-all}  threads=${THREADS:-default} ${SAVE:+save=$SAVE}${BASELINE:+baseline=$BASELINE}"

if [ -n "$AFFINITY" ]; then
  # Windows applies a process affinity mask to every thread the process owns,
  # existing and future, so setting it right after launch still binds the rayon
  # and tokio pools, which are created lazily long after this returns. `cmd /c
  # start /affinity` would set it at creation time but cannot be driven reliably
  # through MSYS argument mangling.
  WIN_EXE="$(cygpath -w "$EXE" 2>/dev/null || echo "$EXE")"
  PS_ARGS=""
  for a in "${ARGS[@]}"; do
    PS_ARGS="$PS_ARGS,'$(printf '%s' "$a" | sed "s/'/''/g")'"
  done
  PS_ARGS="${PS_ARGS#,}"
  powershell -NoProfile -Command "\$ErrorActionPreference='Stop'
    \$p = Start-Process -FilePath '$WIN_EXE' -ArgumentList @($PS_ARGS) -PassThru -NoNewWindow
    \$p.ProcessorAffinity = [System.IntPtr]0x$AFFINITY
    \$p.WaitForExit()
    exit \$p.ExitCode"
else
  "./$EXE" "${ARGS[@]}"
fi
