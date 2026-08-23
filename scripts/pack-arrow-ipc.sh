#!/usr/bin/env bash
#
# Pack the five pcs-arrow-ipc codec packages into target/arrow-ipc-dist/.
#
# One Arrow IPC wire format, five languages, one version. The version lives in
# packages/VERSION and every manifest that carries one must match it:
#
#   packages/arrow-ipc-py/pyproject.toml
#   packages/arrow-ipc-ts/package.json
#   packages/arrow-ipc-kt/build.gradle.kts
#   packages/arrow-ipc-cs/Pcs.ArrowIpc.csproj
#
# Go has no manifest version: its version is the git tag
# packages/arrow-ipc-go/v<version>, so this script only builds it.
#
# Artifacts:
#
#   pcs_arrow_ipc-<v>-py3-none-any.whl   Python wheel
#   pcs_arrow_ipc-<v>.tar.gz             Python sdist
#   nassor-pcs-arrow-ipc-<v>.tgz         npm tarball
#   Pcs.ArrowIpc.<v>.nupkg               NuGet package
#   pcs-arrow-ipc-maven-<v>.tar.gz       the Pages Maven repository, as of <v>
#
# The Kotlin step is the one that writes into the working tree: it publishes into
# docs/static/maven/, which Zola copies verbatim into the built site. Those files
# are the released Maven repository and are committed.
#
# Each check below has its own exit code so a CI failure names the cause:
#
#   2 version mismatch   3 Go   4 Python   5 python -m build
#   6 Node/npm   7 dotnet   8 Gradle   9 an artifact is missing

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

PACKAGES="${REPO_ROOT}/packages"
DIST="${REPO_ROOT}/target/arrow-ipc-dist"
MAVEN="${REPO_ROOT}/docs/static/maven"

log() { echo "[pack] $*"; }

require() {
    # require <command> <exit-code> <install hint>
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "[pack] ERROR: $1 not found on PATH" >&2
        echo "[pack] install with: $3" >&2
        echo "[pack] see examples/polyglot/PINS.md for the pinned versions" >&2
        exit "$2"
    fi
}

VERSION="$(cat "${PACKAGES}/VERSION")"
log "version: ${VERSION}"

# One version, five packages. A manifest that drifts ships a package whose
# coordinate disagrees with the release tag, which no consumer can diagnose.
assert_declares() {
    # assert_declares <file> <grep pattern>
    if ! grep -qF -- "$2" "$1"; then
        echo "[pack] ERROR: $1 does not declare version ${VERSION}" >&2
        echo "[pack] expected to find: $2" >&2
        echo "[pack] packages/VERSION is the source of truth" >&2
        exit 2
    fi
}

assert_declares "${PACKAGES}/arrow-ipc-py/pyproject.toml" "version = \"${VERSION}\""
assert_declares "${PACKAGES}/arrow-ipc-ts/package.json" "\"version\": \"${VERSION}\""
assert_declares "${PACKAGES}/arrow-ipc-kt/build.gradle.kts" "version = \"${VERSION}\""
assert_declares "${PACKAGES}/arrow-ipc-cs/Pcs.ArrowIpc.csproj" "<Version>${VERSION}</Version>"
log "all four manifests declare ${VERSION}"

rm -rf "${DIST}"
mkdir -p "${DIST}"

require go 3 "https://go.dev/dl/ (1.25.5 or newer)"
log "building Go package (source-only distribution, nothing to pack)..."
( cd "${PACKAGES}/arrow-ipc-go" && go build ./... )

require python 4 "https://www.python.org/downloads/ (3.10 or newer)"
require npm 6 "ships with Node"
require dotnet 7 "https://dotnet.microsoft.com/download/dotnet/10.0 (SDK 10)"
require gradle 8 "https://gradle.org/install/ (8.14.4 or newer, on JDK 21)"

log "packing Python wheel and sdist..."
if ! python -m build --outdir "${DIST}" "${PACKAGES}/arrow-ipc-py"; then
    echo "[pack] ERROR: python -m build failed" >&2
    echo "[pack] install with: pip install build" >&2
    exit 5
fi

log "packing npm tarball..."
(
    cd "${PACKAGES}/arrow-ipc-ts"
    if [[ -f package-lock.json ]]; then npm ci; else npm install; fi
    npm run --silent build
    npm pack --pack-destination "${DIST}"
)

log "packing NuGet package..."
dotnet pack "${PACKAGES}/arrow-ipc-cs" -c Release -o "${DIST}" --nologo

log "publishing the Kotlin package into docs/static/maven/..."
gradle -p "${PACKAGES}/arrow-ipc-kt" --quiet --console=plain \
    publishAllPublicationsToPagesRepository

# A Kotlin multiplatform publication is three Maven modules, `pcs-arrow-ipc`
# plus one per target, and the version list each carries lives in a
# maven-metadata.xml above the version directory. The asset is therefore the
# whole repository rather than one version directory: anything less does not
# resolve. Relative paths keep tar off a Windows drive letter, which it would
# read as a remote host.
tar -czf "target/arrow-ipc-dist/pcs-arrow-ipc-maven-${VERSION}.tar.gz" \
    -C docs/static maven

for f in "pcs_arrow_ipc-${VERSION}-py3-none-any.whl" \
         "pcs_arrow_ipc-${VERSION}.tar.gz" \
         "nassor-pcs-arrow-ipc-${VERSION}.tgz" \
         "Pcs.ArrowIpc.${VERSION}.nupkg" \
         "pcs-arrow-ipc-maven-${VERSION}.tar.gz"; do
    if [[ ! -f "${DIST}/${f}" ]]; then
        echo "[pack] ERROR: ${DIST}/${f} was not produced" >&2
        exit 9
    fi
done

log "PASS: artifacts in ${DIST}"
for f in "${DIST}"/*; do
    log "  $(basename "${f}") ($(wc -c < "${f}") bytes)"
done
log "maven repository: ${MAVEN}"
log "commit docs/static/maven/** with the release: Zola copies it verbatim"
