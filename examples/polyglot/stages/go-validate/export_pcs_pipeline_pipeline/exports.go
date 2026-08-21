// Package export_pcs_pipeline_pipeline implements the `pcs:pipeline/pipeline`
// export for stage 1 of the polyglot example — the **Go** guest.
//
// It reads `amount` and writes `valid`, the gate every later stage keys off:
// the Python stage converts only valid rows, and the Rust stage rejects the
// invalid ones outright.
//
// The package name and the `wit_component/...` import paths are not a choice:
// `componentize-go bindings` generates `wit_exports.go`, which imports this
// package by that exact path and calls exactly Describe and RunBatch. It also
// rewrites go.mod's module line to `wit_component`.
//
// # Why there is no Arrow dependency
//
// Writing `valid` means overwriting one bit per row in a fixed-width value
// buffer, so this stage mutates the input Arrow IPC bytes in place and hands the
// same buffer back — see the arrowipc package for the format and for what that
// deliberately cannot do. Everything else in the stream, including the trailing
// `__alive` bitmap, passes through untouched.
//
// # Why nothing here panics
//
// A Go panic inside a component traps the instance, and the host then sees an
// opaque wasm trap instead of a reason. Every failure path is therefore folded
// into `run-error::permanent`, which the WIT contract designates for bad input
// shape and guest bugs; `schema-mismatch` must never come out of `run-batch`.
package export_pcs_pipeline_pipeline

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	witTypes "go.bytecodealliance.org/pkg/wit/types"

	"wit_component/arrowipc"
	hostio "wit_component/pcs_pipeline_host_io"
	"wit_component/pcs_pipeline_types"
)

const (
	// stageName is what the driver and the integration test expect back from
	// describe(); the host keys config and checkpoint compatibility on it.
	stageName    = "polyglot-validate-go"
	stageVersion = "0.1.0"

	// logTarget is the tracing target the host bridges host-io::log onto.
	logTarget = "validate"

	componentName = "Order"
	fieldAmount   = "amount"
	fieldValid    = "valid"

	// minAmountKey is read from `pipeline.wasm.config` in the service TOML, or
	// from the driver's config map. Absent means "no floor".
	minAmountKey     = "min_amount"
	minAmountDefault = 0.0
)

// Describe reports the stage identity and the one component it operates on.
//
// The schema bytes and the fingerprint are generated constants rather than
// values computed here: encoding an Arrow schema flatbuffer would mean shipping
// a writer, and the fingerprint is derived from the canonical Rust `Order`
// definition. The driver and the integration test both fail loudly if either
// constant drifts from that definition.
func Describe() pcs_pipeline_types.PipelineDescriptor {
	schema, err := arrowipc.OrderSchemaIPC()
	if err != nil {
		// describe() has no error arm in the WIT world. A corrupt generated
		// constant is reported here and then surfaces as a load-time failure
		// when the host tries to parse the empty schema — which is a far better
		// diagnostic than trapping the instance.
		hostio.Log(hostio.LogLevelError, logTarget, err.Error())
	}
	return pcs_pipeline_types.PipelineDescriptor{
		Name:    stageName,
		Version: stageVersion,
		Components: []pcs_pipeline_types.ComponentDescriptor{{
			Name:           componentName,
			ArrowSchemaIpc: schema,
		}},
		Stateful:          false,
		SchemaFingerprint: arrowipc.OrderFingerprint,
	}
}

// RunBatch marks every row whose `amount` clears the configured floor as valid.
//
// `prior` is ignored and `checkpoint` is none: this stage keeps no state across
// batches, which is what `stateful: false` in Describe promises the host.
func RunBatch(input []uint8, prior witTypes.Option[[]uint8]) witTypes.Result[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError] {
	_ = prior
	started := time.Now()

	minAmount, err := configFloat(minAmountKey, minAmountDefault)
	if err != nil {
		return failure("%v", err)
	}

	stream, err := arrowipc.Parse(input)
	if err != nil {
		return failure("parse input stream: %v", err)
	}
	batch, err := stream.Component(componentName)
	if err != nil {
		return failure("locate %s batch: %v", componentName, err)
	}
	amounts, err := batch.Float64s(fieldAmount)
	if err != nil {
		return failure("read %s.%s: %v", componentName, fieldAmount, err)
	}

	invalid := 0
	for row, amount := range amounts {
		valid := amount > minAmount
		if !valid {
			invalid++
		}
		if err := batch.SetBool(fieldValid, row, valid); err != nil {
			return failure("write %s.%s row %d: %v", componentName, fieldValid, row, err)
		}
	}

	hostio.Metric("validate.invalid_rows", float64(invalid))
	hostio.Log(hostio.LogLevelInfo, logTarget, fmt.Sprintf(
		"%s: %d of %d rows valid at %s=%g",
		stageName, batch.Rows-invalid, batch.Rows, minAmountKey, minAmount,
	))

	rows := uint64(batch.Rows)
	return witTypes.Ok[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError](pcs_pipeline_types.RunResult{
		Output:     stream.Buf,
		Checkpoint: witTypes.None[[]uint8](),
		Metrics: pcs_pipeline_types.RunMetrics{
			WallNs:     uint64(time.Since(started).Nanoseconds()),
			RowsIn:     rows,
			RowsOut:    rows,
			SystemsRun: 1,
			Retries:    0,
		},
	})
}

// configFloat reads a host-injected config value. The WIT contract hands config
// over as strings and leaves numeric parsing to the guest, so an unparseable
// value is a misconfiguration worth failing on rather than silently defaulting.
func configFloat(key string, fallback float64) (float64, error) {
	raw := hostio.GetConfig(key)
	if !raw.IsSome() {
		return fallback, nil
	}
	text := strings.TrimSpace(raw.Some())
	if text == "" {
		return fallback, nil
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, fmt.Errorf("config %s=%q is not a float64", key, text)
	}
	return value, nil
}

// failure logs and returns the permanent error arm. Nothing this stage can hit
// is worth a host retry: the same bytes and the same config would fail again.
func failure(format string, args ...any) witTypes.Result[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError] {
	message := fmt.Sprintf(format, args...)
	hostio.Log(hostio.LogLevelError, logTarget, stageName+": "+message)
	return witTypes.Err[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError](
		pcs_pipeline_types.MakeRunErrorPermanent(message),
	)
}
