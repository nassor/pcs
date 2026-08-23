// Implements the `pcs:pipeline/pipeline` export for stage 5 of the polyglot
// example, the C# guest.
//
// It reads `flagged` and `risk_score` and writes `review_tier`, the escalation
// level the Rust stage turns into a settlement decision: tier 2 holds, tier 1
// goes to review, tier 0 settles.
//
// The namespace and the class name are not a choice. wit-bindgen generates
// PipelineExportsInterop, which calls exactly PipelineExportsImpl.Describe and
// PipelineExportsImpl.RunBatch by unqualified name from inside
// PcsPipelineWorld.wit.Exports.pcs.pipeline.v0_2_0, so the impl class has to live
// there under that name.
//
// Why there is no Arrow dependency
//
// Writing `review_tier` means overwriting eight bytes per row in a fixed-width
// value buffer, so this stage mutates the input Arrow IPC bytes in place and hands
// the same buffer back. The codec is the Pcs.ArrowIpc package, an ordinary project
// reference, which documents the format and what in-place mutation cannot do.
// Everything else in the stream, including the trailing `__alive` bitmap, passes
// through untouched.
//
// Why nothing here escapes as an exception
//
// The generated glue converts only WitException into the WIT error arm; anything
// else unwinds out of the component and traps the instance, leaving the host with
// an opaque trap instead of a reason. Every failure path is therefore folded into
// `run-error::permanent`, which the WIT contract designates for bad input shape
// and guest bugs. `schema-mismatch` is reserved for a future load-time check and
// must never come out of run-batch.

using System.Diagnostics;
using System.Globalization;
using Pcs.ArrowIpc;
using PcsPipelineWorld.wit.Imports.pcs.pipeline.v0_2_0;
using PolyglotTier;

namespace PcsPipelineWorld.wit.Exports.pcs.pipeline.v0_2_0;

public sealed class PipelineExportsImpl : IPipelineExports
{
    /// <summary>What the driver and the integration test expect back from
    /// describe(); the host keys config and checkpoint compatibility on it.</summary>
    private const string StageName = "polyglot-tier-cs";
    private const string StageVersion = "0.1.0";

    /// <summary>The tracing target the host bridges host-io::log onto.</summary>
    private const string LogTarget = "tier";

    private const string ComponentName = "Order";
    private const string FieldFlagged = "flagged";
    private const string FieldRiskScore = "risk_score";
    private const string FieldReviewTier = "review_tier";

    /// <summary>Read from `pipeline.wasm.config` in the service TOML, or from the
    /// driver's config map. It is the risk score at or above which an unflagged row
    /// still earns a look.</summary>
    private const string ReviewScoreKey = "review_score";
    private const double ReviewScoreDefault = 0.2;

    // Escalation levels, in the order the Rust stage reads them.
    private const long TierClear = 0;
    private const long TierReview = 1;
    private const long TierHold = 2;

    /// <summary>Reports the stage identity and the one component it operates on.</summary>
    /// <remarks>The schema bytes and the fingerprint are generated constants rather
    /// than values computed here: encoding an Arrow schema flatbuffer would mean
    /// shipping a writer, and the fingerprint is derived from the canonical Rust
    /// `Order` definition. The driver and the integration test both fail loudly if
    /// either constant drifts from that definition.</remarks>
    public static ITypesImports.PipelineDescriptor Describe()
    {
        byte[] schema;
        try
        {
            schema = ArrowIpc.DecodeBase64(SchemaGen.OrderSchemaIpcBase64);
        }
        catch (ArrowIpcException e)
        {
            // describe() has no error arm in the WIT world. A corrupt generated
            // constant is reported here and then surfaces as a load-time failure
            // when the host tries to parse the empty schema, which is a far better
            // diagnostic than trapping the instance.
            IHostIoImports.Log(IHostIoImports.LogLevel.ERROR, LogTarget, e.Message);
            schema = [];
        }

        return new ITypesImports.PipelineDescriptor(
            StageName,
            StageVersion,
            [new ITypesImports.ComponentDescriptor(ComponentName, schema)],
            stateful: false,
            SchemaGen.OrderFingerprint);
    }

    /// <summary>Assigns every row an escalation tier from `flagged` and
    /// `risk_score`.</summary>
    /// <remarks><paramref name="prior"/> is ignored and `checkpoint` is none: this
    /// stage keeps no state across batches, which is what `stateful: false` in
    /// Describe promises the host.</remarks>
    public static ITypesImports.RunResult RunBatch(byte[] input, byte[]? prior)
    {
        _ = prior;
        long started = Stopwatch.GetTimestamp();

        // phase names the step a failure came out of. The codec's own messages name
        // the component, the field and the row, so this only has to say what the
        // stage was trying to do; every value is a compile-time constant string.
        string phase = "read config";
        try
        {
            double reviewScore = ConfigFloat(ReviewScoreKey, ReviewScoreDefault);

            phase = "parse input stream";
            PcsStream stream = new(input);

            phase = $"locate {ComponentName} batch";
            ArrowBatch batch = stream.Component(ComponentName);

            phase = $"read {ComponentName}.{FieldFlagged}";
            bool[] flagged = batch.Bools(FieldFlagged);

            phase = $"read {ComponentName}.{FieldRiskScore}";
            double[] risk = batch.Float64s(FieldRiskScore);

            phase = $"write {ComponentName}.{FieldReviewTier}";
            int review = 0;
            int hold = 0;
            for (int row = 0; row < batch.Rows; row++)
            {
                long tier;
                if (flagged[row])
                {
                    tier = TierHold;
                    hold++;
                }
                else if (risk[row] >= reviewScore)
                {
                    tier = TierReview;
                    review++;
                }
                else
                {
                    tier = TierClear;
                }
                batch.SetInt64(FieldReviewTier, row, tier);
            }

            // Two counters rather than one "escalated" total: the Rust stage
            // treats the two tiers differently, so collapsing them would hide
            // which branch fired. The log names tiers, not outcomes, because
            // this stage never sees `valid` and cannot know a row will be
            // rejected downstream.
            IHostIoImports.Metric("tier.review_rows", review);
            IHostIoImports.Metric("tier.hold_rows", hold);
            IHostIoImports.Log(IHostIoImports.LogLevel.INFO, LogTarget, string.Format(
                CultureInfo.InvariantCulture,
                "{0}: tiers {1} clear, {2} review, {3} hold of {4} rows at {5}={6:G}",
                StageName, batch.Rows - review - hold, review, hold, batch.Rows,
                ReviewScoreKey, reviewScore));

            ulong rows = (ulong)batch.Rows;
            return new ITypesImports.RunResult(
                stream.Buffer,
                checkpoint: null,
                new ITypesImports.RunMetrics(
                    wallNs: (ulong)Math.Max(0.0, Stopwatch.GetElapsedTime(started).TotalNanoseconds),
                    rowsIn: rows,
                    rowsOut: rows,
                    systemsRun: 1,
                    retries: 0));
        }
        catch (ArrowIpcException e)
        {
            throw Failure($"{phase}: {e.Message}");
        }
        catch (WitException)
        {
            throw;
        }
        catch (Exception e)
        {
            // Unreachable by design: the codec bounds checks every read and reports
            // through ArrowIpcException. Reached anyway, it is a guest bug, and a
            // named exception in the host's log beats a bare wasm trap.
            throw Failure($"{phase}: unexpected {e.GetType().Name}: {e.Message}");
        }
    }

    /// <summary>Reads a host-injected config value.</summary>
    /// <remarks>The WIT contract hands config over as strings and leaves numeric
    /// parsing to the guest, so an unparseable value is a misconfiguration worth
    /// failing on rather than silently defaulting.</remarks>
    private static double ConfigFloat(string key, double fallback)
    {
        string? raw = IHostIoImports.GetConfig(key);
        if (raw is null)
        {
            return fallback;
        }
        string text = raw.Trim();
        if (text.Length == 0)
        {
            return fallback;
        }
        if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double value))
        {
            throw Failure($"config {key}=\"{text}\" is not a float64");
        }
        return value;
    }

    /// <summary>Logs and builds the permanent error arm. Callers write
    /// `throw Failure(...)` so the compiler still sees the path terminating.</summary>
    /// <remarks>Nothing this stage can hit is worth a host retry: the same bytes and
    /// the same config would fail again.</remarks>
    private static WitException<ITypesImports.RunError> Failure(string message)
    {
        IHostIoImports.Log(IHostIoImports.LogLevel.ERROR, LogTarget, $"{StageName}: {message}");
        return new WitException<ITypesImports.RunError>(ITypesImports.RunError.Permanent(message), 0);
    }
}
