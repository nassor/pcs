; Analyzer release tracking for Pcs.Sdk.Generators, the file RS2008 asks for.
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
PCS0001 | Pcs.Sdk | Error | Assembly declares no PCS processor
PCS0002 | Pcs.Sdk | Error | PCS processor declares no component
PCS0003 | Pcs.Sdk | Error | PCS component must be a reference type
PCS0004 | Pcs.Sdk | Error | PCS component needs a parameterless constructor
PCS0005 | Pcs.Sdk | Error | PCS component property must be settable
PCS0006 | Pcs.Sdk | Error | PCS component property has no Arrow type
PCS0007 | Pcs.Sdk | Error | PCS component name is declared twice
PCS0008 | Pcs.Sdk | Error | PCS transform has the wrong signature
PCS0009 | Pcs.Sdk | Error | PCS transform names a type that is not a component
PCS0010 | Pcs.Sdk | Error | PCS component declares no fields
