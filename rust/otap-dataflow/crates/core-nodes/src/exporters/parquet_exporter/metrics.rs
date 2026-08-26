// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics specific to the Parquet exporter IO lifecycle.

use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_telemetry::error::Error;
use otel_arrow_dfe_telemetry::instrument::Counter;
use otel_arrow_dfe_telemetry::metrics::{
    MeasurementMetricSet, MetricSet, MetricSetHandler, MetricSetSnapshot,
};
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
use otel_arrow_dfe_telemetry_macros::{AttributeEnum, attribute_set, metric_set};

/// Lifecycle operations for Parquet exporter files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
pub enum FileOperation {
    /// A file was created.
    Created,
    /// A file was closed.
    Closed,
    /// A flush was scheduled because the max rows threshold was reached.
    FlushScheduledMaxRows,
    /// A flush was scheduled because the max age threshold was reached.
    FlushScheduledMaxAge,
    /// A flush attempt was made.
    FlushAttempts,
    /// A flush completed successfully.
    FlushSuccesses,
    /// A flush failed.
    FlushFailures,
}

/// Parquet exporter IO metrics attributes.
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub struct ParquetExporterFileAttributes {
    /// The file operation type.
    pub operation: FileOperation,
}

/// Parquet exporter file IO metrics.
/// Grouped under `otap.exporter.parquet.files`.
#[metric_set(
    name = "otap.exporter.parquet.files",
    measurement_attributes = ParquetExporterFileAttributes
)]
#[derive(Debug, Default, Clone)]
pub struct ParquetExporterFileMetrics {
    /// Number of Parquet files processed (across all payload types and partitions).
    #[metric(unit = "{file}")]
    pub count: Counter<u64>,
}

/// Parquet exporter row IO metrics.
/// Grouped under `otap.exporter.parquet.rows`.
#[metric_set(name = "otap.exporter.parquet.rows")]
#[derive(Debug, Default, Clone)]
pub struct ParquetExporterRowMetrics {
    /// Total number of rows written into Parquet writers (appended, not necessarily flushed yet).
    #[metric(unit = "{row}")]
    pub written: Counter<u64>,
}

/// Shared bounded-cardinality Parquet exporter metrics tracker.
pub struct ParquetExporterMetrics {
    /// File metrics.
    pub files: MeasurementMetricSet<ParquetExporterFileMetrics>,
    /// Row metrics.
    pub rows: MetricSet<ParquetExporterRowMetrics>,
}

impl ParquetExporterMetrics {
    /// Registers Parquet exporter metric sets for a pipeline node.
    #[must_use]
    pub fn register(pipeline_ctx: &PipelineContext) -> Self {
        Self {
            files: ParquetExporterFileMetrics::register(pipeline_ctx),
            rows: pipeline_ctx.register_metrics::<ParquetExporterRowMetrics>(),
        }
    }

    /// Reports touched metric buckets.
    pub fn report(&mut self, reporter: &mut MetricsReporter) -> Result<(), Error> {
        reporter.report_measurement(&mut self.files)?;
        reporter.report(&mut self.rows)?;
        Ok(())
    }

    /// Takes every touched metric bucket for terminal handoff.
    pub fn terminal_snapshots(&mut self) -> Vec<MetricSetSnapshot> {
        let mut snapshots = self.files.terminal_snapshots();
        if self.rows.needs_flush() {
            snapshots.push(self.rows.snapshot());
        }
        snapshots
    }
}
