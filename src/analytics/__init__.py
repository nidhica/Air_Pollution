"""Analytical stages for the CAAQM series.

``analytics.spikes.detect`` scores the cleaned measurements.
``analytics.spatiotemporal.summarize`` aggregates the annotated spike file
by time, station, and place.

Functions added here should accept an ``AppConfig`` and a ``SparkSession``
created by the caller.
"""

__all__: list[str] = []
