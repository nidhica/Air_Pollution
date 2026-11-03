"""Prepare CAAQM measurements for analysis.

``preprocessing.clean`` applies the configured source, pollutant, and date
filters, flags invalid measurements without deleting them, and fills a
missing city or state only from a matching ``stations.parquet`` row.
Gap filling and outlier deletion stay out of scope. Spike detection is not
part of this package.
"""

__all__: list[str] = []
