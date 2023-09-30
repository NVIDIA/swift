:orphan:

Labeled Metrics
===============

.. note::
   Labeled metrics are still an experimental feature. Metric names and labels
   may be subject to change as we explore the space.

.. warning::
   Enabling labeled metrics will likely cause a dramatic increase in the number
   of distinct metrics time series. Ensure your metrics pipeline is prepared.

Recent versions of Swift allow StatsD metrics to be emitted with explicit
application-defined labels, rather than relying on knowing how to unpack
the legacy label names. A variety of StatsD extension formats are available,
many of which are parsed by `statsd_exporter
<https://github.com/prometheus/statsd_exporter/>`__:

- ``librato``
- ``infuxdb``
- ``dogstatsd``
- ``signalfx``
- ``graphite``

Here, we will try to enumerate a number of common labels and their meanings,
which may be used when building graphs or relabeling metrics.


Common Labels
-------------

.. table::
   :align: left

   ================ ==========================================================
   Label Name       Meaning
   ---------------- ----------------------------------------------------------
   ``log_name``     The configured ``log_name`` for the code emitting the
                    metric. Commonly ``proxy-server``, ``object-server``, etc.
   ``account``      The quoted account name associated with the metric.
   ``container``    The quoted container name associated with the metric.
   ``policy_index`` The storage policy index associated with the metric.
   ``device``       The device name associated with the metric. This will
                    likely be most useful if device names are unique
                    throughout the cluster.
   ================ ==========================================================

Note that metrics should *not* be labeled with ``object`` names -- the
cardinality of objects is expected to be so high as to be problematic.
Some operators may even need to drop ``container`` labels to keep metric
cardinalities reasonable.

