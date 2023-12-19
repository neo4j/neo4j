/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.metrics;

import java.io.File;
import java.time.Duration;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.HostnamePort;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.HOSTNAME_PORT;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.buildSetting;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.pathSetting;
import static org.neo4j.kernel.configuration.Settings.range;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for the Neo4j Enterprise metrics reporting.
 */
@Description( "Metrics settings" )
public class MetricsSettings implements LoadableConfig
{
    // Common settings
    @Description( "A common prefix for the reported metrics field names. By default, this is either be 'neo4j', " +
                  "or a computed value based on the cluster and instance names, when running in an HA configuration." )
    public static final Setting<String> metricsPrefix = setting( "metrics.prefix", STRING, "neo4j" );

    // The below settings define what metrics to gather
    // By default everything is on
    @Description( "The default enablement value for all the supported metrics. Set this to `false` to turn off all " +
                  "metrics by default. The individual settings can then be used to selectively re-enable specific " +
                  "metrics." )
    public static final Setting<Boolean> metricsEnabled = setting( "metrics.enabled", BOOLEAN, TRUE );

    @Description( "The default enablement value for all Neo4j specific support metrics. Set this to `false` to turn " +
                  "off all Neo4j specific metrics by default. The individual `metrics.neo4j.*` metrics can then be " +
                  "turned on selectively." )
    public static final Setting<Boolean> neoEnabled = buildSetting( "metrics.neo4j.enabled", BOOLEAN ).inherits( metricsEnabled ).build();

    @Description( "Enable reporting metrics about transactions; number of transactions started, committed, etc." )
    public static final Setting<Boolean> neoTxEnabled = buildSetting( "metrics.neo4j.tx.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about the Neo4j page cache; page faults, evictions, flushes, exceptions, " +
                  "etc." )
    public static final Setting<Boolean> neoPageCacheEnabled = buildSetting(
            "metrics.neo4j.pagecache.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about approximately how many entities are in the database; nodes, " +
                  "relationships, properties, etc." )
    public static final Setting<Boolean> neoCountsEnabled = buildSetting(
            "metrics.neo4j.counts.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about the network usage." )
    public static final Setting<Boolean> neoNetworkEnabled = buildSetting(
            "metrics.neo4j.network.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about Causal Clustering mode." )
    public static final Setting<Boolean> causalClusteringEnabled = buildSetting(
            "metrics.neo4j.causal_clustering.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about Neo4j check pointing; when it occurs and how much time it takes to " +
                  "complete." )
    public static final Setting<Boolean> neoCheckPointingEnabled = buildSetting(
            "metrics.neo4j.checkpointing.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about the Neo4j log rotation; when it occurs and how much time it takes to "
                  + "complete." )
    public static final Setting<Boolean> neoLogRotationEnabled = buildSetting(
            "metrics.neo4j.logrotation.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about HA cluster info." )
    public static final Setting<Boolean> neoClusterEnabled = buildSetting(
            "metrics.neo4j.cluster.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about Server threading info." )
    public static final Setting<Boolean> neoServerEnabled = buildSetting(
            "metrics.neo4j.server.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about the duration of garbage collections" )
    public static final Setting<Boolean> jvmGcEnabled =
            buildSetting( "metrics.jvm.gc.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about the memory usage." )
    public static final Setting<Boolean> jvmMemoryEnabled = buildSetting( "metrics.jvm.memory.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about the buffer pools." )
    public static final Setting<Boolean> jvmBuffersEnabled = buildSetting( "metrics.jvm.buffers.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about the current number of threads running." )
    public static final Setting<Boolean> jvmThreadsEnabled = buildSetting( "metrics.jvm.threads.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about number of occurred replanning events." )
    public static final Setting<Boolean> cypherPlanningEnabled =
            buildSetting( "metrics.cypher.replanning.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    @Description( "Enable reporting metrics about Bolt Protocol message processing." )
    public static final Setting<Boolean> boltMessagesEnabled = buildSetting( "metrics.bolt.messages.enabled", BOOLEAN ).inherits( neoEnabled ).build();

    // CSV settings
    @Description( "Set to `true` to enable exporting metrics to CSV files" )
    public static final Setting<Boolean> csvEnabled = setting( "metrics.csv.enabled", BOOLEAN, TRUE );

    @Description( "The target location of the CSV files: a path to a directory wherein a CSV file per reported " +
                  "field  will be written." )
    public static final Setting<File> csvPath = pathSetting( "dbms.directories.metrics", "metrics" );

    @Description( "The reporting interval for the CSV files. That is, how often new rows with numbers are appended to " +
                  "the CSV files." )
    public static final Setting<Duration> csvInterval = setting( "metrics.csv.interval", DURATION, "3s" );

    @Description( "The file size in bytes at which the csv files will auto-rotate. If set to zero then no " +
            "rotation will occur. Accepts a binary suffix `k`, `m` or `g`." )
    public static final Setting<Long> csvRotationThreshold = buildSetting( "metrics.csv.rotation.size",
            BYTES, "10m" ).constraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Maximum number of history files for the csv files." )
    public static final Setting<Integer> csvMaxArchives = buildSetting( "metrics.csv.rotation.keep_number",
            INTEGER, "7" ).constraint( min( 1 ) ).build();

    // Graphite settings
    @Description( "Set to `true` to enable exporting metrics to Graphite." )
    public static final Setting<Boolean> graphiteEnabled = setting( "metrics.graphite.enabled", BOOLEAN, FALSE );

    @Description( "The hostname or IP address of the Graphite server" )
    public static final Setting<HostnamePort> graphiteServer = setting( "metrics.graphite.server", HOSTNAME_PORT, ":2003" );

    @Description( "The reporting interval for Graphite. That is, how often to send updated metrics to Graphite." )
    public static final Setting<Duration> graphiteInterval = setting( "metrics.graphite.interval", DURATION, "3s" );

    // Prometheus settings
    @Description( "Set to `true` to enable the Prometheus endpoint" )
    public static final Setting<Boolean> prometheusEnabled = setting( "metrics.prometheus.enabled", BOOLEAN, FALSE );

    @Description( "The hostname and port to use as Prometheus endpoint" )
    public static final Setting<HostnamePort> prometheusEndpoint =
            setting( "metrics.prometheus.endpoint", HOSTNAME_PORT, "localhost:2004" );
}
