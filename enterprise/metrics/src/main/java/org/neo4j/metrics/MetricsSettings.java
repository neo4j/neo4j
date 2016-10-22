/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.metrics;

import java.io.File;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Settings;

import static org.neo4j.kernel.configuration.Settings.pathSetting;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for the Neo4j Enterprise metrics reporting.
 */
@Description( "Metrics settings" )
public class MetricsSettings
{
    // Common settings
    @Description( "A common prefix for the reported metrics field names. By default, this is either be 'neo4j', " +
                  "or a computed value based on the cluster and instance names, when running in an HA configuration." )
    public static Setting<String> metricsPrefix = setting( "metrics.prefix", Settings.STRING, "neo4j" );

    // The below settings define what metrics to gather
    // By default everything is on
    @Description( "The default enablement value for all the supported metrics. Set this to `false` to turn off all " +
                  "metrics by default. The individual settings can then be used to selectively re-enable specific " +
                  "metrics." )
    public static Setting<Boolean> metricsEnabled = setting( "metrics.enabled", Settings.BOOLEAN, Settings.FALSE );

    @Description( "The default enablement value for all Neo4j specific support metrics. Set this to `false` to turn " +
                  "off all Neo4j specific metrics by default. The individual `metrics.neo4j.*` metrics can then be " +
                  "turned on selectively." )
    public static Setting<Boolean> neoEnabled = setting( "metrics.neo4j.enabled", Settings.BOOLEAN, metricsEnabled );
    @Description( "Enable reporting metrics about transactions; number of transactions started, committed, etc." )
    public static Setting<Boolean> neoTxEnabled = setting( "metrics.neo4j.tx.enabled", Settings.BOOLEAN, neoEnabled );
    @Description( "Enable reporting metrics about the Neo4j page cache; page faults, evictions, flushes, exceptions, " +
                  "etc." )
    public static Setting<Boolean> neoPageCacheEnabled = setting(
            "metrics.neo4j.pagecache.enabled", Settings.BOOLEAN, neoEnabled );
    @Description( "Enable reporting metrics about approximately how many entities are in the database; nodes, " +
                  "relationships, properties, etc." )
    public static Setting<Boolean> neoCountsEnabled = setting(
            "metrics.neo4j.counts.enabled", Settings.BOOLEAN, neoEnabled );
    @Description( "Enable reporting metrics about the network usage." )
    public static Setting<Boolean> neoNetworkEnabled = setting(
            "metrics.neo4j.network.enabled", Settings.BOOLEAN, neoEnabled );
    @Description( "Enable reporting metrics about Causal Clustering mode." )
    public static Setting<Boolean> causalClusteringEnabled = setting(
            "metrics.neo4j.causal_clustering.enabled", Settings.BOOLEAN, neoEnabled );
    @Description( "Enable reporting metrics about Neo4j check pointing; when it occurs and how much time it takes to " +
                  "complete." )
    public static Setting<Boolean> neoCheckPointingEnabled = setting(
            "metrics.neo4j.checkpointing.enabled", Settings.BOOLEAN, neoEnabled );
    @Description( "Enable reporting metrics about the Neo4j log rotation; when it occurs and how much time it takes to "
                  + "complete." )
    public static Setting<Boolean> neoLogRotationEnabled = setting(
            "metrics.neo4j.logrotation.enabled", Settings.BOOLEAN, neoEnabled );
    @Description( "Enable reporting metrics about HA cluster info." )
    public static Setting<Boolean> neoClusterEnabled = setting(
            "metrics.neo4j.cluster.enabled", Settings.BOOLEAN, neoEnabled );
    @Description( "Enable reporting metrics about Server threading info." )
    public static Setting<Boolean> neoServerEnabled = setting(
            "metrics.neo4j.server.enabled", Settings.BOOLEAN, neoEnabled );

    @Description( "Enable reporting metrics about the duration of garbage collections" )
    public static Setting<Boolean> jvmGcEnabled = setting( "metrics.jvm.gc.enabled", Settings.BOOLEAN, neoEnabled );
    @Description( "Enable reporting metrics about the memory usage." )
    public static Setting<Boolean> jvmMemoryEnabled = setting( "metrics.jvm.memory.enabled", Settings.BOOLEAN, neoEnabled );
    @Description( "Enable reporting metrics about the buffer pools." )
    public static Setting<Boolean> jvmBuffersEnabled = setting( "metrics.jvm.buffers.enabled", Settings.BOOLEAN, neoEnabled );
    @Description( "Enable reporting metrics about the current number of threads running." )
    public static Setting<Boolean> jvmThreadsEnabled = setting( "metrics.jvm.threads.enabled", Settings.BOOLEAN, neoEnabled );

    @Description( "Enable reporting metrics about number of occurred replanning events." )
    public static Setting<Boolean> cypherPlanningEnabled = setting( "metrics.cypher.replanning.enabled", Settings.BOOLEAN, neoEnabled );

    @Description( "Enable reporting metrics about Bolt Protocol message processing." )
    public static Setting<Boolean> boltMessagesEnabled = setting( "metrics.bolt.messages.enabled", Settings.BOOLEAN, neoEnabled );

    // CSV settings
    @Description( "Set to `true` to enable exporting metrics to CSV files" )
    public static Setting<Boolean> csvEnabled = setting( "metrics.csv.enabled", Settings.BOOLEAN, Settings.FALSE );
    @Description( "The target location of the CSV files: a path to a directory wherein a CSV file per reported " +
                  "field  will be written." )
    public static Setting<File> csvPath = pathSetting( "dbms.directories.metrics", "metrics" );

    @Description( "The reporting interval for the CSV files. That is, how often new rows with numbers are appended to " +
                  "the CSV files." )
    public static Setting<Long> csvInterval = setting( "metrics.csv.interval", Settings.DURATION, "3s" );

    // Graphite settings
    @Description( "Set to `true` to enable exporting metrics to Graphite." )
    public static Setting<Boolean> graphiteEnabled = setting( "metrics.graphite.enabled", Settings.BOOLEAN, Settings.FALSE );
    @Description( "The hostname or IP address of the Graphite server" )
    public static Setting<HostnamePort> graphiteServer = setting( "metrics.graphite.server", Settings.HOSTNAME_PORT, ":2003" );
    @Description( "The reporting interval for Graphite. That is, how often to send updated metrics to Graphite." )
    public static Setting<Long> graphiteInterval = setting( "metrics.graphite.interval", Settings.DURATION, "3s" );
}
