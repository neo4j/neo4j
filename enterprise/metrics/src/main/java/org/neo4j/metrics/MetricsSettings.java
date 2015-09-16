/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Settings;

import static org.neo4j.helpers.Settings.basePath;
import static org.neo4j.helpers.Settings.setting;

public class MetricsSettings
{
    public enum CsvFile
    {
        single, // Use a single file for all metrics, with one metric per column
        split // Use one file per metric
    }

    // Common settings
    public static Setting<String> metricsPrefix = setting( "metrics.prefix", Settings.STRING, "default" );

    // CSV settings
    public static Setting<Boolean> csvEnabled = setting( "metrics.csv.enabled", Settings.BOOLEAN, Settings.FALSE );
    public static Setting<File> csvPath = setting(
            "metrics.csv.path", Settings.PATH, "metrics.csv" , basePath( GraphDatabaseSettings.store_dir ) );
    public static Setting<CsvFile> csvFile = setting("metrics.csv.file", Settings.options(CsvFile.class), CsvFile.single.name());
    public static Setting<Long> csvInterval = setting( "metrics.csv.interval", Settings.DURATION, "3s" );

    // Graphite settings
    public static Setting<Boolean> graphiteEnabled = setting( "metrics.graphite.enabled", Settings.BOOLEAN, Settings.FALSE );
    public static Setting<HostnamePort> graphiteServer = setting( "metrics.graphite.server", Settings.HOSTNAME_PORT, ":2003" );
    public static Setting<Long> graphiteInterval = setting( "metrics.graphite.interval", Settings.DURATION, "3s" );

    // Ganglia settings
    public static Setting<Boolean> gangliaEnabled = setting( "metrics.ganglia.enabled", Settings.BOOLEAN, Settings.FALSE );
    public static Setting<HostnamePort> gangliaServer = setting( "metrics.ganglia.server", Settings.HOSTNAME_PORT, ":8469" );
    public static Setting<Long> gangliaInterval = setting( "metrics.ganglia.interval", Settings.DURATION, "3s" );

    // The below settings define what metrics to gather
    // By default everything is on
    public static Setting<Boolean> metricsEnabled = setting( "metrics.enabled", Settings.BOOLEAN, Settings.TRUE );

    public static Setting<Boolean> neoEnabled = setting( "metrics.neo4j.enabled", Settings.BOOLEAN, metricsEnabled );
    public static Setting<Boolean> neoTxEnabled = setting( "metrics.neo4j.tx.enabled", Settings.BOOLEAN, neoEnabled );
    public static Setting<Boolean> neoPageCacheEnabled = setting( "metrics.neo4j.pagecache.enabled", Settings.BOOLEAN, neoEnabled );
    public static Setting<Boolean> neoCountsEnabled = setting( "metrics.neo4j.counts.enabled", Settings.BOOLEAN, neoEnabled );
    public static Setting<Boolean> neoNetworkEnabled = setting( "metrics.neo4j.network.enabled", Settings.BOOLEAN, neoEnabled );
//    public static Setting<Boolean> neoLogEnabled = setting( "metrics.neo4j.log.enabled", Settings.BOOLEAN, neoEnabled );
//    public static Setting<Boolean> neoClusterEnabled = setting( "metrics.neo4j.cluster.enabled", Settings.BOOLEAN, neoEnabled );

//    public static Setting<Boolean> jvmEnabled = setting( "metrics.jvm.enabled", Settings.BOOLEAN, metricsEnabled );
//    public static Setting<Boolean> jvmGcEnabled = setting( "metrics.jvm.gc.enabled", Settings.BOOLEAN, jvmEnabled );
//    public static Setting<Boolean> jvmMemoryEnabled = setting( "metrics.jvm.memory.enabled", Settings.BOOLEAN, jvmEnabled );
//    public static Setting<Boolean> jvmBuffersEnabled = setting( "metrics.jvm.buffers.enabled", Settings.BOOLEAN, jvmEnabled );
//    public static Setting<Boolean> jvmThreadsEnabled = setting( "metrics.jvm.threads.enabled", Settings.BOOLEAN, jvmEnabled );
//    public static Setting<Boolean> jvmAllocationEnabled = setting( "metrics.jvm.allocation.enabled",
//            Settings.BOOLEAN, Settings.FALSE);
//    public static Setting<Boolean> jvmAllocationHistogramsEnabled = setting( "metrics.jvm.allocation.histograms.enabled",
//            Settings.BOOLEAN, Settings.FALSE);
//    public static Setting<File> jvmAllocationHistogramPath = setting( "metrics.jvm.allocation.histograms.path", Settings.PATH,
//            "allocation" , basePath(GraphDatabaseSettings.store_dir ) );
}
