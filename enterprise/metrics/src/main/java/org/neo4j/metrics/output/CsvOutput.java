/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.metrics.output;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.metrics.MetricsSettings;

import static org.neo4j.kernel.configuration.Config.absoluteFileOrRelativeTo;
import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.csvInterval;
import static org.neo4j.metrics.MetricsSettings.csvPath;

public class CsvOutput extends LifecycleAdapter
{
    private final Config config;
    private final MetricRegistry registry;
    private final Log logger;
    private final KernelContext kernelContext;
    private ScheduledReporter csvReporter;
    private File outputFile;

    public CsvOutput( Config config, MetricRegistry registry, Log logger, KernelContext kernelContext )
    {
        this.config = config;
        this.registry = registry;
        this.logger = logger;
        this.kernelContext = kernelContext;
    }

    @Override
    public void init()
    {
        if ( config.get( csvEnabled ) )
        {
            // Setup CSV reporting
            File configuredPath = config.get( csvPath );
            if ( configuredPath == null )
            {
                throw new IllegalArgumentException( csvPath.name() + " configuration is required since " +
                        csvEnabled.name() + " is enabled" );
            }
            outputFile = absoluteFileOrRelativeTo( kernelContext.storeDir(), configuredPath );
            MetricsSettings.CsvFile csvFile = config.get( MetricsSettings.csvFile );
            switch ( csvFile )
            {
            case single:
                csvReporter = CsvReporterSingle.forRegistry( registry )
                        .convertRatesTo( TimeUnit.SECONDS )
                        .convertDurationsTo( TimeUnit.MILLISECONDS )
                        .filter( MetricFilter.ALL )
                        .build( outputFile );
                break;
            case split:
                csvReporter = CsvReporter.forRegistry( registry )
                        .convertRatesTo( TimeUnit.SECONDS )
                        .convertDurationsTo( TimeUnit.MILLISECONDS )
                        .filter( MetricFilter.ALL )
                        .build( ensureDirectoryExists( outputFile ) );
                break;
            default: throw new IllegalArgumentException(
                    "Unsupported " + MetricsSettings.csvFile.name() + " setting: " + csvFile );
            }
        }
    }

    private File ensureDirectoryExists( File dir )
    {
        if ( !dir.exists() )
        {
            if ( !dir.mkdirs() )
            {
                throw new IllegalStateException(
                        "Could not create path for CSV files: " + dir.getAbsolutePath() );
            }
        }
        if ( dir.isFile() )
        {
            throw new IllegalStateException(
                    "The given path for CSV files points to a file, but a directory is required: " +
                    dir.getAbsolutePath() );
        }
        return dir;
    }

    @Override
    public void start()
    {
        if ( csvReporter != null )
        {
            csvReporter.start( config.get( csvInterval ), TimeUnit.MILLISECONDS );
            logger.info( "Sending metrics to CSV file at " + outputFile );
        }
    }

    @Override
    public void stop() throws IOException
    {
        if ( csvReporter != null )
        {
            csvReporter.stop();
            csvReporter = null;
        }
    }
}
