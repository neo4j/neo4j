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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.csvInterval;
import static org.neo4j.metrics.MetricsSettings.csvPath;

public class CsvOutput implements Lifecycle, EventReporter
{
    private final Config config;
    private final MetricRegistry registry;
    private final Log logger;
    private final KernelContext kernelContext;
    private final FileSystemAbstraction fileSystem;
    private final JobScheduler scheduler;
    private RotatableCsvReporter csvReporter;
    private File outputPath;

    CsvOutput( Config config, MetricRegistry registry, Log logger, KernelContext kernelContext,
            FileSystemAbstraction fileSystem, JobScheduler scheduler )
    {
        this.config = config;
        this.registry = registry;
        this.logger = logger;
        this.kernelContext = kernelContext;
        this.fileSystem = fileSystem;
        this.scheduler = scheduler;
    }

    @Override
    public void init() throws IOException
    {
        // Setup CSV reporting
        File configuredPath = config.get( csvPath );
        if ( configuredPath == null )
        {
            throw new IllegalArgumentException( csvPath.name() + " configuration is required since " +
                                                csvEnabled.name() + " is enabled" );
        }
        Long rotationThreshold = config.get( MetricsSettings.csvRotationThreshold );
        Integer maxArchives = config.get( MetricsSettings.csvMaxArchives );
        outputPath = absoluteFileOrRelativeTo( kernelContext.storeDir(), configuredPath );
        csvReporter = RotatableCsvReporter.forRegistry( registry )
                .convertRatesTo( TimeUnit.SECONDS )
                .convertDurationsTo( TimeUnit.MILLISECONDS )
                .formatFor( Locale.US )
                .outputStreamSupplierFactory(
                        getFileRotatingFileOutputStreamSupplier( rotationThreshold, maxArchives ) )
                .build( ensureDirectoryExists( outputPath ) );
    }

    @Override
    public void start()
    {
        csvReporter.start( config.get( csvInterval ).toMillis(), TimeUnit.MILLISECONDS );
        logger.info( "Sending metrics to CSV file at " + outputPath );
    }

    @Override
    public void stop() throws IOException
    {
        csvReporter.stop();
    }

    @Override
    public void shutdown()
    {
        csvReporter = null;
    }

    @Override
    public void report( SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters,
            SortedMap<String,Histogram> histograms, SortedMap<String,Meter> meters, SortedMap<String,Timer> timers )
    {
        csvReporter.report( gauges, counters, histograms, meters, timers );
    }

    private BiFunction<File,RotatingFileOutputStreamSupplier.RotationListener,RotatingFileOutputStreamSupplier> getFileRotatingFileOutputStreamSupplier(
            Long rotationThreshold, Integer maxArchives )
    {
        return ( File file, RotatingFileOutputStreamSupplier.RotationListener listener ) ->
        {
            try
            {
                return new RotatingFileOutputStreamSupplier( fileSystem, file, rotationThreshold, 0, maxArchives,
                        scheduler.executor( JobScheduler.Groups.metricsLogRotations ), listener );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private File ensureDirectoryExists( File dir ) throws IOException
    {
        if ( !fileSystem.fileExists( dir ) )
        {
            fileSystem.mkdirs( dir );
        }
        if ( !fileSystem.isDirectory( dir ) )
        {
            throw new IllegalStateException(
                    "The given path for CSV files points to a file, but a directory is required: " +
                            dir.getAbsolutePath() );
        }
        return dir;
    }

    /**
     * Looks at configured file {@code absoluteOrRelativeFile} and just returns it if absolute, otherwise
     * returns a {@link File} with {@code baseDirectoryIfRelative} as parent.
     *
     * @param baseDirectoryIfRelative base directory to use as parent if {@code absoluteOrRelativeFile}
     * is relative, otherwise unused.
     * @param absoluteOrRelativeFile file to return as absolute or relative to {@code baseDirectoryIfRelative}.
     */
    private static File absoluteFileOrRelativeTo( File baseDirectoryIfRelative, File absoluteOrRelativeFile )
    {
        return absoluteOrRelativeFile.isAbsolute()
                ? absoluteOrRelativeFile
                : new File( baseDirectoryIfRelative, absoluteOrRelativeFile.getPath() );
    }
}
