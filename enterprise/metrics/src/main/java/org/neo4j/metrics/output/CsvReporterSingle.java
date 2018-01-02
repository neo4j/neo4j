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

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.Log;

import static java.lang.String.format;

/**
 * Version of CSV reporter that logs all metrics into a single file.
 * Restriction is that all metrics must be set up before starting it,
 * as it cannot handle changing sets of metrics at runtime.
 *
 * @deprecated please use {@code com.codahale.metrics.CsvReporter} instead. This reporter is incompatible with the
 * event based reporting that will be introduced in the next major release, hence it will be removed.
 */
@Deprecated
public class CsvReporterSingle extends ScheduledReporter
{
    // CSV separator
    public static final char SEPARATOR = ',';

    /**
     * Returns a new {@link Builder} for {@link CsvReporterSingle}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link CsvReporterSingle}
     */
    public static Builder forRegistry( MetricRegistry registry )
    {
        return new Builder( registry );
    }

    /**
     * A builder for {@link CsvReporterSingle} instances. Defaults to using the default locale, converting
     * rates to events/second, converting durations to milliseconds, and not filtering metrics.
     */
    public static class Builder
    {
        private final MetricRegistry registry;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private Log logger;
        private MetricFilter filter;

        private Builder( MetricRegistry registry )
        {
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.clock = Clock.defaultClock();
            this.filter = MetricFilter.ALL;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo( TimeUnit rateUnit )
        {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo( TimeUnit durationUnit )
        {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock( Clock clock )
        {
            this.clock = clock;
            return this;
        }

        /**
         * Use the given {@link Log} for reporting any errors.
         *
         * @param logger A Log instance, never null.
         * @return {@code this}
         */
        public Builder withLogger( Log logger )
        {
            this.logger = logger;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter( MetricFilter filter )
        {
            this.filter = filter;
            return this;
        }

        /**
         * Builds a {@link CsvReporterSingle} with the given properties, writing {@code .csv} data to the
         * given file.
         *
         * @param file the file in which the {@code .csv} data will be inserted
         * @return a {@link CsvReporterSingle}
         */
        public CsvReporterSingle build( File file )
        {
            return new CsvReporterSingle( registry,
                    file,
                    rateUnit,
                    durationUnit,
                    clock,
                    logger,
                    filter );
        }
    }

    private static final ThreadLocal<SimpleDateFormat> ISO8601 = new ThreadLocal<SimpleDateFormat>()
    {
        @Override
        protected SimpleDateFormat initialValue()
        {
            return new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
        }
    };

    private final File file;
    private final Clock clock;
    private final Log logger;
    private PrintWriter out;

    private CsvReporterSingle( MetricRegistry registry,
                               File file,
                               TimeUnit rateUnit,
                               TimeUnit durationUnit,
                               Clock clock,
                               Log logger,
                               MetricFilter filter )
    {
        super( registry, "csv-reporter-single", filter, rateUnit, durationUnit );
        this.file = file;
        this.clock = clock;
        this.logger = logger;
    }

    @Override
    public void stop()
    {
        super.stop();

        if ( out != null )
        {
            out.close();
        }
    }

    @Override
    public void report( SortedMap<String,Gauge> gauges,
            SortedMap<String,Counter> counters,
            SortedMap<String,Histogram> histograms,
            SortedMap<String,Meter> meters,
            SortedMap<String,Timer> timers )
    {
        if ( out == null )
        {
            try
            {
                startNewCsvFile( gauges, counters, histograms, meters, timers );
            }
            catch ( Exception e )
            {
                logger.warn( "Could not create output CSV file: " + file.getAbsolutePath(), e );
            }
        }

        StringBuilder line = new StringBuilder();

        final long timestamp = TimeUnit.MILLISECONDS.toSeconds( clock.getTime() );

        line.append( timestamp ).append( SEPARATOR ).append( ISO8601.get().format( new Date( clock.getTime() ) ) );

        for ( Map.Entry<String,Gauge> entry : gauges.entrySet() )
        {
            reportGauge( entry.getValue(), line );
        }

        for ( Map.Entry<String,Counter> entry : counters.entrySet() )
        {
            reportCounter( entry.getValue(), line );
        }

        for ( Map.Entry<String,Histogram> entry : histograms.entrySet() )
        {
            reportHistogram( entry.getValue(), line );
        }

        for ( Map.Entry<String,Meter> entry : meters.entrySet() )
        {
            reportMeter( entry.getValue(), line );
        }

        for ( Map.Entry<String,Timer> entry : timers.entrySet() )
        {
            reportTimer( entry.getValue(), line );
        }

        out.println( line.toString() );
        out.flush();
    }

    private void startNewCsvFile( SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters,
                                  SortedMap<String,Histogram> histograms, SortedMap<String,Meter> meters,
                                  SortedMap<String,Timer> timers ) throws IOException
    {
        if ( file.exists() )
        {
            archiveExistingLogFile();
        }

        out = new PrintWriter( file, StandardCharsets.UTF_8.name() );

        StringBuilder header = new StringBuilder();
        header.append( "timestamp" ).append( SEPARATOR ).append( "datetime" );

        for ( Map.Entry<String,Gauge> entry : gauges.entrySet() )
        {
            header.append( SEPARATOR ).append( entry.getKey() );
        }

        for ( Map.Entry<String,Counter> entry : counters.entrySet() )
        {
            header.append( SEPARATOR ).append( entry.getKey() );
        }

        for ( Map.Entry<String,Histogram> entry : histograms.entrySet() )
        {
            header
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".count" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".max" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".mean" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".min" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".stddev" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p50" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p75" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p95" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p98" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p99" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p999" );
        }

        for ( Map.Entry<String,Meter> entry : meters.entrySet() )
        {
            header
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".count" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".mean_rate" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".m1_rate" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".m5_rate" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".m15_rate" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".rate_unit" );
        }

        for ( Map.Entry<String,Timer> entry : timers.entrySet() )
        {
            header
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".count" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".max" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".mean" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".min" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".stddev" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p50" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p75" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p95" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p98" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p99" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".p999" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".mean_rate" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".m1_rate" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".m5_rate" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".m15_rate" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".rate_unit" )
                    .append( SEPARATOR ).append( entry.getKey() ).append( ".duration_unit" );
        }

        out.println( header.toString() );
    }

    private void archiveExistingLogFile()
    {
        String fileName = file.getName();
        SimpleDateFormat archivePrefix = new SimpleDateFormat( "yyyy_MM_dd_HH_mm_ss" );
        fileName = archivePrefix.format( new Date( file.lastModified() ) ) + fileName;
        File archiveFile = new File( file.getParentFile(), fileName );

        if ( !file.renameTo( archiveFile ) )
        {
            throw new IllegalStateException( "Could not move old metrics log to " + archiveFile );
        }
    }

    private void reportTimer( Timer timer, StringBuilder line )
    {
        final Snapshot snapshot = timer.getSnapshot();

        line.append( format( ",%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,calls/%s,%s", timer.getCount(),
                convertDuration( snapshot.getMax() ),
                convertDuration( snapshot.getMean() ),
                convertDuration( snapshot.getMin() ),
                convertDuration( snapshot.getStdDev() ),
                convertDuration( snapshot.getMedian() ),
                convertDuration( snapshot.get75thPercentile() ),
                convertDuration( snapshot.get95thPercentile() ),
                convertDuration( snapshot.get98thPercentile() ),
                convertDuration( snapshot.get99thPercentile() ),
                convertDuration( snapshot.get999thPercentile() ),
                convertRate( timer.getMeanRate() ),
                convertRate( timer.getOneMinuteRate() ),
                convertRate( timer.getFiveMinuteRate() ),
                convertRate( timer.getFifteenMinuteRate() ),
                getRateUnit(),
                getDurationUnit() ) );
    }

    private void reportMeter( Meter meter, StringBuilder line )
    {
        line.append( format( ",%d,%f,%f,%f,%f,events/%s", meter.getCount(),
                convertRate( meter.getMeanRate() ),
                convertRate( meter.getOneMinuteRate() ),
                convertRate( meter.getFiveMinuteRate() ),
                convertRate( meter.getFifteenMinuteRate() ),
                getRateUnit() ) );
    }

    private void reportHistogram( Histogram histogram, StringBuilder line )
    {
        final Snapshot snapshot = histogram.getSnapshot();

        line.append( format( ",%d,%d,%f,%d,%f,%f,%f,%f,%f,%f,%f",
                histogram.getCount(),
                snapshot.getMax(),
                snapshot.getMean(),
                snapshot.getMin(),
                snapshot.getStdDev(),
                snapshot.getMedian(),
                snapshot.get75thPercentile(),
                snapshot.get95thPercentile(),
                snapshot.get98thPercentile(),
                snapshot.get99thPercentile(),
                snapshot.get999thPercentile() ) );
    }

    private void reportCounter( Counter counter, StringBuilder line )
    {
        line.append( SEPARATOR ).append( counter.getCount() );
    }

    private void reportGauge( Gauge gauge, StringBuilder line )
    {
        line.append( SEPARATOR ).append( gauge.getValue().toString() );
    }
}
