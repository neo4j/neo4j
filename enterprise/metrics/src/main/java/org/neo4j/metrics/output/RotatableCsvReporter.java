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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.io.IOUtils;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;

public class RotatableCsvReporter extends ScheduledReporter
{
    private final Locale locale;
    private final Clock clock;
    private final File directory;
    private final Map<File,CsvRotatableWriter> writers;
    private final BiFunction<File,RotatingFileOutputStreamSupplier.RotationListener,RotatingFileOutputStreamSupplier> fileSupplierStreamCreator;

    RotatableCsvReporter( MetricRegistry registry, Locale locale, TimeUnit rateUnit, TimeUnit durationUnit, Clock clock,
            File directory,
            BiFunction<File,RotatingFileOutputStreamSupplier.RotationListener,RotatingFileOutputStreamSupplier> fileSupplierStreamCreator )
    {
        super( registry, "csv-reporter", MetricFilter.ALL, rateUnit, durationUnit );
        this.locale = locale;
        this.clock = clock;
        this.directory = directory;
        this.fileSupplierStreamCreator = fileSupplierStreamCreator;
        this.writers = new ConcurrentHashMap<>();
    }

    public static Builder forRegistry( MetricRegistry registry )
    {
        return new Builder( registry );
    }

    @Override
    public void stop()
    {
        super.stop();
        writers.values().forEach( CsvRotatableWriter::close );
    }

    @Override
    public void report( SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters,
            SortedMap<String,Histogram> histograms, SortedMap<String,Meter> meters, SortedMap<String,Timer> timers )
    {
        final long timestamp = TimeUnit.MILLISECONDS.toSeconds( clock.getTime() );

        for ( Map.Entry<String,Gauge> entry : gauges.entrySet() )
        {
            reportGauge( timestamp, entry.getKey(), entry.getValue() );
        }

        for ( Map.Entry<String,Counter> entry : counters.entrySet() )
        {
            reportCounter( timestamp, entry.getKey(), entry.getValue() );
        }

        for ( Map.Entry<String,Histogram> entry : histograms.entrySet() )
        {
            reportHistogram( timestamp, entry.getKey(), entry.getValue() );
        }

        for ( Map.Entry<String,Meter> entry : meters.entrySet() )
        {
            reportMeter( timestamp, entry.getKey(), entry.getValue() );
        }

        for ( Map.Entry<String,Timer> entry : timers.entrySet() )
        {
            reportTimer( timestamp, entry.getKey(), entry.getValue() );
        }
    }

    private void reportTimer( long timestamp, String name, Timer timer )
    {
        final Snapshot snapshot = timer.getSnapshot();

        report( timestamp, name,
                "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit",
                "%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,calls/%s,%s", timer.getCount(),
                convertDuration( snapshot.getMax() ), convertDuration( snapshot.getMean() ),
                convertDuration( snapshot.getMin() ), convertDuration( snapshot.getStdDev() ),
                convertDuration( snapshot.getMedian() ), convertDuration( snapshot.get75thPercentile() ),
                convertDuration( snapshot.get95thPercentile() ), convertDuration( snapshot.get98thPercentile() ),
                convertDuration( snapshot.get99thPercentile() ), convertDuration( snapshot.get999thPercentile() ),
                convertRate( timer.getMeanRate() ), convertRate( timer.getOneMinuteRate() ),
                convertRate( timer.getFiveMinuteRate() ), convertRate( timer.getFifteenMinuteRate() ), getRateUnit(),
                getDurationUnit() );
    }

    private void reportMeter( long timestamp, String name, Meter meter )
    {
        report( timestamp, name, "count,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit", "%d,%f,%f,%f,%f,events/%s",
                meter.getCount(), convertRate( meter.getMeanRate() ), convertRate( meter.getOneMinuteRate() ),
                convertRate( meter.getFiveMinuteRate() ), convertRate( meter.getFifteenMinuteRate() ), getRateUnit() );
    }

    private void reportHistogram( long timestamp, String name, Histogram histogram )
    {
        final Snapshot snapshot = histogram.getSnapshot();

        report( timestamp, name, "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999",
                "%d,%d,%f,%d,%f,%f,%f,%f,%f,%f,%f", histogram.getCount(), snapshot.getMax(), snapshot.getMean(),
                snapshot.getMin(), snapshot.getStdDev(), snapshot.getMedian(), snapshot.get75thPercentile(),
                snapshot.get95thPercentile(), snapshot.get98thPercentile(), snapshot.get99thPercentile(),
                snapshot.get999thPercentile() );
    }

    private void reportCounter( long timestamp, String name, Counter counter )
    {
        report( timestamp, name, "count", "%d", counter.getCount() );
    }

    private void reportGauge( long timestamp, String name, Gauge gauge )
    {
        report( timestamp, name, "value", "%s", gauge.getValue() );
    }

    private void report( long timestamp, String name, String header, String line, Object... values )
    {
        File file = new File( directory, name + ".csv" );
        CsvRotatableWriter csvRotatableWriter = writers.computeIfAbsent( file,
                new RotatingCsvWriterSupplier( header, fileSupplierStreamCreator, writers ) );
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        csvRotatableWriter.writeValues( locale, timestamp, line, values );
    }

    public static class Builder
    {
        private final MetricRegistry registry;
        private Locale locale;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private BiFunction<File,RotatingFileOutputStreamSupplier.RotationListener,RotatingFileOutputStreamSupplier>
                outputStreamSupplierFactory;

        private Builder( MetricRegistry registry )
        {
            this.registry = registry;
            this.locale = Locale.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.clock = Clock.defaultClock();
        }

        public Builder formatFor( Locale locale )
        {
            this.locale = locale;
            return this;
        }

        public Builder convertRatesTo( TimeUnit rateUnit )
        {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertDurationsTo( TimeUnit durationUnit )
        {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder outputStreamSupplierFactory(
                BiFunction<File,RotatingFileOutputStreamSupplier.RotationListener,RotatingFileOutputStreamSupplier> outputStreamSupplierFactory )
        {
            this.outputStreamSupplierFactory = outputStreamSupplierFactory;
            return this;
        }

        /**
         * Builds a {@link RotatableCsvReporter} with the given properties, writing {@code .csv} files to the
         * given directory.
         *
         * @param directory the directory in which the {@code .csv} files will be created
         * @return a {@link RotatableCsvReporter}
         */
        public RotatableCsvReporter build( File directory )
        {
            return new RotatableCsvReporter( registry, locale, rateUnit, durationUnit, clock, directory,
                    outputStreamSupplierFactory );
        }
    }

    private static class RotatingCsvWriterSupplier implements Function<File,CsvRotatableWriter>
    {
        private final String header;
        private final BiFunction<File,RotatingFileOutputStreamSupplier.RotationListener,RotatingFileOutputStreamSupplier> fileSupplierStreamCreator;
        private final Map<File,CsvRotatableWriter> writers;

        RotatingCsvWriterSupplier( String header,
                BiFunction<File,RotatingFileOutputStreamSupplier.RotationListener,RotatingFileOutputStreamSupplier> fileSupplierStreamCreator,
                Map<File,CsvRotatableWriter> writers )
        {
            this.header = header;
            this.fileSupplierStreamCreator = fileSupplierStreamCreator;
            this.writers = writers;
        }

        @Override
        public CsvRotatableWriter apply( File file )
        {
            RotatingFileOutputStreamSupplier outputStreamSupplier =
                    fileSupplierStreamCreator.apply( file, new HeaderWriterRotationListener() );
            PrintWriter printWriter = createWriter( outputStreamSupplier.get() );
            CsvRotatableWriter writer = new CsvRotatableWriter( printWriter, outputStreamSupplier );
            writeHeader( printWriter, header );
            return writer;
        }

        private class HeaderWriterRotationListener extends RotatingFileOutputStreamSupplier.RotationListener
        {

            @Override
            public void rotationCompleted( OutputStream out )
            {
                super.rotationCompleted( out );
                try ( PrintWriter writer = createWriter( out ) )
                {
                    writeHeader( writer, header );
                }
            }
        }
        private static PrintWriter createWriter( OutputStream outputStream )
        {
            return new PrintWriter( new OutputStreamWriter( outputStream, StandardCharsets.UTF_8 ) );
        }

        private static void writeHeader( PrintWriter printWriter, String header )
        {
            printWriter.println( "t," + header );
            printWriter.flush();
        }
    }

    private static class CsvRotatableWriter
    {
        private final PrintWriter printWriter;
        private final RotatingFileOutputStreamSupplier streamSupplier;

        CsvRotatableWriter( PrintWriter printWriter, RotatingFileOutputStreamSupplier streamSupplier )
        {
            this.printWriter = printWriter;
            this.streamSupplier = streamSupplier;
        }

        void close()
        {
            IOUtils.closeAllSilently( printWriter, streamSupplier );
        }

        synchronized void writeValues( Locale locale, long timestamp, String line, Object[] values )
        {
            streamSupplier.get();
            printWriter.printf( locale, String.format( locale, "%d,%s%n", timestamp, line ), values );
            printWriter.flush();
        }
    }
}
