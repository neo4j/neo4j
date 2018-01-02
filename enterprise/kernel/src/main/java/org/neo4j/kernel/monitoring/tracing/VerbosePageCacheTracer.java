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
package org.neo4j.kernel.monitoring.tracing;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.helpers.TimeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.logging.Log;
import org.neo4j.time.SystemNanoClock;

import static java.lang.String.format;
import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.flag;
import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.getInteger;

public class VerbosePageCacheTracer extends DefaultPageCacheTracer
{
    private static final boolean USE_RAW_REPORTING_UNITS =
            flag( VerbosePageCacheTracer.class, "reportInRawUnits", false );
    private static final int SPEED_REPORTING_TIME_THRESHOLD = getInteger( VerbosePageCacheTracer.class,
            "speedReportingThresholdSeconds", 10 );

    private final Log log;
    private final SystemNanoClock clock;
    private final AtomicLong flushedPages = new AtomicLong();
    private final AtomicLong flushBytesWritten = new AtomicLong();

    VerbosePageCacheTracer( Log log, SystemNanoClock clock )
    {
        this.log = log;
        this.clock = clock;
    }

    @Override
    public void mappedFile( File file )
    {
        log.info( format( "Map file: '%s'.", file.getName() ) );
        super.mappedFile( file );
    }

    @Override
    public void unmappedFile( File file )
    {
        log.info( format( "Unmap file: '%s'.", file.getName() ) );
        super.unmappedFile( file );
    }

    @Override
    public MajorFlushEvent beginCacheFlush()
    {
        log.info( "Start whole page cache flush." );
        return new PageCacheMajorFlushEvent( flushedPages.get(), flushBytesWritten.get(), clock.nanos() );
    }

    @Override
    public MajorFlushEvent beginFileFlush( PageSwapper swapper )
    {
        String fileName = swapper.file().getName();
        log.info( format( "Flushing file: '%s'.", fileName ) );
        return new FileFlushEvent( fileName, flushedPages.get(), flushBytesWritten.get(), clock.nanos() );
    }

    private static String nanosToString( long nanos )
    {
        if ( USE_RAW_REPORTING_UNITS )
        {
            return nanos + "ns";
        }
        return TimeUtil.nanosToString( nanos );
    }

    private static String flushSpeed( long bytesWrittenInTotal, long flushTimeNanos )
    {
        if ( USE_RAW_REPORTING_UNITS )
        {
            return bytesInNanoSeconds( bytesWrittenInTotal, flushTimeNanos );
        }
        long seconds = TimeUnit.NANOSECONDS.toSeconds( flushTimeNanos );
        if ( seconds > 0 )
        {
            return bytesToString( bytesWrittenInTotal / seconds ) + "/s";
        }
        else
        {
            return bytesInNanoSeconds( bytesWrittenInTotal, flushTimeNanos );
        }
    }

    private static String bytesInNanoSeconds( long bytesWrittenInTotal, long flushTimeNanos )
    {
        long bytesInNanoSecond = flushTimeNanos > 0 ? (bytesWrittenInTotal / flushTimeNanos) : bytesWrittenInTotal;
        return bytesInNanoSecond + "bytes/ns";
    }

    private static String bytesToString( long bytes )
    {
        if ( USE_RAW_REPORTING_UNITS )
        {
            return bytes + "bytes";
        }
        return ByteUnit.bytesToString( bytes );
    }

    private final FlushEvent flushEvent = new FlushEvent()
    {
        @Override
        public void addBytesWritten( long bytes )
        {
            bytesWritten.add( bytes );
            flushBytesWritten.getAndAdd( bytes );
        }

        @Override
        public void done()
        {
            flushes.increment();
        }

        @Override
        public void done( IOException exception )
        {
            done();
        }

        @Override
        public void addPagesFlushed( int pageCount )
        {
            flushedPages.getAndAdd( pageCount );
        }
    };

    private class FileFlushEvent implements MajorFlushEvent
    {
        private final long startTimeNanos;
        private final String fileName;
        private long flushesOnStart;
        private long bytesWrittenOnStart;

        FileFlushEvent( String fileName, long flushesOnStart, long bytesWrittenOnStart, long startTimeNanos )
        {
            this.fileName = fileName;
            this.flushesOnStart = flushesOnStart;
            this.bytesWrittenOnStart = bytesWrittenOnStart;
            this.startTimeNanos = startTimeNanos;
        }

        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return new VerboseFlushOpportunity( fileName, startTimeNanos, bytesWrittenOnStart );
        }

        @Override
        public void close()
        {
            long fileFlushNanos = clock.nanos() - startTimeNanos;
            long bytesWrittenInTotal = flushBytesWritten.get() - bytesWrittenOnStart;
            long flushedPagesInTotal = flushedPages.get() - flushesOnStart;
            log.info( "'%s' flush completed. Flushed %s in %d pages. Flush took: %s. Average speed: %s.",
                    fileName,
                    bytesToString( bytesWrittenInTotal ), flushedPagesInTotal,
                    nanosToString( fileFlushNanos ), flushSpeed( bytesWrittenInTotal, fileFlushNanos ) );
        }
    }

    private class PageCacheMajorFlushEvent implements MajorFlushEvent
    {
        private final long flushesOnStart;
        private final long bytesWrittenOnStart;
        private final long startTimeNanos;

        PageCacheMajorFlushEvent( long flushesOnStart, long bytesWrittenOnStart, long startTimeNanos )
        {
            this.flushesOnStart = flushesOnStart;
            this.bytesWrittenOnStart = bytesWrittenOnStart;
            this.startTimeNanos = startTimeNanos;
        }

        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return new VerboseFlushOpportunity( "Page Cache", startTimeNanos, bytesWrittenOnStart );
        }

        @Override
        public void close()
        {
            long pageCacheFlushNanos = clock.nanos() - startTimeNanos;
            long bytesWrittenInTotal = flushBytesWritten.get() - bytesWrittenOnStart;
            long flushedPagesInTotal = flushedPages.get() - flushesOnStart;
            log.info( "Page cache flush completed. Flushed %s in %d pages. Flush took: %s. Average speed: %s.",
                    bytesToString( bytesWrittenInTotal ), flushedPagesInTotal,
                    nanosToString(pageCacheFlushNanos),
                    flushSpeed( bytesWrittenInTotal, pageCacheFlushNanos ) );
        }
    }

    private class VerboseFlushOpportunity implements FlushEventOpportunity
    {
        private final String fileName;
        private long lastReportingTime;
        private long lastReportedBytesWritten;

        VerboseFlushOpportunity( String fileName, long nanoStartTime, long bytesWrittenOnStart )
        {
            this.fileName = fileName;
            this.lastReportingTime = nanoStartTime;
            this.lastReportedBytesWritten = bytesWrittenOnStart;
        }

        @Override
        public FlushEvent beginFlush( long filePageId, long cachePageId, PageSwapper swapper )
        {
            long now = clock.nanos();
            long opportunityIntervalNanos = now - lastReportingTime;
            if ( TimeUnit.NANOSECONDS.toSeconds( opportunityIntervalNanos ) > SPEED_REPORTING_TIME_THRESHOLD )
            {
                long writtenBytes = flushBytesWritten.get();
                log.info( format("'%s' flushing speed: %s.", fileName,
                        flushSpeed( writtenBytes - lastReportedBytesWritten, opportunityIntervalNanos ) ) );
                lastReportingTime = now;
                lastReportedBytesWritten = writtenBytes;
            }
            return flushEvent;
        }
    }
}
