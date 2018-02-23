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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.tracing.DummyPageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

public class VerbosePageCacheTracerTest
{
    private AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private Log log = logProvider.getLog( getClass() );
    private FakeClock clock = Clocks.fakeClock();

    @Test
    public void traceFileMap()
    {
        VerbosePageCacheTracer tracer = createTracer();
        tracer.mappedFile( new File( "mapFile" ) );
        logProvider.assertContainsMessageContaining( "Map file: 'mapFile'." );
    }

    @Test
    public void traceUnmapFile()
    {
        VerbosePageCacheTracer tracer = createTracer();
        tracer.unmappedFile( new File( "unmapFile" ) );
        logProvider.assertContainsMessageContaining( "Unmap file: 'unmapFile'." );
    }

    @Test
    public void traceSinglePageCacheFlush()
    {
        VerbosePageCacheTracer tracer = createTracer();
        try ( MajorFlushEvent majorFlushEvent = tracer.beginCacheFlush() )
        {
            FlushEventOpportunity flushEventOpportunity = majorFlushEvent.flushEventOpportunity();
            FlushEvent flushEvent = flushEventOpportunity.beginFlush( 1, 2, new DummyPageSwapper( "testFile", 1 ) );
            flushEvent.addBytesWritten( 2 );
            flushEvent.addPagesFlushed( 7 );
            flushEvent.done();
        }
        logProvider.assertContainsMessageContaining( "Start whole page cache flush." );
        logProvider.assertLogStringContains( "Page cache flush completed. Flushed 2B in 7 pages. Flush took: 0ns. " +
                "Average speed: 2bytes/ns." );
    }

    @Test
    public void evictionDoesNotInfluenceFlushNumbers()
    {
        VerbosePageCacheTracer tracer = createTracer();
        try ( MajorFlushEvent majorFlushEvent = tracer.beginCacheFlush() )
        {
            FlushEventOpportunity flushEventOpportunity = majorFlushEvent.flushEventOpportunity();
            FlushEvent flushEvent = flushEventOpportunity.beginFlush( 1, 2, new DummyPageSwapper( "testFile", 1 ) );
            clock.forward( 2, TimeUnit.MILLISECONDS );

            try ( EvictionRunEvent evictionRunEvent = tracer.beginPageEvictions( 5 ) )
            {
                try ( EvictionEvent evictionEvent = evictionRunEvent.beginEviction() )
                {
                    FlushEventOpportunity evictionEventOpportunity = evictionEvent.flushEventOpportunity();
                    FlushEvent evictionFlush = evictionEventOpportunity.beginFlush( 2, 3,
                            new DummyPageSwapper( "evictionFile", 1 ) );
                    evictionFlush.addPagesFlushed( 10 );
                    evictionFlush.addPagesFlushed( 100 );
                }
            }
            flushEvent.addBytesWritten( 2 );
            flushEvent.addPagesFlushed( 7 );
            flushEvent.done();
        }
        logProvider.assertContainsMessageContaining( "Start whole page cache flush." );
        logProvider.assertLogStringContains( "Page cache flush completed. Flushed 2B in 7 pages. Flush took: 2ms. " +
                "Average speed: 0bytes/ns." );
    }

    @Test
    public void traceFileFlush()
    {
        VerbosePageCacheTracer tracer = createTracer();
        DummyPageSwapper swapper = new DummyPageSwapper( "fileToFlush", 1 );
        try ( MajorFlushEvent fileToFlush = tracer.beginFileFlush( swapper ) )
        {
            FlushEventOpportunity flushEventOpportunity = fileToFlush.flushEventOpportunity();
            FlushEvent flushEvent = flushEventOpportunity.beginFlush( 1, 2, swapper );
            flushEvent.addPagesFlushed( 100 );
            flushEvent.addBytesWritten( ByteUnit.ONE_MEBI_BYTE );
            flushEvent.done();
            clock.forward( 1, TimeUnit.SECONDS );
            FlushEvent flushEvent2 = flushEventOpportunity.beginFlush( 1, 2, swapper );
            flushEvent2.addPagesFlushed( 10 );
            flushEvent2.addBytesWritten( ByteUnit.ONE_MEBI_BYTE );
            flushEvent2.done();
        }
        logProvider.assertContainsMessageContaining( "Flushing file: 'fileToFlush'." );
        logProvider.assertLogStringContains( "'fileToFlush' flush completed. Flushed 2.000MiB in 110 pages. Flush took: 1s. Average speed: 2.000MiB/s." );
    }

    private VerbosePageCacheTracer createTracer()
    {
        return new VerbosePageCacheTracer( log, clock );
    }
}
