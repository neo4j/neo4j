/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.pagecache.tracing;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageSwapper;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public abstract class PageCacheTracerTest
{
    private PageCacheTracer tracer;
    private PageSwapper swapper;

    @Before
    public void setUp()
    {
        tracer = createTracer();
        swapper = new DummyPageSwapper( "filename" );
    }

    protected abstract PageCacheTracer createTracer();

    @Test
    public void mustCountPinsAndUnpins()
    {
        PinEvent pinEvent = tracer.beginPin( true, 0, swapper );
        pinEvent.done();

        // We don't particularly care whether the counts are incremented on begin or close

        assertCounts( 1, 1, 0, 0, 0, 0, 0, 0, 0, 0 );
    }

    private void assertCounts( long pins, long unpins, long faults, long evictions, long evictionExceptions,
                               long flushes, long bytesRead, long bytesWritten, long filesMapped, long filesUnmapped )
    {
        assertThat( "countPins", tracer.countPins(), is( pins ) );
        assertThat( "countUnpins", tracer.countUnpins(), is( unpins ) );
        assertThat( "countFaults", tracer.countFaults(), is( faults ) );
        assertThat( "countEvictions", tracer.countEvictions(), is( evictions ) );
        assertThat( "countEvictionExceptions", tracer.countEvictionExceptions(), is( evictionExceptions ) );
        assertThat( "countFlushes", tracer.countFlushes(), is( flushes ) );
        assertThat( "countBytesRead", tracer.countBytesRead(), is( bytesRead ) );
        assertThat( "countBytesWritten", tracer.countBytesWritten(), is( bytesWritten ) );
        assertThat( "countFilesMapped", tracer.countFilesMapped(), is( filesMapped ) );
        assertThat( "countFilesUnmapped", tracer.countFilesUnmapped(), is( filesUnmapped ) );
    }

    @Test
    public void mustCountPageFaults()
    {
        PinEvent pinEvent = tracer.beginPin( true, 0, swapper );
        PageFaultEvent pageFaultEvent = pinEvent.beginPageFault();
        pageFaultEvent.addBytesRead( 42 );
        pageFaultEvent.done();
        pageFaultEvent = pinEvent.beginPageFault();
        pageFaultEvent.addBytesRead( 42 );
        pageFaultEvent.done();
        pinEvent.done();

        assertCounts( 1, 1, 2, 0, 0, 0, 84, 0, 0, 0 );
    }

    @Test
    public void mustCountEvictions()
    {
        try ( EvictionRunEvent evictionRunEvent = tracer.beginPageEvictions( 2 ) )
        {
            try ( EvictionEvent evictionEvent = evictionRunEvent.beginEviction() )
            {
                FlushEvent flushEvent = evictionEvent.flushEventOpportunity().beginFlush( 0, 0, swapper );
                flushEvent.addBytesWritten( 12 );
                flushEvent.done();
            }

            try ( EvictionEvent evictionEvent = evictionRunEvent.beginEviction() )
            {
                FlushEvent flushEvent = evictionEvent.flushEventOpportunity().beginFlush( 0, 0, swapper );
                flushEvent.addBytesWritten( 12 );
                flushEvent.done();
                evictionEvent.threwException( new IOException() );
            }

            try ( EvictionEvent evictionEvent = evictionRunEvent.beginEviction() )
            {
                FlushEvent flushEvent = evictionEvent.flushEventOpportunity().beginFlush( 0, 0, swapper );
                flushEvent.addBytesWritten( 12 );
                flushEvent.done();
                evictionEvent.threwException( new IOException() );
            }

            evictionRunEvent.beginEviction().close();
        }

        assertCounts( 0, 0, 0, 4, 2, 3, 0, 36, 0, 0 );
    }

    @Test
    public void mustCountFileMappingAndUnmapping()
    {
        tracer.mappedFile( new File( "a" ) );

        assertCounts( 0, 0, 0, 0, 0, 0, 0, 0, 1, 0 );

        tracer.unmappedFile( new File( "a" ) );

        assertCounts( 0, 0, 0, 0, 0, 0, 0, 0, 1, 1 );
    }

    @Test
    public void mustCountFlushes()
    {
        try ( MajorFlushEvent cacheFlush = tracer.beginCacheFlush() )
        {
            cacheFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            cacheFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            cacheFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
        }

        assertCounts( 0, 0, 0, 0, 0, 3, 0, 0, 0, 0 );

        try ( MajorFlushEvent fileFlush = tracer.beginFileFlush( swapper ) )
        {
            fileFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            fileFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            fileFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
        }

        assertCounts( 0, 0, 0, 0, 0, 6, 0, 0, 0, 0 );
    }
}
