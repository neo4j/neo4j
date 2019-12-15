/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageSwapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DefaultPageCacheTracerTest
{
    private PageCacheTracer tracer;
    private PageSwapper swapper;

    @BeforeEach
    public void setUp()
    {
        tracer = new DefaultPageCacheTracer();
        swapper = new DummyPageSwapper( "filename", (int) ByteUnit.kibiBytes( 8 ) );
    }

    @Test
    void independentCursorTracers()
    {
        var first = tracer.createPageCursorTracer( "first" );
        var second = tracer.createPageCursorTracer( "second" );
        var third = tracer.createPageCursorTracer( "third" );

        assertEquals( "first", first.getTag() );
        assertEquals( "second", second.getTag() );
        assertEquals( "third", third.getTag() );

        first.beginPin( false, 1, swapper ).done();
        first.beginPin( false, 1, swapper ).done();

        assertEquals( 2, first.pins() );
        assertEquals( 0, second.pins() );
        assertEquals( 0, third.pins() );

        PinEvent secondPin = second.beginPin( true, 2, swapper );
        secondPin.beginPageFault().done();

        assertEquals( 2, first.pins() );
        assertEquals( 1, second.pins() );
        assertEquals( 0, third.pins() );

        assertEquals( 0, first.faults() );
        assertEquals( 1, second.faults() );
        assertEquals( 0, third.faults() );
    }

    @Test
    void mustCountEvictions()
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

        assertCounts( 0, 0, 0, 0, 4, 2, 3, 0, 36, 0, 0,  0d);
    }

    @Test
    void mustCountFileMappingAndUnmapping()
    {
        tracer.mappedFile( new File( "a" ) );

        assertCounts( 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,  0d );

        tracer.unmappedFile( new File( "a" ) );

        assertCounts( 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1,  0d );
    }

    @Test
    void mustCountFlushes()
    {
        try ( MajorFlushEvent cacheFlush = tracer.beginCacheFlush() )
        {
            cacheFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            cacheFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            cacheFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
        }

        assertCounts( 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0d );

        try ( MajorFlushEvent fileFlush = tracer.beginFileFlush( swapper ) )
        {
            fileFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            fileFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            fileFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
        }

        assertCounts( 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0d );
    }

    @Test
    void shouldCalculateHitRatio()
    {
        assertThat( tracer.hitRatio() ).as( "hitRation" ).isCloseTo( 0d, within( 0.0001 ) );
        tracer.hits( 3 );
        tracer.faults( 7 );
        assertThat( tracer.hitRatio() ).as( "hitRation" ).isCloseTo( 3.0 / 10, within( 0.0001 ) );
    }

    @Test
    void usageRatio()
    {
        assertThat( tracer.usageRatio() ).isEqualTo( 0 );
        tracer.maxPages( 10 );
        assertThat( tracer.usageRatio() ).isCloseTo( 0d, within( 0.0001 ) );
        tracer.faults( 5 );
        assertThat( tracer.usageRatio() ).isCloseTo( 0.5, within( 0.0001 ) );
        tracer.faults( 5 );
        tracer.evictions( 5 );
        assertThat( tracer.usageRatio() ).isCloseTo( 0.5, within( 0.0001 ) );
        tracer.faults( 5 );
        assertThat( tracer.usageRatio() ).isCloseTo( 1d, within( 0.0001 ) );

        tracer.evictions( 500 );
        assertThat( tracer.usageRatio() ).isCloseTo( 0, within( 0.0001 ) );
    }

    private void assertCounts( long pins, long unpins, long hits, long faults, long evictions, long evictionExceptions,
            long flushes, long bytesRead, long bytesWritten, long filesMapped, long filesUnmapped, double hitRatio )
    {
        assertThat( tracer.pins() ).as( "pins" ).isEqualTo( pins );
        assertThat( tracer.unpins() ).as( "unpins" ).isEqualTo( unpins );
        assertThat( tracer.hits() ).as( "hits" ).isEqualTo( hits );
        assertThat( tracer.faults() ).as( "faults" ).isEqualTo( faults );
        assertThat( tracer.evictions() ).as( "evictions" ).isEqualTo( evictions );
        assertThat( tracer.evictionExceptions() ).as( "evictionExceptions" ).isEqualTo( evictionExceptions );
        assertThat( tracer.flushes() ).as( "flushes" ).isEqualTo( flushes );
        assertThat( tracer.bytesRead() ).as( "bytesRead" ).isEqualTo( bytesRead );
        assertThat( tracer.bytesWritten() ).as( "bytesWritten" ).isEqualTo( bytesWritten );
        assertThat( tracer.filesMapped() ).as( "filesMapped" ).isEqualTo( filesMapped );
        assertThat( tracer.filesUnmapped() ).as( "filesUnmapped" ).isEqualTo( filesUnmapped );
        assertThat( tracer.hitRatio() ).as( "hitRatio" ).isCloseTo( hitRatio, within( 0.0001 ) );
    }
}
