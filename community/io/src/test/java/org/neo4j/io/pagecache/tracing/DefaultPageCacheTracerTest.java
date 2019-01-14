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

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageSwapper;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DefaultPageCacheTracerTest
{
    private PageCacheTracer tracer;
    private PageSwapper swapper;

    @Before
    public void setUp()
    {
        tracer = new DefaultPageCacheTracer();
        swapper = new DummyPageSwapper( "filename", (int) ByteUnit.kibiBytes( 8 ) );
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

        assertCounts( 0, 0, 0, 0, 4, 2, 3, 0, 36, 0, 0,  0d);
    }

    @Test
    public void mustCountFileMappingAndUnmapping()
    {
        tracer.mappedFile( new File( "a" ) );

        assertCounts( 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,  0d );

        tracer.unmappedFile( new File( "a" ) );

        assertCounts( 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1,  0d );
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
    public void shouldCalculateHitRatio()
    {
        assertThat( "hitRation", tracer.hitRatio(), closeTo( 0d, 0.0001 ) );
        tracer.hits( 3 );
        tracer.faults( 7 );
        assertThat( "hitRation", tracer.hitRatio(), closeTo( 3.0 / 10, 0.0001 ) );
    }

    @Test
    public void usageRatio()
    {
        assertThat( tracer.usageRatio(), is( Double.NaN ) );
        tracer.maxPages( 10 );
        assertThat( tracer.usageRatio(), closeTo( 0d, 0.0001 ) );
        tracer.faults( 5 );
        assertThat( tracer.usageRatio(), closeTo( 0.5, 0.0001 ) );
        tracer.faults( 5 );
        tracer.evictions( 5 );
        assertThat( tracer.usageRatio(), closeTo( 0.5, 0.0001 ) );
        tracer.faults( 5 );
        assertThat( tracer.usageRatio(), closeTo( 1d, 0.0001 ) );
    }

    private void assertCounts( long pins, long unpins, long hits, long faults, long evictions, long evictionExceptions,
            long flushes, long bytesRead, long bytesWritten, long filesMapped, long filesUnmapped, double hitRatio )
    {
        assertThat( "pins", tracer.pins(), is( pins ) );
        assertThat( "unpins", tracer.unpins(), is( unpins ) );
        assertThat( "hits", tracer.hits(), is( hits ) );
        assertThat( "faults", tracer.faults(), is( faults ) );
        assertThat( "evictions", tracer.evictions(), is( evictions ) );
        assertThat( "evictionExceptions", tracer.evictionExceptions(), is( evictionExceptions ) );
        assertThat( "flushes", tracer.flushes(), is( flushes ) );
        assertThat( "bytesRead", tracer.bytesRead(), is( bytesRead ) );
        assertThat( "bytesWritten", tracer.bytesWritten(), is( bytesWritten ) );
        assertThat( "filesMapped", tracer.filesMapped(), is( filesMapped ) );
        assertThat( "filesUnmapped", tracer.filesUnmapped(), is( filesUnmapped ) );
        assertThat( "hitRatio", tracer.hitRatio(), closeTo( hitRatio, 0.0001 ) );
    }
}
