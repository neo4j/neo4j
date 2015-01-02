/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.io.pagecache.monitoring;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageSwapper;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public abstract class PageCacheMonitorTest
{
    private PageCacheMonitor monitor;
    private PageSwapper swapper;

    @Before
    public void setUp()
    {
        monitor = createMonitor();
        swapper = new DummyPageSwapper( "filename" );
    }

    protected abstract PageCacheMonitor createMonitor();

    @Test
    public void mustCountPinsAndUnpins()
    {
        PinEvent pinEvent = monitor.beginPin( true, 0, swapper );
        pinEvent.done();

        // We don't particularly care whether the counts are incremented on begin or close

        assertCounts( 1, 1, 0, 0, 0, 0, 0, 0, 0, 0 );
    }

    private void assertCounts( long pins, long unpins, long faults, long evictions, long evictionExceptions,
                               long flushes, long bytesRead, long bytesWritten, long filesMapped, long filesUnmapped )
    {
        assertThat( "countPins", monitor.countPins(), is( pins ) );
        assertThat( "countUnpins", monitor.countUnpins(), is( unpins ) );
        assertThat( "countFaults", monitor.countFaults(), is( faults ) );
        assertThat( "countEvictions", monitor.countEvictions(), is( evictions ) );
        assertThat( "countEvictionExceptions", monitor.countEvictionExceptions(), is( evictionExceptions ) );
        assertThat( "countFlushes", monitor.countFlushes(), is( flushes ) );
        assertThat( "countBytesRead", monitor.countBytesRead(), is( bytesRead ) );
        assertThat( "countBytesWritten", monitor.countBytesWritten(), is( bytesWritten ) );
        assertThat( "countFilesMapped", monitor.countFilesMapped(), is( filesMapped ) );
        assertThat( "countFilesUnmapped", monitor.countFilesUnmapped(), is( filesUnmapped ) );
    }

    @Test
    public void mustCountPageFaults()
    {
        PinEvent pinEvent = monitor.beginPin( true, 0, swapper );
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
        try ( EvictionRunEvent evictionRunEvent = monitor.beginPageEvictions( 2 ) )
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
        monitor.mappedFile( new File( "a" ) );

        assertCounts( 0, 0, 0, 0, 0, 0, 0, 0, 1, 0 );

        monitor.unmappedFile( new File( "a" ) );

        assertCounts( 0, 0, 0, 0, 0, 0, 0, 0, 1, 1 );
    }

    @Test
    public void mustCountFlushes()
    {
        try ( MajorFlushEvent cacheFlush = monitor.beginCacheFlush() )
        {
            cacheFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            cacheFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            cacheFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
        }

        assertCounts( 0, 0, 0, 0, 0, 3, 0, 0, 0, 0 );

        try ( MajorFlushEvent fileFlush = monitor.beginFileFlush( swapper ) )
        {
            fileFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            fileFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
            fileFlush.flushEventOpportunity().beginFlush( 0, 0, swapper ).done();
        }

        assertCounts( 0, 0, 0, 0, 0, 6, 0, 0, 0, 0 );
    }
}
