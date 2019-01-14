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

import java.io.IOException;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static org.junit.Assert.assertEquals;

public class DefaultPageCursorTracerTest
{
    private PageSwapper swapper;
    private PageCursorTracer pageCursorTracer;
    private DefaultPageCacheTracer cacheTracer;

    @Before
    public void setUp()
    {
        cacheTracer = new DefaultPageCacheTracer();
        pageCursorTracer = createTracer();
        swapper = new DummyPageSwapper( "filename", (int) ByteUnit.kibiBytes( 8 ) );
    }

    @Test
    public void countPinsAndUnpins()
    {
        PinEvent pinEvent = pageCursorTracer.beginPin( true, 0, swapper );
        pinEvent.done();
        pinEvent = pageCursorTracer.beginPin( true, 0, swapper );

        assertEquals( 2, pageCursorTracer.pins() );
        assertEquals( 1, pageCursorTracer.unpins() );
    }

    @Test
    public void noHitForPinEventWithPageFault()
    {
        pinFaultAndHit();

        assertEquals( 1, pageCursorTracer.pins() );
        assertEquals( 1, pageCursorTracer.faults() );
        assertEquals( 0, pageCursorTracer.hits() );
    }

    @Test
    public void hitForPinEventWithoutPageFault()
    {
        pinAndHit();

        assertEquals( 1, pageCursorTracer.pins() );
        assertEquals( 1, pageCursorTracer.hits() );
    }

    @Test
    public void accumulateHitsReporting()
    {
        pinAndHit();
        pinAndHit();

        assertEquals( 2, pageCursorTracer.hits() );
        assertEquals( 2, pageCursorTracer.accumulatedHits() );

        pageCursorTracer.reportEvents();
        pinAndHit();

        assertEquals( 1, pageCursorTracer.hits() );
        assertEquals( 3, pageCursorTracer.accumulatedHits() );
    }

    @Test
    public void accumulatedFaultsReporting()
    {
        pinFaultAndHit();
        pinFaultAndHit();

        assertEquals( 2, pageCursorTracer.faults() );
        assertEquals( 2, pageCursorTracer.accumulatedFaults() );

        pageCursorTracer.reportEvents();
        pinFaultAndHit();
        pinFaultAndHit();

        assertEquals( 2,  pageCursorTracer.faults() );
        assertEquals( 4, pageCursorTracer.accumulatedFaults() );
        assertEquals( 0, pageCursorTracer.accumulatedHits() );
    }

    @Test
    public void countHitsOnlyForPinEventsWithoutPageFaults()
    {
        pinAndHit();
        pinAndHit();
        pinAndHit();
        pinFaultAndHit();
        pinFaultAndHit();
        pinAndHit();
        pinAndHit();

        assertEquals( 7, pageCursorTracer.pins() );
        assertEquals( 5, pageCursorTracer.hits() );
    }

    @Test
    public void countPageFaultsAndBytesRead()
    {
        PinEvent pinEvent = pageCursorTracer.beginPin( true, 0, swapper );
        {
            PageFaultEvent pageFaultEvent = pinEvent.beginPageFault();
            {
                pageFaultEvent.addBytesRead( 42 );
            }
            pageFaultEvent.done();
            pageFaultEvent = pinEvent.beginPageFault();
            {
                pageFaultEvent.addBytesRead( 42 );
            }
            pageFaultEvent.done();
        }
        pinEvent.done();

        assertEquals( 1, pageCursorTracer.pins() );
        assertEquals( 1, pageCursorTracer.unpins() );
        assertEquals( 2, pageCursorTracer.faults() );
        assertEquals( 84, pageCursorTracer.bytesRead() );
    }

    @Test
    public void countPageEvictions()
    {
        PinEvent pinEvent = pageCursorTracer.beginPin( true, 0, swapper );
        {
            PageFaultEvent faultEvent = pinEvent.beginPageFault();
            {
                EvictionEvent evictionEvent = faultEvent.beginEviction();
                evictionEvent.setFilePageId( 0 );
                evictionEvent.setCachePageId( 0 );
                evictionEvent.threwException( new IOException( "exception" ) );
                evictionEvent.close();
            }
            faultEvent.done();
        }
        pinEvent.done();

        assertEquals( 1, pageCursorTracer.pins() );
        assertEquals( 1, pageCursorTracer.unpins() );
        assertEquals( 1, pageCursorTracer.faults() );
        assertEquals( 1, pageCursorTracer.evictions() );
        assertEquals( 1, pageCursorTracer.evictionExceptions() );
    }

    @Test
    public void countFlushesAndBytesWritten()
    {
        PinEvent pinEvent = pageCursorTracer.beginPin( true, 0, swapper );
        {
            PageFaultEvent faultEvent = pinEvent.beginPageFault();
            {
                EvictionEvent evictionEvent = faultEvent.beginEviction();
                {
                    FlushEventOpportunity flushEventOpportunity = evictionEvent.flushEventOpportunity();
                    {
                        FlushEvent flushEvent = flushEventOpportunity.beginFlush( 0, 0, swapper );
                        flushEvent.addBytesWritten( 27 );
                        flushEvent.done();
                        FlushEvent flushEvent1 = flushEventOpportunity.beginFlush( 0, 1, swapper );
                        flushEvent1.addBytesWritten( 13 );
                        flushEvent1.done();
                    }
                }
                evictionEvent.close();
            }
            faultEvent.done();
        }
        pinEvent.done();

        assertEquals( 1, pageCursorTracer.pins() );
        assertEquals( 1, pageCursorTracer.unpins() );
        assertEquals( 1, pageCursorTracer.faults() );
        assertEquals( 1, pageCursorTracer.evictions() );
        assertEquals( 2, pageCursorTracer.flushes() );
        assertEquals( 40, pageCursorTracer.bytesWritten() );
    }

    @Test
    public void reportCountersToPageCursorTracer()
    {
        generateEventSet();
        pageCursorTracer.reportEvents();

        assertEquals( 1, cacheTracer.pins() );
        assertEquals( 1, cacheTracer.unpins() );
        assertEquals( 1, cacheTracer.faults() );
        assertEquals( 1, cacheTracer.evictions() );
        assertEquals( 1, cacheTracer.evictionExceptions() );
        assertEquals( 1, cacheTracer.flushes() );
        assertEquals( 10, cacheTracer.bytesWritten() );
        assertEquals( 150, cacheTracer.bytesRead() );

        generateEventSet();
        generateEventSet();
        pageCursorTracer.reportEvents();

        assertEquals( 3, cacheTracer.pins() );
        assertEquals( 3, cacheTracer.unpins() );
        assertEquals( 3, cacheTracer.faults() );
        assertEquals( 3, cacheTracer.evictions() );
        assertEquals( 3, cacheTracer.evictionExceptions() );
        assertEquals( 3, cacheTracer.flushes() );
        assertEquals( 30, cacheTracer.bytesWritten() );
        assertEquals( 450, cacheTracer.bytesRead() );
    }

    @Test
    public void shouldCalculateHitRatio()
    {
        assertEquals( 0d, pageCursorTracer.hitRatio(), 0.0001 );

        pinFaultAndHit();

        assertEquals( 0.0 / 1, pageCursorTracer.hitRatio(), 0.0001 );

        pinAndHit();

        assertEquals( 1.0 / 2, pageCursorTracer.hitRatio(), 0.0001 );

        pinFaultAndHit();
        pinFaultAndHit();
        pinFaultAndHit();
        pinAndHit();
        pinAndHit();

        assertEquals( 3.0 / 7, pageCursorTracer.hitRatio(), 0.0001 );

        pageCursorTracer.reportEvents();

        assertEquals( 3.0 / 7, cacheTracer.hitRatio(), 0.0001 );
    }

    private void generateEventSet()
    {
        PinEvent pinEvent = pageCursorTracer.beginPin( false, 0, swapper );
        {
            PageFaultEvent pageFaultEvent = pinEvent.beginPageFault();
            pageFaultEvent.addBytesRead( 150 );
            {
                EvictionEvent evictionEvent = pageFaultEvent.beginEviction();
                {
                    FlushEventOpportunity flushEventOpportunity = evictionEvent.flushEventOpportunity();
                    FlushEvent flushEvent = flushEventOpportunity.beginFlush( 0, 0, swapper );
                    flushEvent.addBytesWritten( 10 );
                    flushEvent.done();
                }
                evictionEvent.threwException( new IOException( "eviction exception" ) );
                evictionEvent.close();
            }
            pageFaultEvent.done();
        }
        pinEvent.done();
    }

    private PageCursorTracer createTracer()
    {
        DefaultPageCursorTracer pageCursorTracer = new DefaultPageCursorTracer();
        pageCursorTracer.init( cacheTracer );
        return pageCursorTracer;
    }

    private void pinAndHit()
    {
        PinEvent pinEvent = pageCursorTracer.beginPin( true, 0, swapper );
        pinEvent.hit();
        pinEvent.done();
    }

    private void pinFaultAndHit()
    {
        PinEvent pinEvent = pageCursorTracer.beginPin( true, 0, swapper );
        PageFaultEvent pageFaultEvent = pinEvent.beginPageFault();
        pinEvent.hit();
        pageFaultEvent.done();
        pinEvent.done();
    }
}
