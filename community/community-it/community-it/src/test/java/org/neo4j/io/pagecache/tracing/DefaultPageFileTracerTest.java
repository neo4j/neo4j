/*
 * Copyright (c) "Neo4j"
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
import org.mockito.Mockito;

import java.io.IOException;

import org.neo4j.io.ByteUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultPageFileTracerTest
{
    private PageCacheTracer pageCacheTracer;
    private DummyPageSwapper swapper;
    private PageFileSwapperTracer swapperTracer;

    @BeforeEach
    public void setUp()
    {
        pageCacheTracer = new DefaultPageCacheTracer();
        swapperTracer = pageCacheTracer.createFileSwapperTracer();
        swapper = new DummyPageSwapper( "filename", (int) ByteUnit.kibiBytes( 8 ), swapperTracer );
    }

    @Test
    void pageCursorEventReportPinUnpinEventsToFileTracer()
    {
        var cursorTracer = pageCacheTracer.createPageCursorTracer( "pageCursorEventReportPinUnpinEventsToFileTracer" );
        PinEvent pinEvent = cursorTracer.beginPin( false, 1, swapper );
        pinEvent.done();

        assertEquals( 1, swapperTracer.pins() );
        assertEquals( 0, swapperTracer.hits() );
        assertEquals( 1, swapperTracer.unpins() );
    }

    @Test
    void pageCursorEventReportPinUnpinHitEventsToFileTracer()
    {
        var cursorTracer = pageCacheTracer.createPageCursorTracer( "pageCursorEventReportPinUnpinHitEventsToFileTracer" );
        PinEvent pinEvent = cursorTracer.beginPin( false, 1, swapper );
        pinEvent.hit();
        pinEvent.done();

        assertEquals( 1, swapperTracer.pins() );
        assertEquals( 1, swapperTracer.hits() );
        assertEquals( 1, swapperTracer.unpins() );
    }

    @Test
    void pageCursorEventReportPageFaultAndBytesReadEventsToFileTracer()
    {
        var cursorTracer = pageCacheTracer.createPageCursorTracer( "pageCursorEventReportPageFaultAndBytesReadEventsToFileTracer" );
        PinEvent pinEvent = cursorTracer.beginPin( false, 1, swapper );

        PageFaultEvent pageFaultEvent = pinEvent.beginPageFault( 1, swapper );
        pageFaultEvent.addBytesRead( 123 );
        pageFaultEvent.done();

        pinEvent.done();

        assertEquals( 1, swapperTracer.pins() );
        assertEquals( 1, swapperTracer.unpins() );
        assertEquals( 1, swapperTracer.faults() );
        assertEquals( 123, swapperTracer.bytesRead() );
    }

    @Test
    void pageCursorEventReportEvictionExceptionEventsToFileTracer()
    {
        var cursorTracer = pageCacheTracer.createPageCursorTracer( "pageCursorEventReportEvictionExceptionEventsToFileTracer" );
        PinEvent pinEvent = cursorTracer.beginPin( false, 1, swapper );

        PageFaultEvent pageFaultEvent = pinEvent.beginPageFault( 1, swapper );
        try ( EvictionEvent evictionEvent = pageFaultEvent.beginEviction( 2 ) )
        {
            evictionEvent.setSwapper( swapper );
            evictionEvent.threwException( new IOException() );
        }
        pageFaultEvent.done();

        pinEvent.done();

        assertEquals( 1, swapperTracer.pins() );
        assertEquals( 1, swapperTracer.unpins() );
        assertEquals( 1, swapperTracer.faults() );
        assertEquals( 1, swapperTracer.evictionExceptions() );
    }

    @Test
    void pageCursorEventReportEvictionEventsToFileTracer()
    {
        var cursorTracer = pageCacheTracer.createPageCursorTracer( "pageCursorEventReportEvictionEventsToFileTracer" );
        PinEvent pinEvent = cursorTracer.beginPin( false, 1, swapper );

        PageFaultEvent pageFaultEvent = pinEvent.beginPageFault( 1, swapper );
        try ( EvictionEvent evictionEvent = pageFaultEvent.beginEviction( 2 ) )
        {
            evictionEvent.setSwapper( swapper );
            FlushEvent flushEvent = evictionEvent.beginFlush( 1, swapper, Mockito.mock( PageReferenceTranslator.class ) );
            flushEvent.addPagesMerged( 11 );
            flushEvent.addPagesFlushed( 22 );
            flushEvent.addBytesWritten( 33 );
            flushEvent.done();
        }
        pageFaultEvent.done();

        pinEvent.done();

        assertEquals( 1, swapperTracer.pins() );
        assertEquals( 1, swapperTracer.unpins() );
        assertEquals( 1, swapperTracer.faults() );
        assertEquals( 1, swapperTracer.evictions() );
        assertEquals( 11, swapperTracer.merges() );
        assertEquals( 22, swapperTracer.flushes() );
        assertEquals( 33, swapperTracer.bytesWritten() );
        assertEquals( 0, swapperTracer.evictionExceptions() );
    }
}
