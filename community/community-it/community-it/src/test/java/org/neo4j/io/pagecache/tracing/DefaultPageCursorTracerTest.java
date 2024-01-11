/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.io.pagecache.tracing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.cursor.CursorStatisticSnapshot;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

class DefaultPageCursorTracerTest {
    private static final String TEST_TRACER = "testTracer";
    private PageSwapper swapper;
    private PageCursorTracer pageCursorTracer;
    private DefaultPageCacheTracer cacheTracer;
    private PageReferenceTranslator referenceTranslator;

    @BeforeEach
    void setUp() {
        cacheTracer = new DefaultPageCacheTracer();
        pageCursorTracer = createTracer();
        swapper = new DummyPageSwapper("filename", (int) ByteUnit.kibiBytes(8));
        referenceTranslator = mock(PageReferenceTranslator.class);
    }

    @Test
    void countClosedAndOpenCursors() {
        assertEquals(0, cacheTracer.closedCursors());
        assertEquals(0, cacheTracer.openedCursors());

        pageCursorTracer.openCursor();
        pageCursorTracer.openCursor();
        pageCursorTracer.openCursor();
        pageCursorTracer.openCursor();

        pageCursorTracer.closeCursor();
        pageCursorTracer.closeCursor();
        pageCursorTracer.closeCursor();

        assertEquals(3, pageCursorTracer.closedCursors());
        assertEquals(4, pageCursorTracer.openedCursors());

        assertEquals(0, cacheTracer.closedCursors());
        assertEquals(0, cacheTracer.openedCursors());

        pageCursorTracer.close();

        assertEquals(3, cacheTracer.closedCursors());
        assertEquals(4, cacheTracer.openedCursors());
    }

    @Test
    void countPinsAndUnpins() {
        pageCursorTracer.beginPin(true, 0, swapper).close();
        pageCursorTracer.unpin(0, swapper);
        pageCursorTracer.beginPin(true, 0, swapper).close();

        assertEquals(2, pageCursorTracer.pins());
        assertEquals(1, pageCursorTracer.unpins());
    }

    @Test
    void countHits() {
        pinAndHit();

        assertEquals(1, pageCursorTracer.pins());
        assertEquals(1, pageCursorTracer.hits());
        assertEquals(1, pageCursorTracer.unpins());
    }

    @Test
    void countPageFaultsAndBytesRead() {
        try (var pinEvent = pageCursorTracer.beginPin(true, 0, swapper)) {
            try (var pageFaultEvent = pinEvent.beginPageFault(1, swapper)) {
                pageFaultEvent.addBytesRead(42);
            }
            try (var pageFaultEvent = pinEvent.beginPageFault(3, swapper)) {
                pageFaultEvent.addBytesRead(42);
            }
        }
        pageCursorTracer.unpin(0, swapper);

        assertEquals(1, pageCursorTracer.pins());
        assertEquals(1, pageCursorTracer.unpins());
        assertEquals(2, pageCursorTracer.faults());
        assertEquals(84, pageCursorTracer.bytesRead());
    }

    @Test
    void countNoFaults() {
        try (var pinEvent = pageCursorTracer.beginPin(true, 0, swapper)) {
            pinEvent.noFault();
        }
        pageCursorTracer.unpin(0, swapper);

        assertEquals(1, pageCursorTracer.pins());
        assertEquals(1, pageCursorTracer.noFaults());
        assertEquals(1, pageCursorTracer.unpins());
    }

    @Test
    void countFailedFaults() {
        try (var pinEvent = pageCursorTracer.beginPin(true, 0, swapper)) {
            try (var pageFaultEvent = pinEvent.beginPageFault(1, swapper)) {
                pageFaultEvent.setException(null);
            }
        }

        assertEquals(1, pageCursorTracer.pins());
        assertEquals(1, pageCursorTracer.faults());
        assertEquals(1, pageCursorTracer.failedFaults());
    }

    @Test
    void countPageEvictions() {
        try (var pinEvent = pageCursorTracer.beginPin(true, 0, swapper)) {
            try (var faultEvent = pinEvent.beginPageFault(1, swapper)) {
                EvictionEvent evictionEvent = faultEvent.beginEviction(0);
                evictionEvent.setSwapper(swapper);
                evictionEvent.setFilePageId(0);
                evictionEvent.setException(new IOException("exception"));
                evictionEvent.close();
            }
        }
        pageCursorTracer.unpin(0, swapper);

        assertEquals(1, pageCursorTracer.pins());
        assertEquals(1, pageCursorTracer.unpins());
        assertEquals(1, pageCursorTracer.faults());
        assertEquals(1, pageCursorTracer.evictions());
        assertEquals(1, pageCursorTracer.evictionExceptions());
    }

    @Test
    void countFlushesAndBytesWritten() {
        try (var pinEvent = pageCursorTracer.beginPin(true, 0, swapper)) {
            try (PinPageFaultEvent faultEvent = pinEvent.beginPageFault(3, swapper)) {
                EvictionEvent evictionEvent = faultEvent.beginEviction(0);
                {
                    evictionEvent.setSwapper(swapper);
                    try (var flushEvent = evictionEvent.beginFlush(0, swapper, referenceTranslator)) {
                        flushEvent.addBytesWritten(27);
                        flushEvent.addPagesMerged(10);
                    }
                    try (var flushEvent1 = evictionEvent.beginFlush(1, swapper, referenceTranslator)) {
                        flushEvent1.addBytesWritten(13);
                        flushEvent1.addPagesFlushed(2);
                        flushEvent1.addPagesMerged(2);
                    }
                }
                evictionEvent.close();
            }
        }
        pageCursorTracer.unpin(0, swapper);

        assertEquals(1, pageCursorTracer.pins());
        assertEquals(1, pageCursorTracer.unpins());
        assertEquals(1, pageCursorTracer.faults());
        assertEquals(1, pageCursorTracer.evictions());
        assertEquals(2, pageCursorTracer.flushes());
        assertEquals(12, pageCursorTracer.merges());
        assertEquals(40, pageCursorTracer.bytesWritten());
    }

    @Test
    void reportCountersToPageCursorTracer() {
        generateEventSet();
        pageCursorTracer.reportEvents();

        assertEquals(1, cacheTracer.pins());
        assertEquals(1, cacheTracer.unpins());
        assertEquals(1, cacheTracer.faults());
        assertEquals(1, cacheTracer.hits());
        assertEquals(1, cacheTracer.noFaults());
        assertEquals(1, cacheTracer.failedFaults());
        assertEquals(1, cacheTracer.evictions());
        assertEquals(1, cacheTracer.cooperativeEvictions());
        assertEquals(1, cacheTracer.evictionExceptions());
        assertEquals(1, cacheTracer.flushes());
        assertEquals(1, cacheTracer.merges());
        assertEquals(10, cacheTracer.bytesWritten());
        assertEquals(150, cacheTracer.bytesRead());

        generateEventSet();
        generateEventSet();
        pageCursorTracer.reportEvents();

        assertEquals(3, cacheTracer.pins());
        assertEquals(3, cacheTracer.unpins());
        assertEquals(3, cacheTracer.faults());
        assertEquals(3, cacheTracer.hits());
        assertEquals(3, cacheTracer.noFaults());
        assertEquals(3, cacheTracer.failedFaults());
        assertEquals(3, cacheTracer.evictions());
        assertEquals(3, cacheTracer.cooperativeEvictions());
        assertEquals(3, cacheTracer.cooperativeEvictionFlushes());
        assertEquals(3, cacheTracer.evictionExceptions());
        assertEquals(3, cacheTracer.flushes());
        assertEquals(3, cacheTracer.merges());
        assertEquals(30, cacheTracer.bytesWritten());
        assertEquals(450, cacheTracer.bytesRead());
    }

    @Test
    void closingTraceCursorReportEvents() {
        generateEventSet();
        pageCursorTracer.close();

        assertEquals(1, cacheTracer.pins());
        assertEquals(1, cacheTracer.unpins());
        assertEquals(1, cacheTracer.faults());
        assertEquals(1, cacheTracer.hits());
        assertEquals(1, cacheTracer.noFaults());
        assertEquals(1, cacheTracer.failedFaults());
        assertEquals(1, cacheTracer.evictions());
        assertEquals(1, cacheTracer.cooperativeEvictions());
        assertEquals(1, cacheTracer.cooperativeEvictionFlushes());
        assertEquals(1, cacheTracer.evictionExceptions());
        assertEquals(1, cacheTracer.flushes());
        assertEquals(1, cacheTracer.merges());
        assertEquals(10, cacheTracer.bytesWritten());
        assertEquals(150, cacheTracer.bytesRead());

        generateEventSet();
        generateEventSet();
        pageCursorTracer.close();

        assertEquals(3, cacheTracer.pins());
        assertEquals(3, cacheTracer.unpins());
        assertEquals(3, cacheTracer.faults());
        assertEquals(3, cacheTracer.hits());
        assertEquals(3, cacheTracer.noFaults());
        assertEquals(3, cacheTracer.failedFaults());
        assertEquals(3, cacheTracer.evictions());
        assertEquals(3, cacheTracer.cooperativeEvictions());
        assertEquals(3, cacheTracer.cooperativeEvictionFlushes());
        assertEquals(3, cacheTracer.evictionExceptions());
        assertEquals(3, cacheTracer.flushes());
        assertEquals(3, cacheTracer.merges());
        assertEquals(30, cacheTracer.bytesWritten());
        assertEquals(450, cacheTracer.bytesRead());
    }

    @Test
    void shouldCalculateHitRatio() {
        assertEquals(0d, pageCursorTracer.hitRatio(), 0.0001);

        pinAndFault();

        assertEquals(0.0 / 1, pageCursorTracer.hitRatio(), 0.0001);

        pinAndHit();

        assertEquals(1.0 / 2, pageCursorTracer.hitRatio(), 0.0001);

        pinAndFault();
        pinAndFault();
        pinAndFault();
        pinAndHit();
        pinAndHit();

        assertEquals(3.0 / 7, pageCursorTracer.hitRatio(), 0.0001);

        pageCursorTracer.reportEvents();

        assertEquals(3.0 / 7, cacheTracer.hitRatio(), 0.0001);
    }

    @Test
    void pageCursorTracerHasDefinedTag() {
        assertEquals(TEST_TRACER, pageCursorTracer.getTag());
    }

    @Test
    void mergePageCursors() {
        var tracer = createTracer();
        for (int i = 0; i < 5; i++) {
            DummyPageSwapper dummyPageSwapper = new DummyPageSwapper("a", 4);
            try (var pinEvent = tracer.beginPin(false, 1, dummyPageSwapper)) {
                pinEvent.hit();
                pinEvent.noFault();
                try (PinPageFaultEvent pageFaultEvent = pinEvent.beginPageFault(1, dummyPageSwapper)) {
                    pageFaultEvent.addBytesRead(16);
                    pageFaultEvent.setException(null);
                    try (EvictionEvent evictionEvent = pageFaultEvent.beginEviction(3)) {
                        evictionEvent.setSwapper(dummyPageSwapper);
                        try (var flushEvent = evictionEvent.beginFlush(1, dummyPageSwapper, pageRef -> (int) pageRef)) {
                            flushEvent.addPagesMerged(7);
                            flushEvent.addBytesWritten(17);
                        }
                    }
                }
            }
            tracer.unpin(1, dummyPageSwapper);
        }
        pageCursorTracer.merge(new CursorStatisticSnapshot(tracer));

        assertEquals(5, pageCursorTracer.pins());
        assertEquals(5, pageCursorTracer.unpins());
        assertEquals(5, pageCursorTracer.evictions());
        assertEquals(35, pageCursorTracer.merges());
        assertEquals(5, pageCursorTracer.faults());
        assertEquals(5, pageCursorTracer.noFaults());
        assertEquals(5, pageCursorTracer.failedFaults());
        assertEquals(80, pageCursorTracer.bytesRead());
        assertEquals(85, pageCursorTracer.bytesWritten());
    }

    private void generateEventSet() {
        try (var pinEvent = pageCursorTracer.beginPin(false, 0, swapper)) {
            pinEvent.hit();
            pinEvent.noFault();
            try (var pageFaultEvent = pinEvent.beginPageFault(4, swapper)) {
                pageFaultEvent.setException(null);
                pageFaultEvent.addBytesRead(150);
                try (var evictionEvent = pageFaultEvent.beginEviction(0)) {
                    evictionEvent.setSwapper(swapper);
                    try (var flushEvent = evictionEvent.beginFlush(0, swapper, referenceTranslator)) {
                        flushEvent.addBytesWritten(10);
                        flushEvent.addEvictionFlushedPages(1);
                        flushEvent.addPagesMerged(1);
                    }
                    evictionEvent.setException(new IOException("eviction exception"));
                }
            }
        }
        pageCursorTracer.unpin(0, swapper);
    }

    private PageCursorTracer createTracer() {
        var tracer = new DefaultPageCursorTracer(cacheTracer, TEST_TRACER);
        // ignore invariants check as here we produce "impossible" sequences of events in order to test tracer
        tracer.setIgnoreCounterCheck(true);
        return tracer;
    }

    private void pinAndHit() {
        try (var pinEvent = pageCursorTracer.beginPin(true, 0, swapper)) {
            pinEvent.hit();
        }
        pageCursorTracer.unpin(0, swapper);
    }

    private void pinAndFault() {
        try (var pinEvent = pageCursorTracer.beginPin(true, 0, swapper);
                var fault = pinEvent.beginPageFault(0, swapper); ) {
            fault.addBytesRead(0);
        }
        pageCursorTracer.unpin(0, swapper);
    }
}
