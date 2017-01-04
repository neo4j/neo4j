/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

public class DefaultPageCursorTracerTest
{
//TODO:
    private PageSwapper swapper;
    private PageCursorTracer tracer;

    @Before
    public void setUp()
    {
        tracer = createTracer();
        swapper = new DummyPageSwapper( "filename" );
    }

    @Test
    public void mustCountPinsAndUnpins()
    {
        PinEvent pinEvent = tracer.beginPin( true, 0, swapper );
        pinEvent.done();

        // We don't particularly care whether the counts are incremented on begin or close
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

    }

    private PageCursorTracer createTracer()
    {
        DefaultPageCursorTracer.enablePinUnpinTracing();
        return new DefaultPageCursorTracer();
    }

}
