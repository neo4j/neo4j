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
package org.neo4j.io.pagecache.tracing.linear;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Records a linearized history of all (global and cursors) internal page cache events.
 * Only use this for debugging internal data race bugs and the like, in the page cache.
 */
class LinearHistoryTracer
{
    private final AtomicReference<HEvents.HEvent> history = new AtomicReference<>();

    // The output buffering mechanics are pre-allocated in case we have to deal with low-memory situations.
    // The output switching is guarded by the monitor lock on the LinearHistoryPageCacheTracer instance.
    // The class name cache is similarly guarded the monitor lock. In short, only a single thread can print history
    // at a time.
    private final SwitchableBufferedOutputStream bufferOut = new SwitchableBufferedOutputStream();
    private final PrintStream out = new PrintStream( bufferOut );

    synchronized boolean processHistory( Consumer<HEvents.HEvent> processor )
    {
        HEvents.HEvent events = history.getAndSet( null );
        if ( events == null )
        {
            return false;
        }
        events = HEvents.HEvent.reverse( events );
        while ( events != null )
        {
            processor.accept( events );
            events = events.prev;
        }
        return true;
    }

    <E extends HEvents.HEvent> E add( E event )
    {
        HEvents.HEvent prev = history.getAndSet( event );
        event.prev = prev == null ? HEvents.HEvent.end : prev;
        return event;
    }

    synchronized void printHistory( PrintStream outputStream )
    {
        bufferOut.setOut( outputStream );
        if ( !processHistory( new HistoryPrinter() ) )
        {
            out.println( "No events recorded." );
        }
        out.flush();
    }

    private static class SwitchableBufferedOutputStream extends BufferedOutputStream
    {

        SwitchableBufferedOutputStream()
        {
            //noinspection ConstantConditions
            super( null ); // No output target by default. This is changed in printHistory.
        }

        public void setOut( OutputStream out )
        {
            super.out = out;
        }
    }

    private class HistoryPrinter implements Consumer<HEvents.HEvent>
    {
        private final List<HEvents.HEvent> concurrentIntervals;

        HistoryPrinter()
        {
            this.concurrentIntervals = new LinkedList<>();
        }

        @Override
        public void accept( HEvents.HEvent event )
        {
            String exceptionLinePrefix = exceptionLinePrefix( concurrentIntervals.size() );
            if ( event.getClass() == HEvents.EndHEvent.class )
            {
                HEvents.EndHEvent endHEvent = (HEvents.EndHEvent) event;
                int idx = concurrentIntervals.indexOf( endHEvent.event );
                putcs( out, '|', idx );
                out.print( '-' );
                int left = concurrentIntervals.size() - idx - 1;
                putcs( out, '|', left );
                out.print( "   " );
                endHEvent.print( out, exceptionLinePrefix );
                concurrentIntervals.remove( idx );
                if ( left > 0 )
                {
                    out.println();
                    putcs( out, '|', idx );
                    putcs( out, '/', left );
                }
            }
            else if ( event instanceof HEvents.IntervalHEvent )
            {
                putcs( out, '|', concurrentIntervals.size() );
                out.print( "+   " );
                event.print( out, exceptionLinePrefix );
                concurrentIntervals.add( event );
            }
            else
            {
                putcs( out, '|', concurrentIntervals.size() );
                out.print( ">   " );
                event.print( out, exceptionLinePrefix );
            }
            out.println();
        }

        private String exceptionLinePrefix( int size )
        {
            StringBuilder sb = new StringBuilder();
            for ( int i = 0; i < size; i++ )
            {
                sb.append( '|' );
            }
            sb.append( ":  " );
            return sb.toString();
        }

        private void putcs( PrintStream out, char c, int count )
        {
            for ( int i = 0; i < count; i++ )
            {
                out.print( c );
            }
        }
    }
}
