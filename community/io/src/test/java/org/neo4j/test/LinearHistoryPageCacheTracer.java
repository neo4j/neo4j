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
package org.neo4j.test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PinEvent;

/**
 * This PageCacheTracer records a linearized history of the internal page cache events.
 *
 * This takes up a lot of heap memory, because nothing is ever thrown away.
 *
 * Only use this for debugging internal data race bugs and the like, in the page cache.
 */
public final class LinearHistoryPageCacheTracer implements PageCacheTracer
{
    private final AtomicReference<HEvent> history = new AtomicReference<>();

    private final HEvent end = new HEvent()
    {
        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( " EOF " );
        }
    };

    private abstract class HEvent
    {
        final long time;
        final long threadId;
        final String threadName;
        volatile HEvent prev;

        private HEvent()
        {
            time = System.nanoTime();
            Thread thread = Thread.currentThread();
            threadId = thread.getId();
            threadName = thread.getName();
        }

        public final void print( PrintStream out, String exceptionLinePrefix )
        {
            if ( getClass() == EndHEvent.class )
            {
                out.append( '-' );
            }
            out.print( getClass().getSimpleName() );
            out.print( '[' );
            out.print( "time:" );
            out.print( (time - end.time) / 1000 );
            out.print( ", threadId:" );
            out.print( threadId );
            printBody( out, exceptionLinePrefix );
            out.print( ']' );
        }

        abstract void printBody( PrintStream out, String exceptionLinePrefix );

        protected final void print( PrintStream out, File file )
        {
            out.print( ", file:" );
            out.print( file == null ? "<null>" : file.getPath() );
        }

        protected final void print( PrintStream out, Throwable exception, String linePrefix )
        {
            if ( exception != null )
            {
                out.println( ", exception:" );
                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                PrintStream sbuf = new PrintStream( buf );
                exception.printStackTrace( sbuf );
                sbuf.flush();
                BufferedReader reader = new BufferedReader( new StringReader( buf.toString() ) );
                try
                {
                    String line = reader.readLine();
                    while ( line != null )
                    {
                        out.print( linePrefix );
                        out.print( '\t' );
                        out.println( line );
                        line = reader.readLine();
                    }
                    out.print( linePrefix );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        }
    }

    private final class EndHEvent extends HEvent
    {
        IntervalHEven event;

        public EndHEvent( IntervalHEven event )
        {
            this.event = event;
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( ", elapsedMicros:" );
            out.print( (time - event.time) / 1000 );
            out.print( ", endOf:" );
            out.print( event.getClass().getSimpleName() );
        }
    }

    private abstract class IntervalHEven extends HEvent
    {
        public void close()
        {
            add( new EndHEvent( this ) );
        }
    }

    private class MappedFileHEvent extends HEvent
    {
        File file;

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            print( out, file );
        }
    }

    private class UnmappedFileHEvent extends HEvent
    {
        File file;

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            print( out, file );
        }
    }

    private class EvictionRunHEvent extends IntervalHEven implements EvictionRunEvent
    {
        int pagesToEvict;

        private EvictionRunHEvent( int pagesToEvict )
        {
            this.pagesToEvict = pagesToEvict;
        }

        @Override
        public EvictionEvent beginEviction()
        {
            return add( new EvictionHEvent() );
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( ", pagesToEvict:" );
            out.print( pagesToEvict );
        }
    }

    private class EvictionHEvent extends IntervalHEven implements EvictionEvent, FlushEventOpportunity
    {
        private long filePageId;
        private File file;
        private IOException exception;
        private int cachePageId;

        @Override
        public void setFilePageId( long filePageId )
        {
            this.filePageId = filePageId;
        }

        @Override
        public void setSwapper( PageSwapper swapper )
        {
            file = swapper.file();
        }

        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return this;
        }

        @Override
        public void threwException( IOException exception )
        {
            this.exception = exception;
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
            this.cachePageId = cachePageId;
        }

        @Override
        public FlushEvent beginFlush( long filePageId, int cachePageId, PageSwapper swapper )
        {
            return add( new FlushHEvent( filePageId, cachePageId, swapper, this ) );
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( ", filePageId:" );
            out.print( filePageId );
            out.print( ", cachePageId:" );
            out.print( cachePageId );
            print( out, file );
            print( out, exception, exceptionLinePrefix );
        }
    }

    private class FlushHEvent extends IntervalHEven implements FlushEvent
    {
        private long filePageId;
        private int cachePageId;
        private File file;
        private HEvent cause;
        private int bytesWritten;
        private IOException exception;

        public FlushHEvent( long filePageId, int cachePageId, PageSwapper swapper, HEvent cause )
        {
            this.filePageId = filePageId;
            this.cachePageId = cachePageId;
            this.file = swapper.file();
            this.cause = cause;
        }

        @Override
        public void addBytesWritten( int bytes )
        {
            bytesWritten += bytes;
        }

        @Override
        public void done()
        {
            close();
        }

        @Override
        public void done( IOException exception )
        {
            this.exception = exception;
            done();
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( ", filePageId:" );
            out.print( filePageId );
            out.print( ", cachePageId:" );
            out.print( cachePageId );
            print( out, file );
            out.print( ", bytesWritten:" );
            out.print( bytesWritten );
            print( out, exception, exceptionLinePrefix );
        }
    }

    private class PinHEvent extends IntervalHEven implements PinEvent
    {
        private boolean exclusiveLock;
        private long filePageId;
        private File file;
        private int cachePageId;

        public PinHEvent( boolean exclusiveLock, long filePageId, PageSwapper swapper )
        {
            this.exclusiveLock = exclusiveLock;
            this.filePageId = filePageId;
            this.file = swapper.file();
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
            this.cachePageId = cachePageId;
        }

        @Override
        public PageFaultEvent beginPageFault()
        {
            return add( new PageFaultHEvent( this ) );
        }

        @Override
        public void done()
        {
            close();
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( ", filePageId:" );
            out.print( filePageId );
            out.print( ", cachePageId:" );
            out.print( cachePageId );
            print( out, file );
            out.append( ", exclusiveLock:" );
            out.print( exclusiveLock );
        }
    }

    private class PageFaultHEvent extends IntervalHEven implements PageFaultEvent
    {
        private PinHEvent cause;
        private int bytesRead;
        private int cachePageId;
        private boolean parked;
        private Throwable exception;

        public PageFaultHEvent( PinHEvent cause )
        {
            this.cause = cause;
        }

        @Override
        public void addBytesRead( int bytes )
        {
            bytesRead += bytes;
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
            this.cachePageId = cachePageId;
        }

        @Override
        public void setParked( boolean parked )
        {
            this.parked = parked;
        }

        @Override
        public void done()
        {
            close();
        }

        @Override
        public void done( Throwable throwable )
        {
            this.exception = throwable;
            done();
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( ", cachePageId:" );
            out.print( cachePageId );
            out.print( ", bytesRead:" );
            out.print( bytesRead );
            out.print( ", parked:" );
            out.print( parked );
            print( out, exception, exceptionLinePrefix );
        }
    }

    private class MajorFlushHEvent extends IntervalHEven implements MajorFlushEvent, FlushEventOpportunity
    {
        private File file;

        public MajorFlushHEvent( File file )
        {
            this.file = file;
        }

        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return this;
        }

        @Override
        public FlushEvent beginFlush( long filePageId, int cachePageId, PageSwapper swapper )
        {
            return add( new FlushHEvent( filePageId, cachePageId, swapper, this ) );
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            print( out, file );
        }
    }

    <E extends HEvent> E add( E event )
    {
        HEvent prev = history.getAndSet( event );
        event.prev = prev == null? end : prev;
        return event;
    }

    public void printHistory( PrintStream out )
    {
        HEvent events = history.getAndSet( null );
        if ( events == null )
        {
            out.println( "No events recorded." );
        }

        events = reverse( events );
        List<HEvent> concurrentIntervals = new LinkedList<>();

        while ( events != end )
        {
            String exceptionLinePrefix = exceptionLinePrefix( concurrentIntervals.size() );
            if ( events.getClass() == EndHEvent.class )
            {
                EndHEvent endHEvent = (EndHEvent) events;
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
                    putcs( out, '|', idx );
                    putcs( out, '/', left );
                    out.println();
                }
            }
            else if ( events instanceof IntervalHEven )
            {
                putcs( out, '|', concurrentIntervals.size() );
                out.print( "+   " );
                events.print( out, exceptionLinePrefix );
                concurrentIntervals.add( events );
                out.println();
            }
            else
            {
                putcs( out, '|', concurrentIntervals.size() );
                out.print( ">   " );
                events.print( out, exceptionLinePrefix );
            }
            events = events.prev;
        }
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

    private HEvent reverse( HEvent events )
    {
        HEvent current = end;
        while ( events != end )
        {
            HEvent prev;
            do
            {
                prev = events.prev;
            } while ( prev == null );
            events.prev = current;
            current = events;
            events = prev;
        }
        return current;
    }

    @Override
    public void mappedFile( File file )
    {
        add( new MappedFileHEvent() ).file = file;
    }

    @Override
    public void unmappedFile( File file )
    {
        add( new UnmappedFileHEvent() ).file = file;
    }

    @Override
    public EvictionRunEvent beginPageEvictions( int pageCountToEvict )
    {
        return add( new EvictionRunHEvent( pageCountToEvict ) );
    }

    @Override
    public PinEvent beginPin( boolean exclusiveLock, long filePageId, PageSwapper swapper )
    {
        return add( new PinHEvent( exclusiveLock, filePageId, swapper ));
    }

    @Override
    public MajorFlushEvent beginFileFlush( PageSwapper swapper )
    {
        return add( new MajorFlushHEvent( swapper.file() ) );
    }

    @Override
    public MajorFlushEvent beginCacheFlush()
    {
        return add( new MajorFlushHEvent( null ) );
    }

    @Override
    public long countFaults()
    {
        return 0;
    }

    @Override
    public long countEvictions()
    {
        return 0;
    }

    @Override
    public long countPins()
    {
        return 0;
    }

    @Override
    public long countUnpins()
    {
        return 0;
    }

    @Override
    public long countFlushes()
    {
        return 0;
    }

    @Override
    public long countBytesRead()
    {
        return 0;
    }

    @Override
    public long countBytesWritten()
    {
        return 0;
    }

    @Override
    public long countFilesMapped()
    {
        return 0;
    }

    @Override
    public long countFilesUnmapped()
    {
        return 0;
    }

    @Override
    public long countEvictionExceptions()
    {
        return 0;
    }
}
