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
package org.neo4j.test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.function.Consumer;
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

    // The output buffering mechanics are pre-allocated in case we have to deal with low-memory situations.
    // The output switching is guarded by the monitor lock on the LinearHistoryPageCacheTracer instance.
    // The class name cache is similarly guarded the monitor lock. In short, only a single thread can print history
    // at a time.
    private final SwitchableBufferedOutputStream bufferOut = new SwitchableBufferedOutputStream();
    private final PrintStream out = new PrintStream( bufferOut );
    private final Map<Class<?>, String> classSimpleNameCache = new IdentityHashMap<>();

    private static class SwitchableBufferedOutputStream extends BufferedOutputStream
    {

        public SwitchableBufferedOutputStream()
        {
            //noinspection ConstantConditions
            super( null ); // No output target by default. This is changed in printHistory.
        }

        public void setOut( OutputStream out )
        {
            super.out = out;
        }
    }

    private final HEvent end = new HEvent()
    {
        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( " EOF " );
        }
    };

    public abstract class HEvent
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
            System.identityHashCode( this );
        }

        public final void print( PrintStream out, String exceptionLinePrefix )
        {
            if ( getClass() == EndHEvent.class )
            {
                out.print( '-' );
            }
            out.print( getClass().getSimpleName() );
            out.print( '#' );
            out.print( System.identityHashCode( this ) );
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

    public final class EndHEvent extends HEvent
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
            Class<? extends IntervalHEven> eventClass = event.getClass();
            String className = classSimpleNameCache.get( eventClass );
            if ( className == null )
            {
                className = eventClass.getSimpleName();
                classSimpleNameCache.put( eventClass, className );
            }
            out.print( className );
            out.print( '#' );
            out.print( System.identityHashCode( event ) );
        }
    }

    public abstract class IntervalHEven extends HEvent
    {
        public void close()
        {
            add( new EndHEvent( this ) );
        }
    }

    public class MappedFileHEvent extends HEvent
    {
        File file;

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            print( out, file );
        }
    }

    public class UnmappedFileHEvent extends HEvent
    {
        File file;

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            print( out, file );
        }
    }

    public class EvictionRunHEvent extends IntervalHEven implements EvictionRunEvent
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

    public class EvictionHEvent extends IntervalHEven implements EvictionEvent, FlushEventOpportunity
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
            file = swapper == null? null : swapper.file();
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
            return add( new FlushHEvent( filePageId, cachePageId, swapper ) );
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

    public class FlushHEvent extends IntervalHEven implements FlushEvent
    {
        private long filePageId;
        private int cachePageId;
        private int pageCount;
        private File file;
        private int bytesWritten;
        private IOException exception;

        public FlushHEvent( long filePageId, int cachePageId, PageSwapper swapper )
        {
            this.filePageId = filePageId;
            this.cachePageId = cachePageId;
            this.pageCount = 1;
            this.file = swapper.file();
        }

        @Override
        public void addBytesWritten( long bytes )
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
        public void addPagesFlushed( int pageCount )
        {
            this.pageCount = pageCount;
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( ", filePageId:" );
            out.print( filePageId );
            out.print( ", cachePageId:" );
            out.print( cachePageId );
            out.print( ", pageCount:" );
            out.print( pageCount );
            print( out, file );
            out.print( ", bytesWritten:" );
            out.print( bytesWritten );
            print( out, exception, exceptionLinePrefix );
        }
    }

    public class PinHEvent extends IntervalHEven implements PinEvent
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
            return add( new PageFaultHEvent() );
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

    public class PageFaultHEvent extends IntervalHEven implements PageFaultEvent
    {
        private int bytesRead;
        private int cachePageId;
        private boolean pageEvictedByFaulter;
        private Throwable exception;

        @Override
        public void addBytesRead( long bytes )
        {
            bytesRead += bytes;
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
            this.cachePageId = cachePageId;
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
        public EvictionEvent beginEviction()
        {
            pageEvictedByFaulter = true;
            return add( new EvictionHEvent() );
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( ", cachePageId:" );
            out.print( cachePageId );
            out.print( ", bytesRead:" );
            out.print( bytesRead );
            out.print( ", pageEvictedByFaulter:" );
            out.print( pageEvictedByFaulter );
            print( out, exception, exceptionLinePrefix );
        }
    }

    public class MajorFlushHEvent extends IntervalHEven implements MajorFlushEvent, FlushEventOpportunity
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
            return add( new FlushHEvent( filePageId, cachePageId, swapper ) );
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

    public synchronized boolean processHistory( Consumer<HEvent> processor )
    {
        HEvent events = history.getAndSet( null );
        if ( events == null )
        {
            return false;
        }
        events = reverse( events );
        while ( events != null )
        {
            processor.accept( events );
            events = events.prev;
        }
        return true;
    }

    public synchronized void printHistory( PrintStream outputStream )
    {
        bufferOut.setOut( outputStream );
        if ( !processHistory( new HistoryPrinter() ) )
        {
            out.println( "No events recorded." );
        }
        out.flush();
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

    private class HistoryPrinter implements Consumer<HEvent>
    {
        private final List<HEvent> concurrentIntervals;

        public HistoryPrinter()
        {
            this.concurrentIntervals = new LinkedList<>();
        }

        @Override
        public void accept( HEvent event )
        {
            String exceptionLinePrefix = exceptionLinePrefix( concurrentIntervals.size() );
            if ( event.getClass() == EndHEvent.class )
            {
                EndHEvent endHEvent = (EndHEvent) event;
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
            else if ( event instanceof IntervalHEven )
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
