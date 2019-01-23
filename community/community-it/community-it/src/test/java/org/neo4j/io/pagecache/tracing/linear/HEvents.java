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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.IdentityHashMap;
import java.util.Map;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PinEvent;

/**
 * Container of events for page cache tracers that are used to build linear historical representation of page cache
 * events.
 * In case if event can generate any other event it will properly add it to corresponding tracer and it will be also
 * tracked.
 * @see LinearHistoryTracer
 */
class HEvents
{
    private HEvents()
    {
    }

    static final class EndHEvent extends HEvent
    {
        private static final Map<Class<?>,String> classSimpleNameCache = new IdentityHashMap<>();
        IntervalHEvent event;

        EndHEvent( IntervalHEvent event )
        {
            this.event = event;
        }

        @Override
        public void print( PrintStream out, String exceptionLinePrefix )
        {
            out.print( '-' );
            super.print( out, exceptionLinePrefix );
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( ", elapsedMicros:" );
            out.print( (time - event.time) / 1000 );
            out.print( ", endOf:" );
            Class<? extends IntervalHEvent> eventClass = event.getClass();
            String className = classSimpleNameCache.computeIfAbsent( eventClass, k -> eventClass.getSimpleName() );
            out.print( className );
            out.print( '#' );
            out.print( System.identityHashCode( event ) );
        }
    }

    static class MappedFileHEvent extends HEvent
    {
        File file;

        MappedFileHEvent( File file )
        {
            this.file = file;
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            print( out, file );
        }
    }

    static class UnmappedFileHEvent extends HEvent
    {
        File file;

        UnmappedFileHEvent( File file )
        {
            this.file = file;
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            print( out, file );
        }
    }

    public static class EvictionRunHEvent extends IntervalHEvent implements EvictionRunEvent
    {
        int pagesToEvict;

        EvictionRunHEvent( LinearHistoryTracer tracer, int pagesToEvict )
        {
            super( tracer );
            this.pagesToEvict = pagesToEvict;
        }

        @Override
        public EvictionEvent beginEviction()
        {
            return tracer.add( new EvictionHEvent( tracer ) );
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            out.print( ", pagesToEvict:" );
            out.print( pagesToEvict );
        }
    }

    public static class FlushHEvent extends IntervalHEvent implements FlushEvent
    {
        private long filePageId;
        private long cachePageId;
        private int pageCount;
        private File file;
        private int bytesWritten;
        private IOException exception;

        FlushHEvent( LinearHistoryTracer tracer, long filePageId, long cachePageId, PageSwapper swapper )
        {
            super( tracer );
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

    public static class MajorFlushHEvent extends IntervalHEvent implements MajorFlushEvent, FlushEventOpportunity
    {
        private File file;

        MajorFlushHEvent( LinearHistoryTracer tracer, File file )
        {
            super( tracer );
            this.file = file;
        }

        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return this;
        }

        @Override
        public FlushEvent beginFlush( long filePageId, long cachePageId, PageSwapper swapper )
        {
            return tracer.add( new FlushHEvent( tracer, filePageId, cachePageId, swapper ) );
        }

        @Override
        void printBody( PrintStream out, String exceptionLinePrefix )
        {
            print( out, file );
        }
    }

    public static class PinHEvent extends IntervalHEvent implements PinEvent
    {
        private boolean exclusiveLock;
        private long filePageId;
        private File file;
        private long cachePageId;
        private boolean hit;

        PinHEvent( LinearHistoryTracer tracer, boolean exclusiveLock, long filePageId, PageSwapper swapper )
        {
            super( tracer );
            this.exclusiveLock = exclusiveLock;
            this.filePageId = filePageId;
            this.hit = true;
            this.file = swapper.file();
        }

        @Override
        public void setCachePageId( long cachePageId )
        {
            this.cachePageId = cachePageId;
        }

        @Override
        public PageFaultEvent beginPageFault()
        {
            hit = false;
            return tracer.add( new PageFaultHEvent( tracer ) );
        }

        @Override
        public void hit()
        {
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
            out.print( ", hit:" );
            out.print( hit );
            print( out, file );
            out.append( ", exclusiveLock:" );
            out.print( exclusiveLock );
        }
    }

    public static class PageFaultHEvent extends IntervalHEvent implements PageFaultEvent
    {
        private int bytesRead;
        private long cachePageId;
        private boolean pageEvictedByFaulter;
        private Throwable exception;

        PageFaultHEvent( LinearHistoryTracer linearHistoryTracer )
        {
            super( linearHistoryTracer );
        }

        @Override
        public void addBytesRead( long bytes )
        {
            bytesRead += bytes;
        }

        @Override
        public void setCachePageId( long cachePageId )
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
            return tracer.add( new EvictionHEvent( tracer ) );
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

    public static class EvictionHEvent extends IntervalHEvent implements EvictionEvent, FlushEventOpportunity
    {
        private long filePageId;
        private File file;
        private IOException exception;
        private long cachePageId;

        EvictionHEvent( LinearHistoryTracer linearHistoryTracer )
        {
            super( linearHistoryTracer );
        }

        @Override
        public void setFilePageId( long filePageId )
        {
            this.filePageId = filePageId;
        }

        @Override
        public void setSwapper( PageSwapper swapper )
        {
            file = swapper == null ? null : swapper.file();
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
        public void setCachePageId( long cachePageId )
        {
            this.cachePageId = cachePageId;
        }

        @Override
        public FlushEvent beginFlush( long filePageId, long cachePageId, PageSwapper swapper )
        {
            return tracer.add( new FlushHEvent( tracer, filePageId, cachePageId, swapper ) );
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

    public abstract static class HEvent
    {
        static final HEvent end = new HEvent()
        {
            @Override
            void printBody( PrintStream out, String exceptionLinePrefix )
            {
                out.print( " EOF " );
            }
        };

        final long time;
        final long threadId;
        final String threadName;
        volatile HEvent prev;

        HEvent()
        {
            time = System.nanoTime();
            Thread thread = Thread.currentThread();
            threadId = thread.getId();
            threadName = thread.getName();
            System.identityHashCode( this );
        }

        public static HEvent reverse( HEvent events )
        {
            HEvent current = end;
            while ( events != end )
            {
                HEvent prev;
                do
                {
                    prev = events.prev;
                }
                while ( prev == null );
                events.prev = current;
                current = events;
                events = prev;
            }
            return current;
        }

        public void print( PrintStream out, String exceptionLinePrefix )
        {
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

    public abstract static class IntervalHEvent extends HEvent
    {
        protected LinearHistoryTracer tracer;

        IntervalHEvent( LinearHistoryTracer tracer )
        {
            this.tracer = tracer;
        }

        public void close()
        {
            tracer.add( new EndHEvent( this ) );
        }
    }
}
