/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.kernel.impl.util;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.helpers.Format;
import org.neo4j.helpers.collection.Visitor;

public abstract class StringLogger
{
    public static final String DEFAULT_NAME = "messages.log";
    public static final StringLogger SYSTEM =
        new ActualStringLogger( new PrintWriter( System.out ) );
    private static final int DEFAULT_THRESHOLD_FOR_ROTATION = 100 * 1024 * 1024;
    private static final int NUMBER_OF_OLD_LOGS_TO_KEEP = 2;

    public interface LineLogger
    {
        void logLine( String line );
    }

    public static StringLogger logger( File logfile )
    {
        try
        {
            return new ActualStringLogger( new PrintWriter( new FileWriter( logfile, true ) ) );
        }
        catch ( IOException cause )
        {
            throw new RuntimeException( "Could not create log file: " + logfile, cause );
        }
    }

    public static StringLogger logger( String storeDir )
    {
        return logger( storeDir, DEFAULT_THRESHOLD_FOR_ROTATION );
    }

    public static StringLogger logger( String storeDir, int rotationThreshold )
    {
        return new ActualStringLogger( new File( storeDir, DEFAULT_NAME ).getAbsolutePath(),
                rotationThreshold );
    }

    public static StringLogger wrap( final StringBuffer target )
    {
        return new ActualStringLogger( new PrintWriter( new Writer()
        {
            @Override
            public void write( char[] cbuf, int off, int len ) throws IOException
            {
                target.append( cbuf, off, len );
            }

            @Override
            public void write( int c ) throws IOException
            {
                target.appendCodePoint( c );
            }

            @Override
            public void write( char[] cbuf ) throws IOException
            {
                target.append( cbuf );
            }

            @Override
            public void write( String str ) throws IOException
            {
                target.append( str );
            }

            @Override
            public void write( String str, int off, int len ) throws IOException
            {
                target.append( str, off, len );
            }

            @Override
            public Writer append( char c ) throws IOException
            {
                target.append( c );
                return this;
            }

            @Override
            public Writer append( CharSequence csq ) throws IOException
            {
                target.append( csq );
                return this;
            }

            @Override
            public Writer append( CharSequence csq, int start, int end ) throws IOException
            {
                target.append( csq, start, end );
                return this;
            }

            @Override
            public void flush() throws IOException
            {
                // do nothing
            }

            @Override
            public void close() throws IOException
            {
                // do nothing
            }
        } ) );
    }

    public void logMessage( String msg )
    {
        logMessage( msg, false );
    }

    public void logMessage( String msg, Throwable cause )
    {
        logMessage( msg, cause, false );
    }

    public void logLongMessage( String msg, Visitor<LineLogger> source )
    {
        logLongMessage( msg, source, false );
    }

    public void logLongMessage( String msg, Iterable<String> source )
    {
        logLongMessage( msg, source, false );
    }

    public void logLongMessage( String msg, Iterable<String> source, boolean flush )
    {
        logLongMessage( msg, source.iterator(), flush );
    }

    public void logLongMessage( String msg, Iterator<String> source )
    {
        logLongMessage( msg, source, false );
    }

    public void logLongMessage( String msg, final Iterator<String> source, boolean flush )
    {
        logLongMessage( msg, new Visitor<LineLogger>()
        {
            @Override
            public boolean visit( LineLogger logger )
            {
                for ( String line : loop( source ) )
                {
                    logger.logLine( line );
                }
                return true;
            }
        }, flush );
    }

    public abstract void logLongMessage( String msg, Visitor<LineLogger> source, boolean flush );

    public abstract void logMessage( String msg, boolean flush );

    public abstract void logMessage( String msg, Throwable cause, boolean flush );

    public abstract void addRotationListener( Runnable listener );

    public abstract void flush();

    public abstract void close();

    protected abstract void logLine( String line );

    public static final StringLogger DEV_NULL = new StringLogger()
    {
        @Override
        public void logMessage( String msg, boolean flush ) {}

        @Override
        public void logMessage( String msg, Throwable cause, boolean flush ) {}

        @Override
        public void logLongMessage( String msg, Visitor<LineLogger> source, boolean flush ) {}

        @Override
        protected void logLine( String line ) {}

        @Override
        public void flush() {}

        @Override
        public void close() {}

        @Override
        public void addRotationListener( Runnable listener ) {}
    };

    private static class ActualStringLogger extends StringLogger
    {
        private PrintWriter out;
        private final Integer rotationThreshold;
        private final File file;
        private final List<Runnable> onRotation = new CopyOnWriteArrayList<Runnable>();

        private ActualStringLogger( String filename, int rotationThreshold )
        {
            this.rotationThreshold = rotationThreshold;
            try
            {
                file = new File( filename );
                if ( file.getParentFile() != null )
                {
                    file.getParentFile().mkdirs();
                }
                instantiateWriter();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        private ActualStringLogger( PrintWriter writer )
        {
            this.out = writer;
            this.rotationThreshold = null;
            this.file = null;
        }

        @Override
        public void addRotationListener( Runnable trigger )
        {
            onRotation.add( trigger );
        }

        private void instantiateWriter() throws IOException
        {
            out = new PrintWriter( new FileWriter( file, true ) );
            for ( Runnable trigger : onRotation )
            {
                trigger.run();
            }
        }

        @Override
        public synchronized void logMessage( String msg, boolean flush )
        {
//            ensureOpen();
            out.println( time() + ": " + msg );
            if ( flush )
            {
                out.flush();
            }
            checkRotation();
        }

        private String time()
        {
            return Format.date();
        }

        @Override
        public synchronized void logMessage( String msg, Throwable cause, boolean flush )
        {
//            ensureOpen();
            out.println( time() + ": " + msg + " " + cause.getMessage() );
            cause.printStackTrace( out );
            if ( flush )
            {
                out.flush();
            }
            checkRotation();
        }

        @Override
        public synchronized void logLongMessage( String msg, Visitor<LineLogger> source, boolean flush )
        {
            out.println( time() + ": " + msg );
            source.visit( new LineLoggerImpl( this ) );
            if ( flush )
            {
                out.flush();
            }
            checkRotation();
        }

        @Override
        protected void logLine( String line )
        {
            out.println( "    " + line );
        }

//        private void ensureOpen()
//        {
//            /*
//             * Since StringLogger has instances in its own static map and HA graph db
//             * does internal restarts of the database the StringLogger instances are kept
//             * whereas the actual files can be removed/replaced, making the PrintWriter
//             * fail at writing stuff and also swallowing those exceptions(!). Since we
//             * have this layout of static map of loggers we'll have to reopen the PrintWriter
//             * in such occasions. It'd be better to tie each StringLogger to a GraphDatabaseService.
//             */
//            if ( out.checkError() )
//            {
//                out.close();
//                try
//                {
//                    instantiateWriter();
//                }
//                catch ( IOException e )
//                {
//                    throw new RuntimeException( e );
//                }
//            }
//        }

        private volatile boolean doingRotation = false;

        private void checkRotation()
        {
            if ( rotationThreshold != null && file.length() > rotationThreshold.intValue() && !doingRotation )
            {
                doRotation();
            }
        }

        private void doRotation()
        {
            doingRotation = true;
            out.close();
            moveAwayFile();
            try
            {
                instantiateWriter();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                doingRotation = false;
            }
        }

        /**
         * Will move:
         * messages.log.1 -> messages.log.2
         * messages.log   -> messages.log.1
         *
         * Will delete (if exists):
         * messages.log.2
         */
        private void moveAwayFile()
        {
            File oldLogFile = new File( file.getParentFile(), file.getName() + "." + NUMBER_OF_OLD_LOGS_TO_KEEP );
            if ( oldLogFile.exists() )
            {
                oldLogFile.delete();
            }

            for ( int i = NUMBER_OF_OLD_LOGS_TO_KEEP-1; i >= 0; i-- )
            {
                oldLogFile = new File( file.getParentFile(), file.getName() + (i == 0 ? "" : ("." + i)) );
                if ( oldLogFile.exists() )
                {
                    oldLogFile.renameTo( new File( file.getParentFile(), file.getName() + "." + (i+1) ) );
                }
            }
        }

        @Override
        public void flush()
        {
            out.flush();
        }

        @Override
        public void close()
        {
            out.close();
        }
        
        @Override
        public String toString()
        {
            return "StringLogger[" + this.file + "]";
        }
    }

    protected static final class LineLoggerImpl implements LineLogger
    {
        private final StringLogger target;

        public LineLoggerImpl( StringLogger target )
        {
            this.target = target;
        }

        public void logLine( String line )
        {
            target.logLine( line );
        }
    }
}
