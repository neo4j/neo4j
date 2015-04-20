/*
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
package org.neo4j.kernel.impl.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Visitable;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.logging.LogMarker;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.Predicates.equalTo;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

/**
 * A string logger implementation for testing that components log things correctly.
 * See the TestTestLogger (I know, sorry) test for how to use this to assert things.
 */
public class TestLogger extends StringLogger
{

    private enum Level
    {
        DEBUG,
        INFO,
        WARN,
        ERROR,
        UNKNOWN
    }

    public static final class LogCall implements Visitable<Visitor<LogCall, RuntimeException>>
    {

        protected final Level level;
        protected final String message;
        protected final Throwable cause;
        protected final boolean flush;

        private LogCall( Level level, String message, Throwable cause, boolean flush )
        {
            this.level = level;
            this.message = message;
            this.cause = cause;
            this.flush = flush;
        }

        // DSL sugar methods to use when writing assertions.
        public static LogCall debug(String msg) { return new LogCall(Level.DEBUG, msg, null, false); }
        public static LogCall info(String msg)  { return new LogCall(Level.INFO,  msg, null, false); }
        public static LogCall warn(String msg)  { return new LogCall(Level.WARN,  msg, null, false); }
        public static LogCall error(String msg) { return new LogCall(Level.ERROR, msg, null, false); }
        public static LogCall unknown(String msg) { return new LogCall(Level.UNKNOWN, msg, null, false); }

        public static LogCall debug(String msg, Throwable c) { return new LogCall(Level.DEBUG, msg, c, false); }
        public static LogCall info(String msg,  Throwable c) { return new LogCall(Level.INFO,  msg, c, false); }
        public static LogCall warn(String msg,  Throwable c) { return new LogCall(Level.WARN,  msg, c, false); }
        public static LogCall error(String msg, Throwable c) { return new LogCall(Level.ERROR, msg, c, false); }
        public static LogCall unknown(String msg, Throwable c) { return new LogCall(Level.UNKNOWN, msg, c, false); }

        @Override
        public void accept( Visitor<LogCall,RuntimeException> visitor )
        {
            visitor.visit( this );
        }

        @Override
        public String toString()
        {
            return "LogCall{ " + level +
                    ", message='" + message + '\'' +
                    ", cause=" + cause +
                    '}';
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
                return true;
            if ( o == null || getClass() != o.getClass() )
                return false;

            LogCall logCall = (LogCall) o;

            return flush == logCall.flush && level == logCall.level &&
                    message.equals( logCall.message ) &&
                    !(cause != null ? !cause.equals( logCall.cause ) : logCall.cause != null);
        }

        @Override
        public int hashCode()
        {
            int result = level.hashCode();
            result = 31 * result + message.hashCode();
            result = 31 * result + (cause != null ? cause.hashCode() : 0);
            result = 31 * result + (flush ? 1 : 0);
            return result;
        }
    }

    private final List<LogCall> logCalls = new ArrayList<LogCall>();

    //
    // TEST TOOLS
    //

    public void assertExactly(LogCall... expectedLogCalls )
    {
        Iterator<LogCall> expected = asList( expectedLogCalls ).iterator();
        Iterator<LogCall> actual   = logCalls.iterator();

        while(expected.hasNext())
        {
            if(actual.hasNext())
            {
                assertEquals( expected.next(), actual.next() );
            } else
            {
                fail(format( "Got fewer log calls than expected. The missing log calls were: \n%s", serialize(expected)));
            }
        }

        if(actual.hasNext())
        {
            fail(format( "Got more log calls than expected. The remaining log calls were: \n%s", serialize(actual)));
        }
    }

    /**
     * Note: Does not care about ordering.
     */
    public void assertAtLeastOnce( LogCall... expectedCalls )
    {
        Set<LogCall> expected = asSet(expectedCalls);
        for ( LogCall logCall : logCalls )
            expected.remove( logCall );
        if(expected.size() > 0)
        {
            fail( "These log calls were expected, but never occurred: \n" + serialize( expected.iterator() ) + "\nActual log calls were:\n" + serialize( logCalls.iterator() ) );
        }
    }

    public void assertContainsMessageContaining( String partOfMessage )
    {
        for ( LogCall logCall : logCalls )
        {
            if ( logCall.message.contains( partOfMessage ) )
            {
                return;
            }
        }
        fail( "Expected at least one log statement containing '" + partOfMessage + "', but none found. ctual log calls were:\n" + serialize( logCalls.iterator() ) );
    }

    public void assertNoDebugs()
    {
        assertNo( hasLevel( Level.DEBUG ), "Expected no messages with level DEBUG.");
    }

    public void assertNoInfos()
    {
        assertNo( hasLevel( Level.INFO ), "Expected no messages with level INFO.");
    }

    public void assertNoWarnings()
    {
        assertNo( hasLevel( Level.WARN ), "Expected no messages with level WARN.");
    }

    public void assertNoErrors()
    {
        assertNo( hasLevel( Level.ERROR ), "Expected no messages with level ERROR.");
    }

    public void assertNoLoggingOccurred()
    {
        if(logCalls.size() != 0)
        {
            fail( "Expected no log messages at all, but got:\n" + serialize( logCalls.iterator() ) );
        }
    }

    public void assertNo(LogCall call)
    {
        long found = count( filter( equalTo(call), logCalls ) );
        if( found != 0 )
        {
            fail( "Expected no occurrence of " + call + ", but it was in fact logged " + found + " times." );
        }
    }

    public void assertNo(Predicate<LogCall> predicate, String failMessage)
    {
        Iterable<LogCall> found = filter( predicate, logCalls );
        if(count( found ) != 0 )
        {
            fail( failMessage + " But found: \n" + serialize( found.iterator() ) );
        }
    }

    /**
     * Clear this logger for re-use.
     */
    public void clear()
    {
        logCalls.clear();
    }

    //
    // IMPLEMENTATION
    //

    private void log( Level level, String message, Throwable cause )
    {
        logCalls.add( new LogCall(level, message, cause, false) );
    }

    @Override
    public void debug( String msg )
    {
        debug( msg, null );
    }

    @Override
    public void debug( String msg, Throwable cause )
    {
        log( Level.DEBUG, msg, cause );
    }

    @Override
    public void info( String msg )
    {
        info( msg, null );
    }

    @Override
    public void info( String msg, Throwable cause )
    {
        log( Level.INFO, msg, cause );
    }

    @Override
    public void warn( String msg )
    {
        warn( msg, null );
    }

    @Override
    public void warn( String msg, Throwable cause )
    {
        log( Level.WARN, msg, cause );
    }

    @Override
    public void error( String msg )
    {
        error( msg, null );
    }

    @Override
    public void error( String msg, Throwable cause )
    {
        log( Level.ERROR, msg, cause );
    }

    @Override
    public void logMessage( String msg, Throwable cause, boolean flush )
    {
        log( Level.UNKNOWN, msg, cause );
    }

    @Override
    public void logMessage( String msg, boolean flush )
    {
        logMessage( msg, null, flush );
    }

    @Override
    public void logMessage( String msg, LogMarker marker )
    {
        logMessage( msg, null, false );
    }

    @Override
    protected void logLine( String line )
    {
        // Not sure about the state of the line logger. For now, we delegate to throw the "don't use this" runtime exception
        // please modify appropriately if you know anything about this, because I'm not confident about this. /jake
        logMessage( line );
    }

    @Override
    public void logLongMessage( String msg, Visitor<LineLogger, RuntimeException> source, boolean flush )
    {
        source.visit( new LineLoggerImpl( this ) );
    }

    @Override
    public void addRotationListener( Runnable listener )
    {
    }

    @Override
    public void flush()
    {
    }

    @Override
    public void close()
    {
        // no-op
    }

    private String serialize( Iterator<LogCall> events )
    {
        StringBuilder sb = new StringBuilder(  );
        while(events.hasNext())
        {
            sb.append( events.next().toString() );
            sb.append("\n");
        }
        return sb.toString();
    }

    public void visitLogCalls( Visitor<LogCall, RuntimeException> visitor )
    {
        for (LogCall logCall : logCalls)
        {
            logCall.accept( visitor );
        }
    }

    private Predicate<LogCall> hasLevel( final Level level )
    {
        return new Predicate<LogCall>(){
            @Override
            public boolean accept( LogCall item )
            {
                return item.level == level;
            }
        };
    }
}
