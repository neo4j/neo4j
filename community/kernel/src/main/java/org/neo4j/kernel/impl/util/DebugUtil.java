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
package org.neo4j.kernel.impl.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.lineSeparator;
import static java.lang.System.nanoTime;
import static org.neo4j.helpers.Exceptions.stringify;
import static org.neo4j.helpers.Format.duration;

public class DebugUtil
{
    private DebugUtil()
    {
    }

    public static void logTrace( String fmt, Object... args )
    {
        logTrace( 2, 5, fmt, args );
    }

    public static void logTrace( int skip, int limit, String fmt, Object... args )
    {
        if ( enabledAssertions() )
        {
            Thread thread = Thread.currentThread();
            String threadName = thread.getName();
            ThreadGroup group = thread.getThreadGroup();
            String groupPart = group != null ? " in group " + group.getName() : "";
            String message = "[" + threadName + groupPart + "] " + String.format( fmt, args );
            TraceLog traceLog = new TraceLog( message );
            printLimitedStackTrace( System.err, traceLog, skip, limit );
        }
    }

    private static void printLimitedStackTrace( PrintStream out, Throwable cause, int skip, int limit )
    {
        synchronized ( out )
        {
            String[] lines = stringify( cause ).split( lineSeparator() );
            for ( String line : lines )
            {
                if ( line.startsWith( "\tat " ) )
                {
                    if ( skip > 0 )
                    {
                        skip--;
                    }
                    else if ( limit > 0 )
                    {
                        limit--;
                        out.println( line );
                    }
                    else
                    {
                        break;
                    }
                }
                else
                {
                    out.println( line );
                }
            }
        }
    }

    public static void printShortStackTrace( Throwable cause, int maxNumberOfStackLines )
    {
        System.out.println( firstLinesOf( stringify( cause ), maxNumberOfStackLines + 1 ) );
    }

    public static String firstLinesOf( String string, int maxNumberOfLines )
    {
        // Totally verbose implementation of this functionality :)
        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter( stringWriter );
        try
        {
            BufferedReader reader = new BufferedReader( new StringReader( string ) );
            String line = null;
            for ( int count = 0; ( line = reader.readLine() ) != null && count < maxNumberOfLines;
                    count++ )
            {
                writer.println( line );
            }
            writer.close();
            return stringWriter.getBuffer().toString();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Can't happen", e );
        }
    }

    public static boolean stackTraceContains( Thread thread, Predicate<StackTraceElement> predicate )
    {
        for ( StackTraceElement element : thread.getStackTrace() )
        {
            if ( predicate.test( element ) )
            {
                return true;
            }
        }
        return false;
    }

    public static boolean currentStackTraceContains( Predicate<StackTraceElement> predicate )
    {
        return stackTraceContains( Thread.currentThread(), predicate );
    }

    public static Predicate<StackTraceElement> classNameIs( final String className )
    {
        return item -> item.getClassName().equals( className );
    }

    public static Predicate<StackTraceElement> classNameContains( final String classNamePart )
    {
        return item -> item.getClassName().contains( classNamePart );
    }

    public static Predicate<StackTraceElement> classIs( final Class<?> cls )
    {
        return item -> item.getClassName().equals( cls.getName() );
    }

    public static Predicate<StackTraceElement> classNameAndMethodAre( final String className,
            final String methodName )
    {
        return item -> item.getClassName().equals( className ) && item.getMethodName().equals( methodName );
    }

    public static Predicate<StackTraceElement> classAndMethodAre( final Class<?> cls, final String methodName )
    {
        return item -> item.getClassName().equals( cls.getName() ) && item.getMethodName().equals( methodName );
    }

    public static Predicate<StackTraceElement> methodIs( String methodName )
    {
        return item -> item.getMethodName().equals( methodName );
    }

    public static class StackTracer
    {
        private final Map<CallStack, AtomicInteger> uniqueStackTraces = new HashMap<>();
        private boolean considerMessages = true;

        /**
         * Returns {@link AtomicInteger} for the unique stack trace provided. It gets updated
         * as more are added.
         */
        public AtomicInteger add( Throwable t )
        {
            CallStack key = new CallStack( t, considerMessages );
            AtomicInteger count = uniqueStackTraces.computeIfAbsent( key, k -> new AtomicInteger() );
            count.incrementAndGet();
            return count;
        }

        public void print( PrintStream out, int interestThreshold )
        {
            System.out.println( "Printing stack trace counts:" );
            long total = 0;
            for ( Map.Entry<CallStack, AtomicInteger> entry : uniqueStackTraces.entrySet() )
            {
                if ( entry.getValue().get() >= interestThreshold )
                {
                    out.println( entry.getValue() + " times:" );
                    entry.getKey().stackTrace.printStackTrace( out );
                }
                total += entry.getValue().get();
            }
            out.println( "------" );
            out.println( "Total:" + total );
        }

        public StackTracer printAtShutdown( final PrintStream out, final int interestThreshold )
        {
            Runtime.getRuntime().addShutdownHook( new Thread( () -> print( out, interestThreshold ) ) );
            return this;
        }

        public StackTracer ignoreMessages()
        {
            considerMessages = false;
            return this;
        }
    }

    public static class CallStack
    {
        private final String message;
        private final Throwable stackTrace;
        private final StackTraceElement[] elements;
        private final boolean considerMessage;

        public CallStack( Throwable stackTrace, boolean considerMessage )
        {
            this.message = stackTrace.getMessage();
            this.stackTrace = stackTrace;
            this.considerMessage = considerMessage;
            this.elements = stackTrace.getStackTrace();
        }

        public CallStack( StackTraceElement[] elements, String message )
        {
            this.message = message;
            this.stackTrace = null;
            this.elements = elements;
            this.considerMessage = true;
        }

        @Override
        public int hashCode()
        {
            int hashCode = message == null || !considerMessage ? 31 : message.hashCode();
            for ( StackTraceElement element : elements )
            {
                hashCode = hashCode * 9 + element.hashCode();
            }
            return hashCode;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( !( obj instanceof CallStack) )
            {
                return false;
            }

            CallStack o = (CallStack) obj;
            if ( considerMessage )
            {
                if ( message == null )
                {
                    if ( o.message != null )
                    {
                        return false;
                    }
                }
                else if ( !message.equals( o.message ) )
                {
                    return false;
                }
            }
            if ( elements.length != o.elements.length )
            {
                return false;
            }
            for ( int i = 0; i < elements.length; i++ )
            {
                if ( !elements[i].equals( o.elements[i] ) )
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append( stackTrace != null ? stackTrace.getClass().getName() + ": " : "" )
                    .append( message != null ? message : "" );
            for ( StackTraceElement element : elements )
            {
                builder.append( format( "%n" ) ).append( "    at " ).append( element.toString() );
            }
            return builder.toString();
        }
    }

    private static boolean enabledAssertions()
    {
        boolean enabled = false;
        //noinspection AssertWithSideEffects,ConstantConditions
        assert enabled = true : "A trick to set this variable to true if assertions are enabled";
        //noinspection ConstantConditions
        return enabled;
    }

    /**
     * Super simple utility for determining where most time is spent when you don't know where to even start.
     * It could be used to home in on right place in a test or in a sequence of operations or similar.
     */
    public abstract static class Timer
    {
        private final TimeUnit unit;
        private long startTime;

        protected Timer( TimeUnit unit )
        {
            this.unit = unit;
            this.startTime = currentTime();
        }

        protected abstract long currentTime();

        public void reset()
        {
            startTime = currentTime();
        }

        public void at( String point )
        {
            long duration = currentTime() - startTime;
            System.out.println( duration( unit.toMillis( duration ) ) + " @ " + point );
            startTime = currentTime();
        }

        public static Timer millis()
        {
            return new Millis();
        }

        private static class Millis extends Timer
        {
            Millis()
            {
                super( TimeUnit.MILLISECONDS );
            }

            @Override
            protected long currentTime()
            {
                return currentTimeMillis();
            }
        }

        public static Timer nanos()
        {
            return new Nanos();
        }

        private static class Nanos extends Timer
        {
            Nanos()
            {
                super( TimeUnit.NANOSECONDS );
            }

            @Override
            protected long currentTime()
            {
                return nanoTime();
            }
        }
    }

    public static long time( long startTime, String message )
    {
        System.out.println( duration( currentTimeMillis() - startTime ) + ": " + message );
        return currentTimeMillis();
    }
}
