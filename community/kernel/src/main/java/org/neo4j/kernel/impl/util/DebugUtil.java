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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.Predicate;

import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

import static org.neo4j.helpers.Exceptions.stringify;

public class DebugUtil
{
    public static void printShortStackTrace( Throwable cause, int maxNumberOfStackLines )
    {
        System.out.println( firstLinesOf( stringify( cause ), maxNumberOfStackLines+1 ) );
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

    public static boolean currentStackTraceContains( Predicate<StackTraceElement> predicate )
    {
        for ( StackTraceElement element : Thread.currentThread().getStackTrace() )
        {
            if ( predicate.accept( element ) )
            {
                return true;
            }
        }
        return false;
    }

    public static Predicate<StackTraceElement> classNameIs( final String className )
    {
        return new Predicate<StackTraceElement>()
        {
            @Override
            public boolean accept( StackTraceElement item )
            {
                return item.getClassName().equals( className );
            }
        };
    }

    public static Predicate<StackTraceElement> classNameContains( final String classNamePart )
    {
        return new Predicate<StackTraceElement>()
        {
            @Override
            public boolean accept( StackTraceElement item )
            {
                return item.getClassName().contains( classNamePart );
            }
        };
    }

    public static Predicate<StackTraceElement> classIs( final Class<?> cls )
    {
        return new Predicate<StackTraceElement>()
        {
            @Override
            public boolean accept( StackTraceElement item )
            {
                return item.getClassName().equals( cls.getName() );
            }
        };
    }

    public static Predicate<StackTraceElement> classNameAndMethodAre( final String className,
            final String methodName )
    {
        return new Predicate<StackTraceElement>()
        {
            @Override
            public boolean accept( StackTraceElement item )
            {
                return item.getClassName().equals( className ) && item.getMethodName().equals( methodName );
            }
        };
    }

    public static Predicate<StackTraceElement> classAndMethodAre( final Class<?> cls, final String methodName )
    {
        return new Predicate<StackTraceElement>()
        {
            @Override
            public boolean accept( StackTraceElement item )
            {
                return item.getClassName().equals( cls.getName() ) && item.getMethodName().equals( methodName );
            }
        };
    }

    public static class StackTracer
    {
        private final Map<Stack, AtomicInteger> uniqueStackTraces = new HashMap<>();
        private boolean considerMessages = true;

        public void add( Throwable t )
        {
            Stack key = new Stack( t, considerMessages );
            AtomicInteger count = uniqueStackTraces.get( key );
            if ( count == null )
            {
                count = new AtomicInteger();
                uniqueStackTraces.put( key, count );
            }
            count.incrementAndGet();
        }

        public void print( PrintStream out, int interestThreshold )
        {
            System.out.println( "Printing stack trace counts:" );
            long total = 0;
            for ( Map.Entry<Stack, AtomicInteger> entry : uniqueStackTraces.entrySet() )
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
            Runtime.getRuntime().addShutdownHook( new Thread()
            {
                @Override
                public void run()
                {
                    print( out, interestThreshold );
                }
            } );
            return this;
        }

        public StackTracer ignoreMessages()
        {
            considerMessages = false;
            return this;
        }
    }

    private static class Stack
    {
        private final Throwable stackTrace;
        private final StackTraceElement[] elements;
        private final boolean considerMessage;

        Stack( Throwable stackTrace, boolean considerMessage )
        {
            this.stackTrace = stackTrace;
            this.considerMessage = considerMessage;
            this.elements = stackTrace.getStackTrace();
        }

        @Override
        public int hashCode()
        {
            int hashCode = stackTrace.getMessage() == null || !considerMessage ? 31 :
                stackTrace.getMessage().hashCode();
            for ( StackTraceElement element : stackTrace.getStackTrace() )
            {
                hashCode = hashCode * 9 + element.hashCode();
            }
            return hashCode;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( !( obj instanceof Stack) )
            {
                return false;
            }

            Stack o = (Stack) obj;
            if ( considerMessage )
            {
                if ( stackTrace.getMessage() == null )
                {
                    if ( o.stackTrace.getMessage() != null )
                    {
                        return false;
                    }
                }
                else if ( !stackTrace.getMessage().equals( o.stackTrace.getMessage() ) )
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
    }

    public static class CallCounter<T>
    {
        private final Map<T, AtomicInteger> calls = new HashMap<>();
        private final String name;

        public CallCounter( String name )
        {
            this.name = name;
        }

        public CallCounter<T> printAtShutdown( final PrintStream out )
        {
            Runtime.getRuntime().addShutdownHook( new Thread()
            {
                @Override
                public void run()
                {
                    print( out );
                }
            } );
            return this;
        }

        public void inc( T key )
        {
            AtomicInteger count = calls.get( key );
            if ( count == null )
            {
                count = new AtomicInteger();
                calls.put( key, count );
            }
            count.incrementAndGet();
        }

        private void print( PrintStream out )
        {
            out.println( "Calls made regarding " + name + ":" );
            for ( Map.Entry<T, AtomicInteger> entry : calls.entrySet() )
            {
                out.println( "\t" + entry.getKey() + ": " + entry.getValue() );
            }
        }
    }

    /**
     * Only enabled iff -ea is enabled.
     *
     * Tries to track down which test that got us to the point in execution we're at right now by analyzing the
     * stack trace elements of the current thread. If no test was found on the stack trace or if -ea is not enabled,
     * then an empty string is returned.
     *
     * Basically it will try to find the first public non-static method with a {@code @Test} annotation
     * and, if found, return {@code <simple-class-name>#<test-method-name>}.
     *
     * This method can be added to places where there's a suspicion that tests forget to close resources,
     * for example threads, where threads can have this string added as the last part of its name. And it can be
     * left there in production code as well, since it will be dormant if the JVM hasn't got assertions enabled.
     */
    public static String trackTest()
    {
        boolean track = false;
        assert (track = true) : "A trick to set this variable to true if assertions are enabled";

        if ( track )
        {
            for ( StackTraceElement element : Thread.currentThread().getStackTrace() )
            {
                try
                {
                    String className = element.getClassName();
                    Class<?> cls = Class.forName( className );
                    Method method = cls.getDeclaredMethod( element.getMethodName() );
                    if ( !isStatic( method.getModifiers() ) &&
                         isPublic( method.getModifiers() ) &&
                         hasTestAnnotation( method ) )
                    {
                        return " @ " + simpleClassName( className ) + "#" + element.getMethodName();
                    }
                }
                catch ( ClassNotFoundException | SecurityException | NoSuchMethodException e )
                {
                    // This is so weird, but hey, who am I to judge all ours precious JVM and class loader
                    continue;
                }
            }
        }
        return "";
    }

    private static String simpleClassName( String className )
    {
        return className.indexOf( '.' ) == -1
                ? className
                : className.substring( className.lastIndexOf( '.' )+1 );
    }

    private static boolean hasTestAnnotation( Method method )
    {
        for ( Annotation annotation : method.getAnnotations() )
        {
            if ( annotation.annotationType().getSimpleName().equals( "Test" ) )
            {
                return true;
            }
        }
        return false;
    }
}
