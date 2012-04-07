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

import static java.lang.Thread.currentThread;
import static java.lang.Thread.getAllStackTraces;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DebugUtil
{
    public static void printShortStackTrace( Throwable cause, int maxNumberOfStackLines )
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter( stringWriter );
        cause.printStackTrace( writer );
        writer.close();
        String string = stringWriter.getBuffer().toString();
        System.out.println( firstLinesOf( string, maxNumberOfStackLines+1 ) );
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

    public static boolean currentStackTraceContains( String className, String method )
    {
        try
        {
            return currentStackTraceContains( Class.forName( className ), method );
        }
        catch ( ClassNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    public static boolean currentStackTraceContains( Class<?> cls, String method )
    {
        for ( StackTraceElement stack : getAllStackTraces().get( currentThread() ) )
        {
            if ( stack.getClassName().equals( cls.getName() ) && stack.getMethodName().equals( method ) )
                return true;
        }
        return false;
    }
    
    public static class StackTracer
    {
        private final Map<Stack, AtomicInteger> uniqueStackTraces = new HashMap<Stack, AtomicInteger>();
        
        public void add( Throwable t )
        {
            Stack key = new Stack( t );
            AtomicInteger count = uniqueStackTraces.get( key );
            if ( count == null )
            {
                count = new AtomicInteger();
                uniqueStackTraces.put( key, count );
            }
            count.incrementAndGet();
        }
        
        public void print( PrintStream out )
        {
            long total = 0;
            for ( Map.Entry<Stack, AtomicInteger> entry : uniqueStackTraces.entrySet() )
            {
                out.println( entry.getValue() + " times:" );
                entry.getKey().stackTrace.printStackTrace( out );
                total += entry.getValue().get();
            }
            out.println( "------" );
            out.println( "Total:" + total );
        }
        
        public StackTracer printAtShutdown( final PrintStream out )
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
    }
    
    private static class Stack
    {
        private final Throwable stackTrace;
        private final StackTraceElement[] elements;

        Stack( Throwable stackTrace )
        {
            this.stackTrace = stackTrace;
            this.elements = stackTrace.getStackTrace();
        }
        
        @Override
        public int hashCode()
        {
            int hashCode = stackTrace.getMessage().hashCode();
            for ( StackTraceElement element : stackTrace.getStackTrace() )
                hashCode = hashCode * 9 + element.hashCode();
            return hashCode;
        }
        
        @Override
        public boolean equals( Object obj )
        {
            if ( !( obj instanceof Stack) ) return false;
            
            Stack o = (Stack) obj;
            if ( !stackTrace.getMessage().equals( o.stackTrace.getMessage() ) ) return false;
            if ( elements.length != o.elements.length ) return false;
            for ( int i = 0; i < elements.length; i++ )
                if ( !elements[i].equals( o.elements[i] ) ) return false;
            return true;
        }
    }
}
