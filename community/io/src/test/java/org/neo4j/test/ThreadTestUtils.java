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

import java.util.EnumSet;
import java.util.Map;

public class ThreadTestUtils
{
    public static Thread fork( Runnable runnable )
    {
        String name = "Forked-from-" + Thread.currentThread().getName();
        Thread thread = new Thread( runnable, name );
        thread.start();
        return thread;
    }

    public static void awaitThreadState( Thread thread, long maxWaitMillis, Thread.State first, Thread.State... rest )
    {
        EnumSet<Thread.State> set = EnumSet.of( first, rest );
        long deadline = maxWaitMillis + System.currentTimeMillis();
        Thread.State currentState;
        do
        {
            currentState = thread.getState();
            if ( System.currentTimeMillis() > deadline )
            {
                throw new AssertionError(
                        "Timed out waiting for thread state of <" +
                                set + ">: " + thread + " (state = " +
                                thread.getState() + ")" );
            }
        }
        while ( !set.contains( currentState ) );
    }

    public static void dumpAllStackTraces()
    {
        synchronized ( System.err )
        {
            Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
            for ( Map.Entry<Thread, StackTraceElement[]> entry : allStackTraces.entrySet() )
            {
                System.err.println( "Stack Trace for " + entry.getKey().getName() );
                StackTraceElement[] elements = entry.getValue();
                for ( StackTraceElement element : elements )
                {
                    System.err.println( "\tat " + element.toString() );
                }
                System.err.println();
            }
        }
    }
}
