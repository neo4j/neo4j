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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * When debugging it can sometimes be useful to know from where a method is being called, and how often.
 * Set a breakpoint in that method and evaluate an expression that calls {@link #trace(String)}. Before the test case
 * ends, set another breakpoint that suspends the VM and call asString() to get printouts of all ways in which the method
 * was invoked.
 */
public class TraceCounter
{
    private static Map<String, Map<List<StackTraceElement>, AtomicInteger>> traceCounts = new HashMap<>(  );

    public static String trace(String name)
    {
        Map<List<StackTraceElement>, AtomicInteger> namedTraces = traceCounts.get( name );

        if (namedTraces == null)
        {
            namedTraces = new HashMap<>(  );
            traceCounts.put(name, namedTraces);
        }

        List<StackTraceElement> trace = Arrays.asList(new Exception().getStackTrace());

        AtomicInteger count = namedTraces.get(trace);
        if (count == null)
        {
            count = new AtomicInteger(  );
            namedTraces.put(trace, count);
        }

        count.incrementAndGet();

        return "";
    }

    public static void clear()
    {
        traceCounts.clear();
    }

    public static void print(PrintWriter out)
    {
        for ( Map.Entry<String, Map<List<StackTraceElement>, AtomicInteger>> stringMapEntry : traceCounts.entrySet() )
        {
            out.println(stringMapEntry.getKey());
            for ( Map.Entry<List<StackTraceElement>, AtomicInteger> listAtomicIntegerEntry : stringMapEntry.getValue
                    ().entrySet() )
            {
                out.println(listAtomicIntegerEntry.getValue().get()+":");
                for ( StackTraceElement stackTraceElement : listAtomicIntegerEntry.getKey() )
                {
                    out.println(stackTraceElement);
                }
                out.println();
            }

            out.println("---------------------------------");
        }
    }

    public static String asString()
    {
        StringWriter writer = new StringWriter(  );
        PrintWriter print = new PrintWriter( writer );
        print(print);
        print.close();
        return writer.toString();
    }
}
