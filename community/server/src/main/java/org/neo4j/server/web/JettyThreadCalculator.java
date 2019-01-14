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
package org.neo4j.server.web;

public class JettyThreadCalculator
{
    // Any higher and maxCapacity will overflow
    public static final int MAX_THREADS = 44738;

    private int acceptors;
    private int selectors;
    private int minThreads;
    private int maxThreads;
    private int maxCapacity;

    public JettyThreadCalculator( int jettyMaxThreads )
    {
        if ( jettyMaxThreads < 1 )
        {
            throw new IllegalArgumentException( "Max threads can't be less than 1" );
        }
        else if ( jettyMaxThreads > MAX_THREADS )
        {
            throw new IllegalArgumentException( String.format( "Max threads can't exceed %d", MAX_THREADS ) );
        }
        // transactionThreads = N / 5
        int transactionThreads = jettyMaxThreads / 5;
        // acceptors = N / 15
        acceptors = Math.max( 1, transactionThreads / 3 );
        // selectors = N * 2 / 15
        selectors = Math.max( 1, transactionThreads - acceptors );
        if ( jettyMaxThreads < 4 )
        {
            acceptors = 1;
            selectors = 1;
        }
        else if ( jettyMaxThreads == 4 )
        {
            acceptors = 1;
            selectors = 2;
        }
        else if ( jettyMaxThreads <= 8 )
        {
            acceptors = 2;
            selectors = 3;
        }
        else if ( jettyMaxThreads <= 16 )
        {
            transactionThreads = jettyMaxThreads / 4;
            acceptors = Math.max( 2, transactionThreads / 3 );
            selectors = Math.max( 3, transactionThreads - acceptors );
        }
        // minThreads = N / 5 + 2 * N / 5
        // max safe value for this = 5 / 3 * INT.MAX = INT.MAX
        minThreads = Math.max( 2, transactionThreads ) + (acceptors + selectors) * 2;
        // maxThreads = N + N / 5
        // max Safe value for this = 6 / 5 * INT.MAX = INT.MAX
        maxThreads = Math.max( jettyMaxThreads - selectors - acceptors, 8 ) + (acceptors + selectors) * 2;
        // maxCapacity = (N - N / 5) * 60_000
        // max safe value = 44738
        maxCapacity = (maxThreads - (selectors + acceptors) * 2) * 1000 * 60; // threads * 1000 req/s * 60 s
    }

    public int getAcceptors()
    {
        return acceptors;
    }

    public int getSelectors()
    {
        return selectors;
    }

    public int getMinThreads()
    {
        return minThreads;
    }

    public int getMaxThreads()
    {
        return maxThreads;
    }

    public int getMaxCapacity()
    {
        return maxCapacity;
    }
}
