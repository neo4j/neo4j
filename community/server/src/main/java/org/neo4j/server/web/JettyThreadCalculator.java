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
package org.neo4j.server.web;

public class JettyThreadCalculator
{
    private int acceptors;
    private int selectors;
    private int minThreads;
    private int maxThreads;
    private int maxCapacity;

    public JettyThreadCalculator( int jettyMaxThreads )
    {
        int transactionThreads = jettyMaxThreads / 5;
        acceptors = Math.max( 1, transactionThreads / 3 );
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
        minThreads = Math.max( 2, transactionThreads ) + (acceptors + selectors) * 2;
        maxThreads = Math.max( (jettyMaxThreads - selectors - acceptors), 8 ) + (acceptors + selectors) * 2;
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
