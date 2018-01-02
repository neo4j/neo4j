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
package org.neo4j.consistency.statistics;

import org.neo4j.helpers.Format;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

public class VerboseStatistics implements Statistics
{
    private final AccessStatistics accessStatistics;
    private final Counts counts;
    private final Log logger;
    private long startTime;

    public VerboseStatistics( AccessStatistics accessStatistics, Counts counts, Log logger )
    {
        this.accessStatistics = accessStatistics;
        this.counts = counts;
        this.logger = logger;
    }

    @Override
    public void print( String name )
    {
        String accessStr = accessStatistics.getAccessStatSummary();
        logger.info( format( "=== %s ===", name ) );
        logger.info( format( "I/Os%n%s", accessStr ) );
        logger.info( counts.toString() );
        logger.info( memoryStats() );
        logger.info( "Done in  " + Format.duration( currentTimeMillis() - startTime ) );
    }

    @Override
    public void reset()
    {
        accessStatistics.reset();
        counts.reset();
        startTime = currentTimeMillis();
    }

    private static String memoryStats()
    {
        Runtime runtime = Runtime.getRuntime();
        return format( "Memory[used:%s, free:%s, total:%s, max:%s]",
                Format.bytes( runtime.totalMemory() - runtime.freeMemory() ),
                Format.bytes( runtime.freeMemory() ),
                Format.bytes( runtime.totalMemory() ),
                Format.bytes( runtime.maxMemory() ) );
    }

    @Override
    public Counts getCounts()
    {
        return counts;
    }
}
