/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.perftest.enterprise.ccheck;

import java.io.IOException;

class TimeLogger implements TimingProgress.Visitor
{
    private final TimingProgress.Visitor next;

    public TimeLogger( TimingProgress.Visitor next )
    {
        this.next = next;
    }

    static double nanosToMillis( long nanoTime )
    {
        return nanoTime / 1000000.0;
    }

    @Override
    public void beginTimingProgress( long totalElementCount, long totalTimeNanos ) throws IOException
    {
        // TODO please pass the PrintStream as an argument
        System.out.printf( "Processed %d elements in %.3f ms%n",
                totalElementCount, nanosToMillis( totalTimeNanos ) );
        next.beginTimingProgress( totalElementCount, totalTimeNanos );
    }

    @Override
    public void phaseTimingProgress( String phase, long elementCount, long timeNanos ) throws IOException
    {
        next.phaseTimingProgress( phase, elementCount, timeNanos );
    }

    @Override
    public void endTimingProgress() throws IOException
    {
        next.endTimingProgress();
    }
}
