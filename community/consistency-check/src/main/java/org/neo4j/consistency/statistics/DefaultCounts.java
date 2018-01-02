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

import java.util.Arrays;

import static java.lang.String.format;

public class DefaultCounts implements Counts
{
    private final long[][] counts;

    public DefaultCounts( int threads )
    {
        counts = new long[Type.values().length][threads];
    }

    private long[] counts( Type type )
    {
        return counts[type.ordinal()];
    }

    @Override
    public long sum( Type type )
    {
        long[] all = counts[type.ordinal()];
        long total = 0;
        for ( long one : all )
        {
            total += one;
        }
        return total;
    }

    @Override
    public void incAndGet( Type type, int threadIndex )
    {
        counts( type )[threadIndex]++;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( "Counts:" );
        for ( Type type : Type.values() )
        {
            long sum = sum( type );
            if ( sum > 0 )
            {
                builder.append( format( "%n  %d %s", sum, type.name() ) );
            }
        }
        return builder.toString();
    }

    @Override
    public void reset()
    {
        for ( long[] c : counts )
        {
            Arrays.fill( c, 0 );
        }
    }
}
