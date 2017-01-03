/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongLongMap;
import org.neo4j.time.CpuClock;

public class FakeCpuClock extends CpuClock
{
    private final PrimitiveLongLongMap cpuTimes = Primitive.offHeapLongLongMap();

    @Override
    public long cpuTimeNanos( long threadId )
    {
        return Math.max( 0, cpuTimes.get( threadId ) );
    }

    public void add( long delta, TimeUnit unit )
    {
        add( unit.toNanos( delta ) );
    }

    public void add( long nanos )
    {
        add( Thread.currentThread().getId(), nanos );
    }

    public void add( long threadId, long nanos )
    {
        cpuTimes.put( threadId, cpuTimeNanos( threadId ) + nanos );
    }
}
