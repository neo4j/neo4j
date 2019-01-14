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
package org.neo4j.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongLongMap;
import org.neo4j.memory.GlobalMemoryTracker;
import org.neo4j.resources.CpuClock;

public class FakeCpuClock extends CpuClock implements TestRule
{
    public static final CpuClock NOT_AVAILABLE = new CpuClock()
    {
        @Override
        public long cpuTimeNanos( long threadId )
        {
            return -1;
        }
    };
    private final PrimitiveLongLongMap cpuTimes = Primitive.offHeapLongLongMap( GlobalMemoryTracker.INSTANCE );

    @Override
    public long cpuTimeNanos( long threadId )
    {
        return Math.max( 0, cpuTimes.get( threadId ) );
    }

    public FakeCpuClock add( long delta, TimeUnit unit )
    {
        return add( unit.toNanos( delta ) );
    }

    public FakeCpuClock add( long nanos )
    {
        return add( Thread.currentThread().getId(), nanos );
    }

    public FakeCpuClock add( long threadId, long nanos )
    {
        cpuTimes.put( threadId, cpuTimeNanos( threadId ) + nanos );
        return this;
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    base.evaluate();
                }
                finally
                {
                    cpuTimes.close();
                }
            }
        };
    }
}
