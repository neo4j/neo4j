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

import org.neo4j.collection.primitive.PrimitiveLongLongMap;
import org.neo4j.memory.GlobalMemoryTracker;
import org.neo4j.resources.HeapAllocation;

import static java.lang.Thread.currentThread;
import static org.neo4j.collection.primitive.Primitive.offHeapLongLongMap;

public class FakeHeapAllocation extends HeapAllocation implements TestRule
{
    private final PrimitiveLongLongMap allocation = offHeapLongLongMap( GlobalMemoryTracker.INSTANCE );

    @Override
    public long allocatedBytes( long threadId )
    {
        return Math.max( 0, allocation.get( threadId ) );
    }

    public FakeHeapAllocation add( long bytes )
    {
        return add( currentThread().getId(), bytes );
    }

    public FakeHeapAllocation add( long threadId, long bytes )
    {
        allocation.put( threadId, allocatedBytes( threadId ) + bytes );
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
                    allocation.close();
                }
            }
        };
    }
}
