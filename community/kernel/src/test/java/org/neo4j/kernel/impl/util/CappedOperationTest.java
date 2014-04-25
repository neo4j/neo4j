/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.neo4j.kernel.impl.util.CappedOperation.Switch;

import static org.junit.Assert.assertEquals;

import static org.neo4j.kernel.impl.util.CappedOperation.count;
import static org.neo4j.kernel.impl.util.CappedOperation.differentItems;

public class CappedOperationTest
{
    @Test
    public void shouldTriggerOnSingleSwitch() throws Exception
    {
        // GIVEN
        AtomicInteger triggerCount = new AtomicInteger();
        CappedOperation<String> operation = countingCappedOperations( triggerCount, count( 2 ) );

        // WHEN/THEN
        assertEquals( 0, triggerCount.get() );

        operation.event( "test" );
        assertEquals( 1, triggerCount.get() );
        operation.event( "test" );
        assertEquals( 1, triggerCount.get() );

        operation.event( "test" );
        assertEquals( 2, triggerCount.get() );
        operation.event( "test" );
        assertEquals( 2, triggerCount.get() );
    }

    @Test
    public void shouldTriggerOnDifferentItemsEvenIfCountSwitch() throws Exception
    {
        // GIVEN
        AtomicInteger triggerCount = new AtomicInteger();
        CappedOperation<String> operation = countingCappedOperations( triggerCount,
                count( 2 ), differentItems() );

        // WHEN/THEN
        operation.event( "test" );
        assertEquals( 1, triggerCount.get() );

        operation.event( "OTHER" );
        assertEquals( 2, triggerCount.get() );
        operation.event( "OTHER" );
        assertEquals( 2, triggerCount.get() );
        operation.event( "OTHER" );
        assertEquals( 3, triggerCount.get() );
    }

    private CappedOperation<String> countingCappedOperations( final AtomicInteger triggerCount,
            Switch... openers )
    {
        return new CappedOperation<String>( openers )
        {
            @Override
            protected void triggered( String event )
            {
                triggerCount.incrementAndGet();
            }
        };
    }
}
