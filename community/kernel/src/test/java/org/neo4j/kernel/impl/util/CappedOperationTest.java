/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.util.CappedOperation.count;
import static org.neo4j.kernel.impl.util.CappedOperation.differentItems;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.impl.util.CappedOperation.Switch;

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

        operation.event( "test" ); // 1
        assertEquals( 0, triggerCount.get() );
        operation.event( "test" ); // 2 ... and reset
        assertEquals( 1, triggerCount.get() );

        operation.event( "test" ); // 1
        assertEquals( 1, triggerCount.get() );
        operation.event( "test" ); // 2
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

    @Test
    public void shouldTriggerBasedOnTime() throws Exception
    {
        // GIVEN
        AtomicInteger triggerCount = new AtomicInteger();
        FakeClock clock = new FakeClock();
        CappedOperation<String> operation = countingCappedOperations( triggerCount,
                CappedOperation.time( clock, 1500, TimeUnit.MILLISECONDS ) );

        // WHEN/THEN
        // event happens right away
        operation.event( "event" );
        assertEquals( 0, triggerCount.get() );

        // after a little while, but before the threshold
        clock.forward( 1499, TimeUnit.MILLISECONDS );
        operation.event( "event" );
        assertEquals( 0, triggerCount.get() );

        // right after the threshold
        clock.forward( 2, TimeUnit.MILLISECONDS );
        operation.event( "event" );
        assertEquals( 1, triggerCount.get() );

        // after another threshold
        clock.forward( 1600, TimeUnit.MILLISECONDS );
        operation.event( "event" );
        assertEquals( 2, triggerCount.get() );
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
