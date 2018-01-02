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
package org.neo4j.unsafe.impl.batchimport.store;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;

import org.neo4j.kernel.impl.util.SimplePool;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class SimplePoolTest
{
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>();

    @Test
    public void shouldWaitForFreeItem() throws Exception
    {
        // GIVEN
        final SimplePool<Item> pool = new SimplePool<>( items( 2 ) );

        // WHEN/THEN
        Item first = pool.acquire();
        assertEquals( 0, first.id );
        Item second = pool.acquire();
        assertEquals( 1, second.id );
        Future<Item> thirdFuture = t2.execute( new WorkerCommand<Void, Item>()
        {
            @Override
            public Item doWork( Void state ) throws Exception
            {
                return pool.acquire();
            }
        } );
        assertThat( t2, OtherThreadRule.isThreadState( Thread.State.TIMED_WAITING ) );
        pool.release( first );
        Item third = thirdFuture.get();
        assertEquals( 0, third.id );
    }

    @Test
    public void shouldNotRelyOnAcquireReleaseOrdering()
    {
        // Given
        SimplePool<Item> pool = new SimplePool<>( items( 2 ) );

        // When
        Item first = pool.acquire();
        Item second = pool.acquire();
        pool.release( second );

        // Then
        assertNotEquals( first, pool.acquire() );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWhenNonPooledObjectIsReleased()
    {
        // Given
        SimplePool<Item> pool = new SimplePool<>( items( 2 ) );

        // When/Then throw
        pool.release( new Item( 42 ) );
    }

    private static class Item
    {
        final int id;

        Item( int id )
        {
            this.id = id;
        }
    }

    private Item[] items( int length )
    {
        Item[] items = new Item[length];
        for ( int i = 0; i < length; i++ )
        {
            items[i] = new Item( i );
        }
        return items;
    }
}
