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
package org.neo4j.unsafe.impl.batchimport.store;

import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.Factory;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.unsafe.impl.batchimport.store.io.Ring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RingTest
{
    @Test
    public void shouldWaitForFreeItem() throws Exception
    {
        // GIVEN
        final Ring<Item> ring = new Ring<>( 2, new Factory<Item>()
        {
            @Override
            public Item newInstance()
            {
                return new Item();
            }
        } );

        // WHEN/THEN
        Item first = ring.next();
        assertEquals( 0, first.id );
        Item second = ring.next();
        assertEquals( 1, second.id );
        Future<Item> thirdFuture = t2.execute( new WorkerCommand<Void,Item>()
        {
            @Override
            public Item doWork( Void state ) throws Exception
            {
                return ring.next();
            }
        } );
        assertThat( t2, OtherThreadRule.isThreadState( Thread.State.TIMED_WAITING ) );
        ring.free( first );
        Item third = thirdFuture.get();
        assertEquals( 0, third.id );
    }

    private int itemIndex;

    private class Item
    {
        private final int id;

        public Item()
        {
            this.id = itemIndex++;
        }
    }

    public final @Rule OtherThreadRule<Void> t2 = new OtherThreadRule<>();
}
