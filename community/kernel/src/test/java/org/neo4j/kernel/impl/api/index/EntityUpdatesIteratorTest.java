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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EntityUpdatesIteratorTest
{

    @Test
    public void iterateOverEmptyNodeIds()
    {
        IndexStoreView storeView = Mockito.mock( IndexStoreView.class );
        PrimitiveLongIterator emptyIterator = PrimitiveLongCollections.emptyIterator();
        NodeUpdatesIterator nodeUpdatesIterator = new NodeUpdatesIterator( storeView, emptyIterator );
        assertFalse( nodeUpdatesIterator.hasNext() );
    }

    @Test
    public void iterateOverUpdatesWithNext()
    {
        IndexStoreView storeView = Mockito.mock( IndexStoreView.class );
        EntityUpdates entityUpdates1 = EntityUpdates.forEntity( 1 ).build();
        EntityUpdates entityUpdates2 = EntityUpdates.forEntity( 2 ).build();
        when( storeView.nodeAsUpdates( 1 ) ).thenReturn( entityUpdates1 );
        when( storeView.nodeAsUpdates( 2 ) ).thenReturn( entityUpdates2 );

        PrimitiveLongIterator nodeIdIterator = PrimitiveLongCollections.iterator( 1, 2 );
        NodeUpdatesIterator nodeUpdatesIterator = new NodeUpdatesIterator( storeView, nodeIdIterator );

        assertSame( entityUpdates1, nodeUpdatesIterator.next() );
        assertSame( entityUpdates2, nodeUpdatesIterator.next() );
        assertFalse( nodeUpdatesIterator.hasNext() );

        verify( storeView ).nodeAsUpdates( 1 );
        verify( storeView ).nodeAsUpdates( 2 );
    }

    @Test
    public void iterateOverUpdatesWithHasNext()
    {
        IndexStoreView storeView = Mockito.mock( IndexStoreView.class );
        EntityUpdates entityUpdates1 = EntityUpdates.forEntity( 1 ).build();
        EntityUpdates entityUpdates2 = EntityUpdates.forEntity( 2 ).build();
        when( storeView.nodeAsUpdates( 1 ) ).thenReturn( entityUpdates1 );
        when( storeView.nodeAsUpdates( 2 ) ).thenReturn( entityUpdates2 );

        Deque<EntityUpdates> updates = new ArrayDeque<>( Arrays.asList( entityUpdates1, entityUpdates2 ) );

        PrimitiveLongIterator nodeIdIterator = PrimitiveLongCollections.iterator( 1, 2 );
        NodeUpdatesIterator nodeUpdatesIterator = new NodeUpdatesIterator( storeView, nodeIdIterator );

        while ( nodeUpdatesIterator.hasNext() )
        {
            EntityUpdates entityUpdates = nodeUpdatesIterator.next();
            assertSame( updates.pop(), entityUpdates );
        }

        verify( storeView ).nodeAsUpdates( 1 );
        verify( storeView ).nodeAsUpdates( 2 );
    }
}
