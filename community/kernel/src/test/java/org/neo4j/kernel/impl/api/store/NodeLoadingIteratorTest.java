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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.function.ThrowingLongFunction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.NodeItem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptyIterator;

public class NodeLoadingIteratorTest
{
    @Test
    public void shouldHandleAnEmptyIterator() throws Exception
    {
        // given
        NodeLoadingIterator iterator = new NodeLoadingIterator( emptyIterator(), id ->
        {
            throw new IllegalStateException( "" );
        } );

        // when - then
        assertNoMoreElements( iterator );
    }

    @Test
    public void shouldHandleANonEmptyIterator() throws Exception
    {
        // given
        Map<Long,Cursor<NodeItem>> map = new HashMap<>( 3 );
        map.put( 1L, mockCursor() );
        map.put( 2L, mockCursor() );
        map.put( 3L, mockCursor() );
        PrimitiveLongIterator inner = PrimitiveLongCollections.iterator( 1, 2, 3 );
        NodeLoadingIterator iterator = new NodeLoadingIterator( inner, createMapping( map ) );

        // when - then
        for ( long i = 1; i <= 3; i++ )
        {
            assertTrue( iterator.hasNext() );
            assertEquals( map.get( i ), iterator.next() );
        }

        assertNoMoreElements( iterator );
    }

    @Test
    public void shouldHandleANonEmptyIteratorWithNotFoundEntities() throws Exception
    {
        // given
        Map<Long,Cursor<NodeItem>> map = new HashMap<>( 3 );
        map.put( 1L, mockCursor() );
        map.put( 2L, null );
        map.put( 3L, mockCursor() );
        PrimitiveLongIterator inner = PrimitiveLongCollections.iterator( 1, 2, 3 );
        NodeLoadingIterator iterator = new NodeLoadingIterator( inner, createMapping( map ) );

        // when - then
        for ( long i = 1; i <= 2; i++ )
        {
            assertTrue( iterator.hasNext() );

            assertEquals( map.get( i + (i - 1) /* 1 -> 1, 2 -> 3 */ ), iterator.next() );
        }

        assertNoMoreElements( iterator );
    }

    private void assertNoMoreElements( NodeLoadingIterator iterator )
    {
        assertFalse( iterator.hasNext() );
        try
        {
            iterator.next();
            fail( "should have thrown ");
        }
        catch ( NoSuchElementException e )
        {
            // good
        }
    }

    private ThrowingLongFunction<Cursor<NodeItem>,EntityNotFoundException> createMapping(
            Map<Long,Cursor<NodeItem>> map )
    {
        return id ->
        {
            if ( !map.containsKey( id ) )
            {
                throw new IllegalStateException( "wat!?" );
            }
            Cursor<NodeItem> cursor = map.get( id );
            if ( cursor == null )
            {
                throw new EntityNotFoundException( EntityType.NODE, id );
            }
            return cursor;
        };
    }

    @SuppressWarnings( "unchecked" )
    private Cursor<NodeItem> mockCursor()
    {
        return (Cursor<NodeItem>) mock( Cursor.class );
    }
}
