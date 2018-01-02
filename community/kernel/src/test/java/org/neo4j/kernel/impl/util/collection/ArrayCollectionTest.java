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
package org.neo4j.kernel.impl.util.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ArrayCollectionTest
{
    @Test
    public void shouldAddItems() throws Exception
    {
        // GIVEN
        Collection<String> collection = new ArrayCollection<>( 5 );
        assertEquals( 0, collection.size() );
        assertTrue( collection.isEmpty() );

        // WHEN
        collection.add( "1" );
        collection.add( "2" );
        collection.add( "3" );

        // THEN
        assertTrue( collection.contains( "1" ) );
        assertTrue( collection.contains( "2" ) );
        assertTrue( collection.contains( "3" ) );
        assertFalse( collection.contains( "4" ) );
        assertEquals( 3, collection.size() );
        assertFalse( collection.isEmpty() );
    }

    @Test
    public void shouldGrowWithMoreItms() throws Exception
    {
        // GIVEN
        int initialCapacity = 3;
        Collection<Integer> collection = new ArrayCollection<>( initialCapacity );
        for ( int i = 0; i < initialCapacity; i++ )
        {
            collection.add( i );
        }

        // WHEN
        collection.add( 10 );

        // THEN
        Iterator<Integer> iterator = collection.iterator();
        for ( int i = 0; i < initialCapacity; i++ )
        {
            assertEquals( i, iterator.next().intValue() );
        }
        assertEquals( 10, iterator.next().intValue() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldClear() throws Exception
    {
        // GIVEN
        Collection<String> collection = new ArrayCollection<>( 10 );
        for ( int i = 0; i < 5; i++ )
        {
            collection.add( String.valueOf( i ) );
        }

        // WHEN
        collection.clear();

        // THEN
        assertTrue( collection.isEmpty() );
        assertEquals( 0, collection.size() );
        assertFalse( collection.iterator().hasNext() );

        // and WHEN adding more after previously clearing it
        for ( int i = 50; i < 54; i++ )
        {
            collection.add( String.valueOf( i ) );
        }

        // THEN only the new values should be there
        Iterator<String> iterator = collection.iterator();
        assertEquals( "50", iterator.next() );
        assertEquals( "51", iterator.next() );
        assertEquals( "52", iterator.next() );
        assertEquals( "53", iterator.next() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldAddAllBeyondCapacity() throws Exception
    {
        // GIVEN
        Collection<Integer> collection = new ArrayCollection<>( 5 );

        // WHEN
        collection.addAll( intCollection( 0, 15 ) );

        // THEN
        Iterator<Integer> iterator = collection.iterator();
        for ( int i = 0; i < 15; i++ )
        {
            assertEquals( i, iterator.next().intValue() );
        }
        assertFalse( iterator.hasNext() );
    }

    private Collection<? extends Integer> intCollection( int startingAt, int size )
    {
        Collection<Integer> ints = new ArrayList<>();
        for ( int i = 0; i < size; i++ )
        {
            ints.add( startingAt+i );
        }
        return ints;
    }
}
