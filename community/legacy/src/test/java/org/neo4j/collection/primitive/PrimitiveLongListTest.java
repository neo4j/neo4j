/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.collection.primitive;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimitiveLongListTest
{

    @Test
    void newListIsEmpty()
    {
        assertTrue( new PrimitiveLongList().isEmpty() );
        assertTrue( new PrimitiveLongList( 12 ).isEmpty() );
    }

    @Test
    void newListHasZeroSize()
    {
        assertEquals( 0, new PrimitiveLongList().size() );
        assertEquals( 0, new PrimitiveLongList( 12 ).size() );
    }

    @Test
    void addingElementsChangeSize()
    {
        PrimitiveLongList longList = new PrimitiveLongList();
        longList.add( 1L );

        assertFalse( longList.isEmpty() );
        assertEquals( 1, longList.size() );

        longList.add( 2L );
        assertFalse( longList.isEmpty() );
        assertEquals( 2, longList.size() );

        longList.add( 3L );

        assertFalse( longList.isEmpty() );
        assertEquals( 3, longList.size() );
    }

    @Test
    void accessAddedElements()
    {
        PrimitiveLongList longList = new PrimitiveLongList();
        for ( long i = 1; i < 6L; i++ )
        {
            longList.add( i );
        }

        assertEquals( 5L, longList.get( 4 ) );
        assertEquals( 1L, longList.get( 0 ) );
    }

    @Test
    void throwExceptionOnAccessingNonExistentElement()
    {
        PrimitiveLongList longList = new PrimitiveLongList();
        assertThrows( IndexOutOfBoundsException.class, () -> longList.get( 0 ) );
    }

    @Test
    void iterateOverListElements()
    {
        PrimitiveLongList longList = new PrimitiveLongList();
        for ( long i = 0; i < 10L; i++ )
        {
            longList.add( i );
        }

        int iteratorElements = 0;
        long value = 0;
        PrimitiveLongIterator iterator = longList.iterator();
        while ( iterator.hasNext() )
        {
            iteratorElements++;
            assertEquals( value++, iterator.next() );
        }

        assertEquals( iteratorElements, longList.size() );
    }

    @Test
    void clearResetListSize()
    {
        PrimitiveLongList longList = new PrimitiveLongList();
        long size = 10;
        for ( long i = 0; i < 10L; i++ )
        {
            longList.add( i );
        }
        assertEquals( size, longList.size() );

        longList.clear();

        assertEquals( 0, longList.size() );
        assertTrue( longList.isEmpty() );
    }

    @Test
    void addAfterClear()
    {
        PrimitiveLongList longList = new PrimitiveLongList();
        longList.clear();

        longList.add( 1 );
        assertEquals( 1, longList.get( 0 ) );
        assertEquals( 1, longList.size() );
    }

    @Test
    void transformListToArray()
    {
        PrimitiveLongList longList = new PrimitiveLongList();
        long size = 24L;
        for ( long i = 0; i < size; i++ )
        {
            longList.add( i );
        }

        long[] longs = longList.toArray();
        assertEquals( size, longs.length );
        for ( int i = 0; i < longs.length; i++ )
        {
            assertEquals( i, longs[i] );
        }
    }

    @Test
    void holdLotsOfElements()
    {
        PrimitiveLongList longList = new PrimitiveLongList();
        long size = 13077L;
        for ( long i = 0; i < size; i++ )
        {
            longList.add( i );
        }

        assertEquals( size, longList.size() );
        for ( int i = 0; i < size; i++ )
        {
            assertEquals( i, longList.get( i ) );
        }
    }
}
