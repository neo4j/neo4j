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
package org.neo4j.helpers.collection;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MultiSetTest
{
    @Test
    public void anEmptySetContainsNothing()
    {
        // given
        Object aValue = new Object();

        // when
        MultiSet<Object> emptyMultiSet = new MultiSet<>();

        // then
        assertTrue( emptyMultiSet.isEmpty() );
        assertEquals( 0, emptyMultiSet.size() );
        assertEquals( 0, emptyMultiSet.uniqueSize() );
        assertFalse( emptyMultiSet.contains( aValue ) );
        assertEquals( 0, emptyMultiSet.count( aValue ) );
    }

    @Test
    public void shouldAddAnElementToTheMultiSet()
    {
        // given
        MultiSet<Object> multiSet = new MultiSet<>();
        Object value = new Object();

        // when
        long count = multiSet.add( value );

        // then
        assertEquals( 1, count );
        assertFalse( multiSet.isEmpty() );
        assertEquals( 1, multiSet.size() );
        assertEquals( 1, multiSet.uniqueSize() );
        assertTrue( multiSet.contains( value ) );
        assertEquals( 1, multiSet.count( value ) );
    }

    @Test
    public void shouldRemoveAnElementFromTheMultiSet()
    {
        // given
        MultiSet<Object> multiSet = new MultiSet<>();
        Object value = new Object();
        multiSet.add( value );

        // when
        long count = multiSet.remove( value );

        // then
        assertEquals( 0, count );
        assertTrue( multiSet.isEmpty() );
        assertEquals( 0, multiSet.size() );
        assertEquals( 0, multiSet.uniqueSize() );
        assertFalse( multiSet.contains( value ) );
        assertEquals( 0, multiSet.count( value ) );
    }

    @Test
    public void shouldAddAnElementTwice()
    {
        // given
        MultiSet<Object> multiSet = new MultiSet<>();
        Object value = new Object();
        multiSet.add( value );

        // when
        long count = multiSet.add( value );

        // then
        assertEquals( 2, count );
        assertFalse( multiSet.isEmpty() );
        assertEquals( 2, multiSet.size() );
        assertEquals( 1, multiSet.uniqueSize() );
        assertTrue( multiSet.contains( value ) );
        assertEquals( 2, multiSet.count( value ) );
    }

    @Test
    public void shouldRemoveAnElementWhenMultiElementArePresentInTheMultiSet()
    {
        // given
        MultiSet<Object> multiSet = new MultiSet<>();
        Object value = new Object();
        multiSet.add( value );
        multiSet.add( value );

        // when
        long count = multiSet.remove( value );

        // then
        assertEquals( 1, count );
        assertFalse( multiSet.isEmpty() );
        assertEquals( 1, multiSet.size() );
        assertEquals( 1, multiSet.uniqueSize() );
        assertTrue( multiSet.contains( value ) );
        assertEquals( 1, multiSet.count( value ) );
    }

    @Test
    public void shouldClearTheMultiSet()
    {
        // given
        MultiSet<Object> multiSet = new MultiSet<>();
        Object value = new Object();
        multiSet.add( value );
        multiSet.add( value );
        multiSet.add( new Object() );

        // when
        multiSet.clear();

        // then
        assertTrue( multiSet.isEmpty() );
        assertEquals( 0, multiSet.size() );
        assertEquals( 0, multiSet.uniqueSize() );
        assertFalse( multiSet.contains( value ) );
        assertEquals( 0, multiSet.count( value ) );
    }
}
