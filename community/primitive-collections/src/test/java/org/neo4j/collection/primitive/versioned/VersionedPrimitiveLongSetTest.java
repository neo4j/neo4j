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

package org.neo4j.collection.primitive.versioned;

import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.asSet;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.iterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.setOf;

public class VersionedPrimitiveLongSetTest
{
    private final VersionedPrimitiveLongSet set = new VersionedPrimitiveLongSet();
    private final PrimitiveLongSet current = set.currentView();
    private final PrimitiveLongSet stable = set.stableView();

    @Test
    public void testAdd()
    {
        current.add( 1 );
        current.add( 2 );
        current.add( 3 );

        assertEquals( setOf( 1, 2, 3 ), asSet( current ) );
    }

    @Test
    public void testRemove()
    {
        current.add( 1 );
        current.add( 2 );
        current.add( 3 );

        assertEquals( 3, current.size() );

        current.remove( 1 );

        assertEquals( 2, current.size() );
    }

    @Test
    public void testSize()
    {
        assertEquals( 0, current.size() );
        assertEquals( 0, stable.size() );

        current.add( 1 );
        current.add( 2 );
        current.add( 3 );

        assertEquals( 3, current.size() );
        assertEquals( 0, stable.size() );
    }

    @Test
    public void testEmpty()
    {
        assertTrue( current.isEmpty() );
        assertTrue( stable.isEmpty() );

        current.add( 1 );
        current.add( 2 );
        current.add( 3 );

        assertFalse( current.isEmpty() );
        assertTrue( stable.isEmpty() );
    }

    public void testStabilization()
    {
        current.add( 1 );
        current.add( 2 );
        current.add( 3 );

        assertEquals( 3, current.size() );
        assertEquals( 0, stable.size() );

        set.markStable();

        assertEquals( 3, current.size() );
        assertEquals( 3, stable.size() );

        current.remove( 1 );

        assertEquals( 2, current.size() );
        assertEquals( 3, stable.size() );

        set.markStable();

        assertEquals( 2, current.size() );
        assertEquals( 2, stable.size() );
    }

    @Test
    public void testAddAll()
    {
        current.addAll( iterator( 1, 2, 3 ) );

        assertEquals( setOf( 1, 2, 3 ), asSet( current ) );
    }
}
