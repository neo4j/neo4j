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

import java.util.List;

import org.junit.Test;
import org.neo4j.helpers.collection.IteratorUtil;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class SimpleBitSetTest
{

    @Test
    public void put() throws Exception
    {
        // Given
        SimpleBitSet set = new SimpleBitSet(16);

        // When
        set.put( 2 );
        set.put( 7 );
        set.put( 15 );

        // Then
        assertFalse( set.contains( 1 ) );
        assertFalse( set.contains( 6 ) );
        assertFalse( set.contains( 14 ) );

        assertTrue( set.contains( 2 ) );
        assertTrue( set.contains( 7 ) );
        assertTrue( set.contains( 15 ) );
    }

    @Test
    public void putAndRemove() throws Exception
    {
        // Given
        SimpleBitSet set = new SimpleBitSet(16);

        // When
        set.put( 2 );
        set.put( 7 );
        set.remove( 2 );

        // Then
        assertFalse( set.contains( 1 ) );
        assertFalse( set.contains( 6 ) );
        assertFalse( set.contains( 14 ) );
        assertFalse( set.contains( 2 ) );

        assertTrue( set.contains( 7 ) );
    }

    @Test
    public void putOtherBitSet() throws Exception
    {
        // Given
        SimpleBitSet set = new SimpleBitSet(16);
        SimpleBitSet otherSet = new SimpleBitSet(16);

        otherSet.put( 4 );
        otherSet.put( 14 );

        set.put( 3 );
        set.put( 4 );

        // When
        set.put( otherSet );

        // Then
        assertFalse( set.contains( 0 ) );
        assertFalse( set.contains( 1 ) );
        assertFalse( set.contains( 15 ) );
        assertFalse( set.contains( 7 ) );

        assertTrue( set.contains( 3 ) );
        assertTrue( set.contains( 4 ) );
        assertTrue( set.contains( 14 ) );
    }

    @Test
    public void removeOtherBitSet() throws Exception
    {
        // Given
        SimpleBitSet set = new SimpleBitSet(16);
        SimpleBitSet otherSet = new SimpleBitSet(16);

        otherSet.put( 4 );
        otherSet.put( 12 );
        otherSet.put( 14 );

        set.put( 3 );
        set.put( 4 );
        set.put( 12 );

        // When
        set.remove( otherSet );

        // Then
        assertFalse( set.contains( 0 ) );
        assertFalse( set.contains( 1 ) );
        assertFalse( set.contains( 4 ) );
        assertFalse( set.contains( 14 ) );

        assertTrue( set.contains( 3 ) );
    }

    @Test
    public void resize() throws Exception
    {
        // Given
        SimpleBitSet set = new SimpleBitSet( 8 );

        // When
        set.put( 128 );

        // Then
        assertTrue( set.contains( 128 ) );

        assertFalse( set.contains( 126 ));
        assertFalse( set.contains( 129 ));
    }

    @Test
    public void shouldAllowIterating() throws Exception
    {
        // Given
        SimpleBitSet set = new SimpleBitSet( 64 );
        set.put( 4 );
        set.put( 7 );
        set.put( 63 );
        set.put( 78 );

        // When
        List<Integer> found = IteratorUtil.asList( set.iterator() );

        // Then
        assertThat( found, equalTo( asList( 4, 7, 63, 78 ) ));
    }

}
