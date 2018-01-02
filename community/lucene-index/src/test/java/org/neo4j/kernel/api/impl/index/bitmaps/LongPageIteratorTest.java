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
package org.neo4j.kernel.api.impl.index.bitmaps;

import java.util.Collections;

import org.junit.Test;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongPageIteratorTest
{
    @Test
    public void shouldIterateThroughEachLongInEachPage() throws Exception
    {
        // given
        LongPageIterator iterator = new LongPageIterator( asList( new long[]{1, 2, 3}, new long[]{4, 5} ).iterator() );

        // then
        assertTrue( iterator.hasNext() );
        assertEquals( 1, iterator.next() );
        assertTrue( iterator.hasNext() );
        assertEquals( 2, iterator.next() );
        assertTrue( iterator.hasNext() );
        assertEquals( 3, iterator.next() );
        assertTrue( iterator.hasNext() );
        assertEquals( 4, iterator.next() );
        assertTrue( iterator.hasNext() );
        assertEquals( 5, iterator.next() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldNotGetAnythingFromEmptySourceIterator() throws Exception
    {
        // given
        LongPageIterator iterator = new LongPageIterator( Collections.<long[]>emptyList().iterator() );

        // then
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldNotGetAnythingFromSourceIteratorOfEmptyLongArrays() throws Exception
    {
        // given
        LongPageIterator iterator = new LongPageIterator( asList( new long[]{}, new long[]{} ).iterator() );

        // then
        assertFalse( iterator.hasNext() );
    }
}
