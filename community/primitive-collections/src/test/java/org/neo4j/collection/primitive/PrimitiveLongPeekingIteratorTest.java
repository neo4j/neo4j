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
package org.neo4j.collection.primitive;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PrimitiveLongPeekingIteratorTest
{
    @Test
    public void shouldDetectMultipleValues()
    {
        // GIVEN
        long[] values = new long[] { 1, 2, 3 };
        PrimitiveLongIterator actual = PrimitiveLongCollections.iterator( values );
        PrimitiveLongPeekingIterator peekingIterator = new PrimitiveLongPeekingIterator( actual );

        // THEN
        assertTrue( peekingIterator.hasMultipleValues() );
        for ( long value : values )
        {
            assertEquals( value, peekingIterator.next() );
        }
        assertFalse( peekingIterator.hasNext() );
        assertTrue( peekingIterator.hasMultipleValues() );
    }

    @Test
    public void shouldDetectSingleValue()
    {
        // GIVEN
        long[] values = new long[] { 1 };
        PrimitiveLongIterator actual = PrimitiveLongCollections.iterator( values );
        PrimitiveLongPeekingIterator peekingIterator = new PrimitiveLongPeekingIterator( actual );
        // THEN
        assertFalse( peekingIterator.hasMultipleValues() );
        for ( long value : values )
        {
            assertEquals( value, peekingIterator.next() );
        }
        assertFalse( peekingIterator.hasNext() );
        assertFalse( peekingIterator.hasMultipleValues() );
    }
}
