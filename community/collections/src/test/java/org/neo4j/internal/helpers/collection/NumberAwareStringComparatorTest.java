/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.helpers.collection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NumberAwareStringComparatorTest
{
    @Test
    void shouldHandleSingleNumber()
    {
        // LESSER
        assertLesser( "123", "456" );
        assertLesser( "123", "1234" );
        assertLesser( "1", "12" );

        // SAME
        assertSame( "123", "123" );
        assertSame( "001", "1" );

        // GREATER
        assertGreater( "555", "66" );
    }

    @Test
    void shouldHandleMixedAlthoughSimilarNumbersAndStrings()
    {
        assertLesser( "same-1-thing-45", "same-12-thing-45" );
        assertGreater( "same-2-thing-46", "same-2-thing-45" );
    }

    @Test
    void shouldHandleMixedAndDifferentNumbersAndStrings()
    {
        assertLesser( "same123thing456", "same123thing456andmore" );
        assertGreater( "same12", "same1thing456andmore" );
    }

    @Test
    void shouldHandleBigNumbers()
    {
        assertGreater( "same-9999999999999999999999999999999999999", "same-9999999999999999999999999999999999998" );
        assertLesser( "same-9", "same-8999999999999999999999999999999999998" );
    }

    private static void assertLesser( String first, String other )
    {
        assertTrue( compare( first, other ) < 0 );
    }

    private static void assertSame( String first, String other )
    {
        assertEquals( 0, compare( first, other ) );
    }

    private static void assertGreater( String first, String other )
    {
        assertTrue( compare( first, other ) > 0 );
    }

    private static int compare( String first, String other )
    {
        return NumberAwareStringComparator.INSTANCE.compare( first, other );
    }
}
