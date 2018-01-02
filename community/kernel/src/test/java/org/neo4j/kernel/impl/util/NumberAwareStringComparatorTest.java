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
package org.neo4j.kernel.impl.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NumberAwareStringComparatorTest
{
    @Test
    public void shouldHandleSingleNumber() throws Exception
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
    public void shouldHandleMixedAlthoughSimilarNumbersAndStrings() throws Exception
    {
        assertLesser( "same-1-thing-45", "same-12-thing-45" );
        assertGreater( "same-2-thing-46", "same-2-thing-45" );
    }

    @Test
    public void shouldHandleMixedAndDifferentNumbersAndStrings() throws Exception
    {
        assertLesser( "same123thing456", "same123thing456andmore" );
        assertGreater( "same12", "same1thing456andmore" );
    }

    private void assertLesser( String first, String other )
    {
        assertTrue( compare( first, other ) < 0 );
    }

    private void assertSame( String first, String other )
    {
        assertEquals( 0, compare( first, other ) );
    }

    private void assertGreater( String first, String other )
    {
        assertTrue( compare( first, other ) > 0 );
    }

    private int compare( String first, String other )
    {
        return new NumberAwareStringComparator().compare( first, other );
    }
}
