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
package org.neo4j.values.virtual;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;
import static org.neo4j.values.virtual.VirtualValues.list;
import static org.neo4j.values.virtual.VirtualValues.reverse;

public class ReversedListTest
{

    @Test
    public void shouldHandleEmptyList()
    {
        // Given
        ListValue inner = EMPTY_LIST;
        // When
        ListValue reverse = reverse( inner );

        // Then
        assertEquals( inner, reverse );
        assertEquals( inner.hashCode(), reverse.hashCode() );
        assertArrayEquals( inner.asArray(), reverse.asArray() );
    }

    @Test
    public void shouldHandleSingleItemList()
    {
        // Given
        ListValue inner = list( longValue( 5L ) );

        // When
        ListValue reverse = reverse( inner );

        // Then
        assertEquals( inner, reverse );
        assertEquals( inner.hashCode(), reverse.hashCode() );
        assertArrayEquals( inner.asArray(), reverse.asArray() );
    }

    @Test
    public void shouldReverseList()
    {
        // Given
        ListValue inner = list( longValue( 5L ), longValue( 6L ), longValue( 7L ) );

        // When
        ListValue reverse = reverse( inner );

        // Then
        ListValue expected = list( longValue( 7L ), longValue( 6L ), longValue( 5L ) );
        assertEquals( expected, reverse );
        assertEquals( expected.hashCode(), reverse.hashCode() );
        assertArrayEquals( expected.asArray(), reverse.asArray() );
    }
}
