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
package org.neo4j.values.virtual;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.helpers.collection.Iterators.iteratorsEqual;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.virtual.VirtualValues.list;

class AppendedPrependListTest
{
    @Test
    void shouldAppendToList()
    {
        // Given
        ListValue inner = list( longValue( 5L ), longValue( 6L ), longValue( 7L ),
                longValue( 8L ), longValue( 9L ), longValue( 10L ), longValue( 11L ) );

        // When
        ListValue appended = inner.append( longValue( 12L ), longValue( 13L ), longValue( 14L ) );

        // Then
        ListValue expected = list( longValue( 5L ), longValue( 6L ), longValue( 7L ),
                longValue( 8L ), longValue( 9L ), longValue( 10L ), longValue( 11L ),
                longValue( 12L ), longValue( 13L ), longValue( 14L ));
        assertListValuesEquals( appended, expected );
    }

    @Test
    void shouldHandleEmptyAppend()
    {
        // Given
        ListValue inner = list( longValue( 5L ), longValue( 6L ), longValue( 7L ),
                longValue( 8L ), longValue( 9L ), longValue( 10L ), longValue( 11L ) );

        // When
        ListValue appended = inner.append( );

        // Then
        assertListValuesEquals( appended, inner );
    }

    @Test
    void shouldAppendToListWithDroppedNull()
    {
        // Given
        ListValue inner = list( longValue( 5L ), longValue( 6L ), longValue( 7L ),
                longValue( 8L ), NO_VALUE, longValue( 9L ), longValue( 10L ), longValue( 11L ) );

        // When
        ListValue appended = inner.dropNoValues().append( longValue( 12L ), longValue( 13L ), longValue( 14L ) );

        // Then
        ListValue expected = list( longValue( 5L ), longValue( 6L ), longValue( 7L ),
                longValue( 8L ), longValue( 9L ), longValue( 10L ), longValue( 11L ),
                longValue( 12L ), longValue( 13L ), longValue( 14L ));
        assertListValuesEquals( appended, expected );
    }

    @Test
    void shouldPrependToList()
    {
        // Given
        ListValue inner = list( longValue( 5L ), longValue( 6L ), longValue( 7L ),
                longValue( 8L ), longValue( 9L ), longValue( 10L ), longValue( 11L ) );

        // When
        ListValue prepend = inner.prepend( longValue( 2L ), longValue( 3L ), longValue( 4L ) );

        // Then
        ListValue expected = list( longValue( 2L ), longValue( 3L ), longValue( 4L ), longValue( 5L ), longValue( 6L ),
                longValue( 7L ),
                longValue( 8L ), longValue( 9L ), longValue( 10L ), longValue( 11L ) );
        assertListValuesEquals( prepend, expected );
    }

    @Test
    void shouldHandleEmptyPrepend()
    {
        // Given
        ListValue inner = list( longValue( 5L ), longValue( 6L ), longValue( 7L ),
                longValue( 8L ), longValue( 9L ), longValue( 10L ), longValue( 11L ) );

        // When
        ListValue prepend = inner.prepend( );

        // Then
        assertListValuesEquals( prepend, inner );
    }

    @Test
    void shouldPrependToListWithDroppedNull()
    {
        // Given
        ListValue inner = list( longValue( 5L ), longValue( 6L ), longValue( 7L ),
                longValue( 8L ), NO_VALUE, longValue( 9L ), longValue( 10L ), longValue( 11L ) );

        // When
        ListValue appended = inner.dropNoValues().prepend( longValue( 2L ), longValue( 3L ), longValue( 4L ) );

        // Then
        ListValue expected = list( longValue( 2L ), longValue( 3L ), longValue( 4L ), longValue( 5L ), longValue( 6L ),
                longValue( 7L ),
                longValue( 8L ), longValue( 9L ), longValue( 10L ), longValue( 11L ) );

        assertListValuesEquals( appended, expected );
    }

    private void assertListValuesEquals( ListValue appended, ListValue expected )
    {
        assertEquals( expected, appended );
        assertEquals( expected.hashCode(), appended.hashCode() );
        assertArrayEquals( expected.asArray(), appended.asArray() );
        assertTrue( iteratorsEqual(expected.iterator(), appended.iterator()) );
    }
}
