/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values.virtual;

import org.junit.Test;

import org.neo4j.values.storable.LongValue;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.virtual.VirtualValues.list;
import static org.neo4j.values.virtual.VirtualValues.transform;

public class TransformedListTest
{
    @Test
    public void shouldTransformList()
    {
        // Given
        ListValue inner = list( longValue( 5L ), longValue( 6L ), longValue( 7L ) );

        // When
        ListValue transform = transform( inner, a ->
        {
            LongValue l = (LongValue) a;
            return longValue( l.value() + 42L );
        } );

        // Then
        ListValue expected = list( longValue( 47L ), longValue( 48L ), longValue( 49L ) );
        assertEquals( expected, transform );
        assertEquals( expected.hashCode(), transform.hashCode() );
        assertArrayEquals( expected.asArray(), transform.asArray() );
    }
}
