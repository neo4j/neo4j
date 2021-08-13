/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.values.storable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.virtual.ListValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.virtual.VirtualValues.fromArray;

@ExtendWith( RandomExtension.class )
class ListValueFuzzTest
{
    @Inject
    private RandomSupport random;

    private static final int ITERATIONS = 1000;

    @Test
    void shouldBeStorableIfAppendToStorableWithCompatibleTypes()
    {
        for ( ValueType valueType : ValueType.arrayTypes() )
        {
            // Given
            ArrayValue arrayValue = (ArrayValue) random.nextValue( valueType );
            ListValue inner = fromArray( arrayValue );

            // When
            ListValue appended = inner.append( nextCompatible( arrayValue ) );

            // Then
            assertTrue( appended.storable() );
            assertEquals( appended, fromArray( appended.toStorableArray() ) );
        }
    }

    @Test
    void shouldNotBeStorableIfAppendToStorableWithIncompatibleTypes()
    {
        for ( ValueType valueType : ValueType.arrayTypes() )
        {
            // Given
            ArrayValue arrayValue = (ArrayValue) random.nextValue( valueType );
            ListValue inner = fromArray( arrayValue );

            // When
            ListValue appended = inner.append( nextIncompatible( arrayValue ) );

            // Then
            assertFalse( appended.storable() );
        }
    }

    @Test
    void shouldBeStorableIfPrependToStorableWithCompatibleTypes()
    {
        for ( ValueType valueType : ValueType.arrayTypes() )
        {
            // Given
            ArrayValue arrayValue = (ArrayValue) random.nextValue( valueType );
            ListValue inner = fromArray( arrayValue );

            // When
            ListValue prepended = inner.prepend( nextCompatible( arrayValue ) );

            // Then
            assertTrue( prepended.storable() );
            assertEquals( prepended, fromArray( prepended.toStorableArray() ) );
        }
    }

    @Test
    void shouldNotBeStorableIfPrependToStorableWithIncompatibleTypes()
    {
        for ( ValueType valueType : ValueType.arrayTypes() )
        {
            // Given
            ArrayValue arrayValue = (ArrayValue) random.nextValue( valueType );
            ListValue inner = fromArray( arrayValue );

            // When
            ListValue prepended = inner.prepend( nextIncompatible( arrayValue ) );

            // Then
            assertFalse( prepended.storable() );
        }
    }

    private Value nextCompatible( ArrayValue value )
    {
        ValueType[] types = ValueType.values();
        while (true)
        {
            Value nextValue = random.nextValue( types[random.nextInt( types.length )] );
            if ( value.hasCompatibleType( nextValue ))
            {
                return nextValue;
            }
        }
    }

    private Value nextIncompatible( ArrayValue value )
    {
        ValueType[] types = ValueType.values();
        while (true)
        {
            Value nextValue = random.nextValue( types[random.nextInt( types.length )] );
            if ( !value.hasCompatibleType( nextValue ))
            {
                return nextValue;
            }
        }
    }
}
