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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.index.schema.GenericKey.NO_ENTITY_ID;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

@ExtendWith( RandomExtension.class )
class StringIndexKeyTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldReuseByteArrayForFairlySimilarSizedKeys()
    {
        // given
        StringIndexKey key = new StringIndexKey();
        key.setBytesLength( 20 );
        byte[] first = key.bytes;

        // when
        key.setBytesLength( 25 );
        byte[] second = key.bytes;

        // then
        assertSame( first, second );
        assertThat( first.length, greaterThanOrEqualTo( 25 ) );
    }

    @Test
    void shouldCreateNewByteArrayForVastlyDifferentKeySizes()
    {
        // given
        StringIndexKey key = new StringIndexKey();
        key.setBytesLength( 20 );
        byte[] first = key.bytes;

        // when
        key.setBytesLength( 100 );
        byte[] second = key.bytes;

        // then
        assertNotSame( first, second );
        assertThat( first.length, greaterThanOrEqualTo( 20 ) );
        assertThat( second.length, greaterThanOrEqualTo( 100 ) );
    }

    @Test
    void shouldDereferenceByteArrayWhenMaterializingValue()
    {
        // given
        StringIndexKey key = new StringIndexKey();
        key.setBytesLength( 20 );
        byte[] first = key.bytes;

        // when
        key.asValue();
        key.setBytesLength( 25 );
        byte[] second = key.bytes;

        // then
        assertNotSame( first, second );
    }

    @Test
    void minimalSplitterForSameValueShouldDivideLeftAndRight()
    {
        // Given
        StringLayout layout = new StringLayout();
        StringIndexKey left = layout.newKey();
        StringIndexKey right = layout.newKey();
        StringIndexKey minimalSplitter = layout.newKey();

        // keys with same value but different entityId
        TextValue value = random.randomValues().nextTextValue();
        left.initialize( 1 );
        right.initialize( 2 );
        left.initFromValue( 0, value, NEUTRAL );
        right.initFromValue( 0, value, NEUTRAL );

        // When creating minimal splitter
        layout.minimalSplitter( left, right, minimalSplitter );

        // Then that minimal splitter need to correctly divide left and right
        assertTrue( layout.compare( left, minimalSplitter ) < 0,
                "Expected minimal splitter to be strictly greater than left but wasn't for value " + value );
        assertTrue( layout.compare( minimalSplitter, right ) <= 0,
                "Expected right to be greater than or equal to minimal splitter but wasn't for value " + value );
    }

    @Test
    void minimalSplitterShouldRemoveEntityIdIfPossible()
    {
        // Given
        StringLayout layout = new StringLayout();
        StringIndexKey left = layout.newKey();
        StringIndexKey right = layout.newKey();
        StringIndexKey minimalSplitter = layout.newKey();

        String string = random.nextString();
        TextValue leftValue = Values.stringValue( string );
        TextValue rightValue = Values.stringValue( string + random.randomValues().nextCharRaw() );

        // keys with unique values
        left.initialize( 1 );
        left.initFromValue( 0, leftValue, NEUTRAL );
        right.initialize( 2 );
        right.initFromValue( 0, rightValue, NEUTRAL );

        // When creating minimal splitter
        layout.minimalSplitter( left, right, minimalSplitter );

        // Then that minimal splitter should have entity id shaved off
        assertEquals( NO_ENTITY_ID, minimalSplitter.getEntityId(),
                "Expected minimal splitter to have entityId removed when constructed from keys with unique values: " +
                        "left=" + leftValue + ", right=" + rightValue );
    }
}
