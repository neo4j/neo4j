/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.Rule;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GenericKeyStateTest
{
    @Rule
    private static RandomRule random = new RandomRule().withConfiguration( new RandomValues.Configuration()
    {
        @Override
        public int stringMinLength()
        {
            return 0;
        }

        @Override
        public int stringMaxLength()
        {
            return 50;
        }

        @Override
        public int arrayMinLength()
        {
            return 0;
        }

        @Override
        public int arrayMaxLength()
        {
            return 10;
        }
    });

    private static final PageCursor cursor = ByteArrayPageCursor.wrap( PageCache.PAGE_SIZE );

    @ParameterizedTest
    @MethodSource( "valueGenerators" )
    void readWhatIsWritten( ValueGenerator valueGenerator )
    {
        // Given
        GenericKeyState writeState = new GenericKeyState();
        Value value = valueGenerator.next();
        System.out.println( value );
        int offset = cursor.getOffset();

        // When
        writeState.writeValue( value, NativeIndexKey.Inclusion.NEUTRAL );
        writeState.put( cursor );

        // Then
        GenericKeyState readState = new GenericKeyState();
        int size = writeState.size();
        cursor.setOffset( offset );
        assertTrue( readState.read( cursor, size ), "failed to read" );
        assertEquals( 0, readState.compareValueTo( writeState ), "key states are not equal" );
        Value readValue = readState.asValue();
        System.out.println( "readValue = " + readValue );
        assertEquals( value, readValue, "deserialized valued are not equal" );
    }

    private static Stream<ValueGenerator> valueGenerators()
    {
        random.reset();
        return Stream.of(
                () -> random.randomValues().nextDateTimeValue(),
                () -> random.randomValues().nextLocalDateTimeValue(),
                () -> random.randomValues().nextDateValue(),
                () -> random.randomValues().nextTimeValue(),
                () -> random.randomValues().nextLocalTimeValue(),
                () -> random.randomValues().nextPeriod(),
                () -> random.randomValues().nextDuration(),
                () -> random.randomValues().nextTextValue(),
                () -> random.randomValues().nextAlphaNumericTextValue(),
                () -> random.randomValues().nextBooleanValue(),
                () -> random.randomValues().nextNumberValue(),
                // todo GEOMETRY
                () -> random.randomValues().nextDateTimeArray(),
                () -> random.randomValues().nextLocalDateTimeArray(),
                () -> random.randomValues().nextDateArray(),
                () -> random.randomValues().nextTimeArray(),
                () -> random.randomValues().nextLocalTimeArray(),
                () -> random.randomValues().nextDurationArray(),
                () -> random.randomValues().nextDurationArray(),
                () -> random.randomValues().nextStringArray(),
                () -> random.randomValues().nextAlphaNumericStringArray(),
                () -> random.randomValues().nextBooleanArray(),
                () -> random.randomValues().nextByteArray(),
                () -> random.randomValues().nextShortArray(),
                () -> random.randomValues().nextIntArray(),
                () -> random.randomValues().nextLongArray(),
                () -> random.randomValues().nextFloatArray(),
                () -> random.randomValues().nextDoubleArray()
                // todo GEOMETRY_ARRAY
        );
    }

    @FunctionalInterface
    private interface ValueGenerator
    {
        Value next();
    }

    // todo copyFrom
    // todo assertCorrectType
    // todo compareValueTo
    // todo minimalSplitter
    // todo size
    // todo initValueAsLowest / Highest
}
