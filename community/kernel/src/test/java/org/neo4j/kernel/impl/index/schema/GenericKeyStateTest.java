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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

@ExtendWith( RandomExtension.class )
class GenericKeyStateTest
{
    @Inject
    private static RandomRule random;

    private static final PageCursor cursor = ByteArrayPageCursor.wrap( PageCache.PAGE_SIZE );

    @BeforeEach
    public void setupRandomConfig()
    {
        random = random.withConfiguration( new RandomValues.Configuration()
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
        } );
        random.reset();
    }

    @ParameterizedTest
    @MethodSource( "validValueGenerators" )
    void readWhatIsWritten( ValueGenerator valueGenerator )
    {
        // Given
        GenericKeyState writeState = new GenericKeyState();
        Value value = valueGenerator.next();
        int offset = cursor.getOffset();

        // When
        writeState.writeValue( value, NEUTRAL );
        writeState.put( cursor );

        // Then
        GenericKeyState readState = new GenericKeyState();
        int size = writeState.size();
        cursor.setOffset( offset );
        assertTrue( readState.read( cursor, size ), "failed to read" );
        assertEquals( 0, readState.compareValueTo( writeState ), "key states are not equal" );
        Value readValue = readState.asValue();
        assertEquals( value, readValue, "deserialized valued are not equal" );
    }

    @ParameterizedTest
    @MethodSource( "validValueGenerators" )
    void copyShouldCopy( ValueGenerator valueGenerator )
    {
        // Given
        GenericKeyState from = new GenericKeyState();
        Value value = valueGenerator.next();
        from.writeValue( value, NEUTRAL );
        GenericKeyState to = genericKeyStateWithSomePreviousState( valueGenerator );

        // When
        to.copyFrom( from );

        // Then
        assertEquals( 0, from.compareValueTo( to ), "states not equals after copy" );
    }

    @ParameterizedTest
    @MethodSource( "validValueGenerators" )
    void assertCorrectTypeMustNotFailForValidTypes( ValueGenerator valueGenerator )
    {
        // Given
        Value value = valueGenerator.next();

        // When
        GenericKeyState.assertCorrectType( value );

        // Then
        // should not fail
    }

    @ParameterizedTest
    @MethodSource( "invalidValueGenerators" )
    void assertCorrectTypeMustFailForInvalidTypes( ValueGenerator valueGenerator )
    {
        // Given
        Value invalidValue = valueGenerator.next();

        // When
        assertThrows( IllegalArgumentException.class, () -> GenericKeyState.assertCorrectType( invalidValue ),
                "did not throw on invalid value " + invalidValue );
    }

    @ParameterizedTest
    @MethodSource( "validValueGenerators" )
    void compareToMustAlignWithValuesCompareTo( ValueGenerator valueGenerator )
    {
        // Given
        random.reset();
        List<Value> values = new ArrayList<>();
        List<GenericKeyState> states = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            Value value = valueGenerator.next();
            values.add( value );
            GenericKeyState state = new GenericKeyState();
            state.writeValue( value, NEUTRAL );
            states.add( state );
        }

        // When
        values.sort( Values.COMPARATOR );
        states.sort( GenericKeyState::compareValueTo );

        // Then
        for ( int i = 0; i < values.size(); i++ )
        {
            assertEquals( values.get( i ), states.get( i ).asValue(), "sort order was different" );
        }
    }

    @ParameterizedTest
    @MethodSource( "validValueGenerators" )
    void mustProduceValidMinimalSplitters( ValueGenerator valueGenerator )
    {
        // Given
        Value value1 = valueGenerator.next();
        Value value2;
        do
        {
            value2 = valueGenerator.next();
        }
        while ( Values.COMPARATOR.compare( value1, value2 ) == 0 );

        // When
        Value left = pickSmaller( value1, value2 );
        Value right = left == value1 ? value2 : value1;

        // Then
        assertValidMinimalSplitter( left, right );
    }

    // todo size
    // todo initValueAsLowest / Highest

    private Value pickSmaller( Value value1, Value value2 )
    {
        return Values.COMPARATOR.compare( value1, value2 ) < 0 ? value1 : value2;
    }

    private void assertValidMinimalSplitter( Value left, Value right )
    {
        GenericKeyState leftState = new GenericKeyState();
        leftState.writeValue( left, NEUTRAL );
        GenericKeyState rightState = new GenericKeyState();
        rightState.writeValue( right, NEUTRAL );

        GenericKeyState minimalSplitter = new GenericKeyState();
        GenericKeyState.minimalSplitter( leftState, rightState, minimalSplitter );

        assertTrue( leftState.compareValueTo( minimalSplitter ) < 0,
                "left state not less than minimal splitter, leftState=" + leftState + ", rightState=" + rightState + ", minimalSplitter=" + minimalSplitter );
        assertTrue( rightState.compareValueTo( minimalSplitter ) >= 0,
                "right state not less than minimal splitter, leftState=" + leftState + ", rightState=" + rightState + ", minimalSplitter=" + minimalSplitter );
    }

    private static Value nextValidValue()
    {
        Value value;
        do
        {
            value = random.randomValues().nextValue();
        }
        while ( isInvalid( value ) );
        return value;
    }

    private static boolean isInvalid( Value value )
    {
        // todo update when spatial is supported
        return Values.isGeometryValue( value ) || Values.isGeometryArray( value );
    }

    private static Stream<ValueGenerator> validValueGenerators()
    {
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
                () -> random.randomValues().nextDoubleArray(),
                // todo GEOMETRY_ARRAY
                GenericKeyStateTest::nextValidValue // and a random
        );
    }

    private static Stream<ValueGenerator> invalidValueGenerators()
    {
        return Stream.of(
                () -> random.randomValues().nextGeographicPoint(),
                () -> random.randomValues().nextGeographic3DPoint(),
                () -> random.randomValues().nextCartesianPoint(),
                () -> random.randomValues().nextCartesian3DPoint(),
                () -> random.randomValues().nextGeographicPointArray(),
                () -> random.randomValues().nextGeographic3DPointArray(),
                () -> random.randomValues().nextCartesianPointArray(),
                () -> random.randomValues().nextCartesian3DPointArray()
        );
        // todo Remove when GEOMERTY is supported
    }

    private GenericKeyState genericKeyStateWithSomePreviousState( ValueGenerator valueGenerator )
    {
        GenericKeyState to = new GenericKeyState();
        if ( random.nextBoolean() )
        {
            // Previous value
            NativeIndexKey.Inclusion inclusion = random.among( NativeIndexKey.Inclusion.values() );
            Value value = valueGenerator.next();
            to.writeValue( value, inclusion );
        }
        // No previous state
        return to;
    }

    @FunctionalInterface
    private interface ValueGenerator
    {
        Value next();
    }
}
