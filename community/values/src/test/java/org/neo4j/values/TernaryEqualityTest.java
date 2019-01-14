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
package org.neo4j.values;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;

import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.values.Equality.UNDEFINED;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.charValue;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.durationValue;
import static org.neo4j.values.storable.Values.floatArray;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortArray;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValueTestUtil.map;
import static org.neo4j.values.virtual.VirtualValues.list;

class TernaryEqualityTest
{
    private TernaryComparator<AnyValue> comparator = AnyValues.TERNARY_COMPARATOR;

    @Test
    void shouldGiveFalseWhenComparingDifferentTypes()
    {
        Value[] values =
                new Value[]{booleanValue( true ), longValue( 1337 ), stringValue( "foo" ),
                        durationValue( Duration.ofDays( 12 ) ),
                        DateTimeValue.now( Clock.systemUTC() ),
                        LocalDateTimeValue.now( Clock.systemUTC() ),
                        DateValue.now( Clock.systemUTC() ),
                        Values.booleanArray( new boolean[]{true, false} )};

        for ( int i = 0; i < values.length; i++ )
        {
            for ( int j = i; j < values.length; j++ )
            {
                Equality expected = i == j ? Equality.TRUE : Equality.FALSE;
                assertTernaryEquality( values[i], values[j], expected );
            }
        }
    }

    @Test
    void shouldHandleNoValue()
    {
        AnyValue[] values =
                new AnyValue[]{booleanValue( true ), longValue( 1337 ), stringValue( "foo" ),
                        durationValue( Duration.ofDays( 12 ) ),
                        DateTimeValue.now( Clock.systemUTC() ),
                        LocalDateTimeValue.now( Clock.systemUTC() ),
                        NO_VALUE,
                        list( intValue( 43 ) ),
                        VirtualValues.map( new String[]{"foo"}, new AnyValue[]{intValue( 43 )} ),
                        DateValue.now( Clock.systemUTC() ),
                        Values.booleanArray( new boolean[]{true, false} )};
        for ( AnyValue value : values )
        {
            assertTernaryEquality( value, NO_VALUE, UNDEFINED );
        }
    }

    @Test
    void shouldGiveTrueWhenComparingSameNumbersOfDifferentType()
    {
        NumberValue[] numbers =
                {byteValue( (byte) 11 ), shortValue( (short) 11 ), intValue( 11 ), longValue( 11 ), floatValue( 11 ),
                        doubleValue( 11 )};

        for ( int i = 0; i < numbers.length; i++ )
        {
            for ( int j = i; j < numbers.length; j++ )
            {
                assertTernaryEquality( numbers[i], numbers[j], Equality.TRUE );
            }
        }
    }

    @Test
    void shouldGiveFalseWhenComparingDifferentNumbersOfDifferentType()
    {
        NumberValue[] numbers =
                {byteValue( (byte) 11 ), shortValue( (short) 12 ), intValue( 13 ), longValue( 14 ), floatValue( 15 ),
                        doubleValue( 16 )};

        for ( int i = 0; i < numbers.length; i++ )
        {
            for ( int j = i + 1; j < numbers.length; j++ )
            {
                assertTernaryEquality( numbers[i], numbers[j], Equality.FALSE );
            }
        }
    }

    @Test
    void shouldCompareStringsWithChar()
    {
        assertTernaryEquality( stringValue( "f" ), charValue( 'f' ), Equality.TRUE );
        assertTernaryEquality( stringValue( "foo" ), charValue( 'f' ), Equality.FALSE );
    }

    @Test
    void shouldCompareListsAndArrays()
    {
        ListValue list = list( intValue( 1 ), intValue( 2 ), intValue( 3 ) );
        //Equals
        assertTernaryEquality( list, byteArray( new byte[]{1, 2, 3} ), Equality.TRUE );
        assertTernaryEquality( list, shortArray( new short[]{1, 2, 3} ), Equality.TRUE );
        assertTernaryEquality( list, intArray( new int[]{1, 2, 3} ), Equality.TRUE );
        assertTernaryEquality( list, longArray( new long[]{1, 2, 3} ), Equality.TRUE );
        assertTernaryEquality( list, floatArray( new float[]{1, 2, 3} ), Equality.TRUE );
        assertTernaryEquality( list, doubleArray( new double[]{1, 2, 3} ), Equality.TRUE );
        assertTernaryEquality( list( intArray( new int[]{1, 2, 3} ) ), list( list ), Equality.TRUE );

        //Not equals
        assertTernaryEquality( list, byteArray( new byte[]{11, 2, 3} ), Equality.FALSE );
        assertTernaryEquality( list, shortArray( new short[]{11, 2, 3} ), Equality.FALSE );
        assertTernaryEquality( list, intArray( new int[]{11, 2, 3} ), Equality.FALSE );
        assertTernaryEquality( list, longArray( new long[]{11, 2, 3} ), Equality.FALSE );
        assertTernaryEquality( list, floatArray( new float[]{11, 2, 3} ), Equality.FALSE );
        assertTernaryEquality( list, doubleArray( new double[]{11, 2, 3} ), Equality.FALSE );
        assertTernaryEquality( list( intArray( new int[]{11, 2, 3} ) ), list( list ), Equality.FALSE );
    }

    @Test
    void shouldHandleListsWithNoValue()
    {
        ListValue listWithNoValue = list( intValue( 1 ), NO_VALUE, intValue( 2 ) );
        assertTernaryEquality( listWithNoValue, list( intValue( 1 ), intValue( 2 ), intValue( 3 ), intValue( 4 ) ),
                Equality.FALSE );
        assertTernaryEquality( listWithNoValue, list( intValue( 1 ), NO_VALUE ), Equality.FALSE );
        assertTernaryEquality( listWithNoValue, list( intValue( 2 ), NO_VALUE, intValue( 2 ) ), Equality.FALSE );
        assertTernaryEquality( listWithNoValue, list( intValue( 1 ), NO_VALUE, intValue( 3 ) ), Equality.FALSE );
        //check so that we are not using a referential equality check
        assertTernaryEquality( listWithNoValue, listWithNoValue, Equality.UNDEFINED );
        assertTernaryEquality( listWithNoValue, list( intValue( 1 ), NO_VALUE, intValue( 2 ) ), Equality.UNDEFINED );
        assertTernaryEquality( listWithNoValue, list( intValue( 2 ), NO_VALUE, intValue( 2 ) ), Equality.FALSE );
        assertTernaryEquality( list( NO_VALUE, NO_VALUE, NO_VALUE ), list( NO_VALUE, NO_VALUE ), Equality.FALSE );
        assertTernaryEquality( list( intValue( 1 ), intValue( 2 ) ), list( intValue( 1 ), stringValue( "two" ) ), Equality.FALSE );

    }

    @Test
    void shouldHandleMaps()
    {
        assertTernaryEquality( map( "key", list( intValue( 42 ) ) ), map( "key", intArray( new int[]{42} ) ),
                Equality.TRUE );
        assertTernaryEquality( map( "key", list( intValue( 42 ) ) ), map( "key", NO_VALUE ), Equality.UNDEFINED );
        assertTernaryEquality( map( "key1", list( intValue( 42 ) ) ), map( "key2", NO_VALUE ), Equality.FALSE );
        assertTernaryEquality( map( "k1", 42, "k2", NO_VALUE ), map( "k1", 43, "k2", 45 ), Equality.FALSE );
        assertTernaryEquality( map( "k1", 42, "k2", NO_VALUE ), map( "k1", 42, "k2", 45 ), Equality.UNDEFINED );
        assertTernaryEquality( map( "k1", 42, "k2", NO_VALUE ), map( "k1", 43, "k2", 45 ), Equality.FALSE );
    }

    @Test
    void shouldHandleDurations()
    {
       assertTernaryEquality(  durationValue( Duration.ofDays( 12 ) ),  durationValue( Duration.ofDays( 12 ) ), Equality.TRUE );
       assertTernaryEquality(  durationValue( Duration.ofDays( 12 ) ),  durationValue( Duration.ofDays( 13 ) ), Equality.FALSE );
    }

    private void assertTernaryEquality( AnyValue a, AnyValue b, Equality expected )
    {
        assertThat( format( "%s = %s should be %s", a, b, expected ), a.ternaryEquals( b ), equalTo( expected ) );
        assertThat( format( "%s = %s shoudl be %s", b, a, expected ), b.ternaryEquals( a ), equalTo( expected ) );
        switch ( expected )
        {
        case TRUE:
            assertThat( format( "%s = %s", a, b ), comparator.ternaryCompare( a, b ), equalTo( Comparison.EQUAL ) );
            assertThat( format( "%s = %s", b, a ), comparator.ternaryCompare( b, a ), equalTo( Comparison.EQUAL ) );
            break;
        case FALSE:
            assertThat( format( "%s = %s", a, b ), comparator.ternaryCompare( a, b ),
                    not( equalTo( Comparison.EQUAL ) ) );
            assertThat( format( "%s = %s", b, a ), comparator.ternaryCompare( b, a ),
                    not( equalTo( Comparison.EQUAL ) ) );
            break;
        case UNDEFINED:
            assertThat( format( "%s = %s", a, b ), comparator.ternaryCompare( a, b ), equalTo( Comparison.UNDEFINED ) );
            assertThat( format( "%s = %s", b, a ), comparator.ternaryCompare( b, a ), equalTo( Comparison.UNDEFINED ) );
            break;
        default:
            fail( "This was highly unexpected" );
        }
    }
}
