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
package org.neo4j.values.storable;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.values.AnyValue;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.values.storable.Values.ZERO_INT;
import static org.neo4j.values.storable.Values.longValue;

@RunWith( Parameterized.class )
public class RandomValuesTest
{
    private static final int ITERATIONS = 1000;

    @Parameterized.Parameter()
    public RandomValues randomValues;

    @Parameterized.Parameter( 1 )
    public String name;

    @Parameterized.Parameters( name = "{1}" )
    public static Iterable<Object[]> generators()
    {
        return Arrays.asList(
                new Object[]{RandomValues.create( ThreadLocalRandom.current() ), Random.class.getName()},
                new Object[]{RandomValues.create( new SplittableRandom() ), SplittableRandom.class.getName()}
        );
    }

    private static final byte BOUND = 100;
    private static final LongValue UPPER = longValue( BOUND );
    private static final Set<Class<? extends NumberValue>> NUMBER_TYPES = new HashSet<>(
            Arrays.asList(
                    LongValue.class,
                    IntValue.class,
                    ShortValue.class,
                    ByteValue.class,
                    FloatValue.class,
                    DoubleValue.class ) );

    private static final Set<Class<? extends AnyValue>> TYPES = new HashSet<>(
            Arrays.asList(
                    LongValue.class,
                    IntValue.class,
                    ShortValue.class,
                    ByteValue.class,
                    FloatValue.class,
                    DoubleValue.class,
                    TextValue.class,
                    BooleanValue.class,
                    PointValue.class,
                    DateTimeValue.class,
                    LocalDateTimeValue.class,
                    DateValue.class,
                    TimeValue.class,
                    LocalTimeValue.class,
                    DurationValue.class
            ) );

    @Test
    public void nextLongValueUnbounded()
    {
        checkDistribution( randomValues::nextLongValue );
    }

    @Test
    public void nextLongValueBounded()
    {
        checkDistribution( () -> randomValues.nextLongValue( BOUND ) );
        checkBounded( () -> randomValues.nextLongValue( BOUND ) );
    }

    @Test
    public void nextLongValueBoundedAndShifted()
    {
        Set<Value> values = new HashSet<>();
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            LongValue value = randomValues.nextLongValue( 1337, 1337 + BOUND );
            assertThat( value, notNullValue() );
            assertThat( value.compareTo( longValue( 1337 ) ), greaterThanOrEqualTo( 0 ) );
            assertThat( value.toString(), value.compareTo( longValue( 1337 + BOUND ) ), lessThanOrEqualTo( 0 ) );
            values.add( value );
        }

        assertThat( values.size(), greaterThan( 1 ) );
    }

    @Test
    public void nextBooleanValue()
    {
        checkDistribution( randomValues::nextBooleanValue );
    }

    @Test
    public void nextIntValueUnbounded()
    {
        checkDistribution( randomValues::nextIntValue );
    }

    @Test
    public void nextIntValueBounded()
    {
        checkDistribution( () -> randomValues.nextIntValue( BOUND ) );
        checkBounded( () -> randomValues.nextIntValue( BOUND ) );
    }

    @Test
    public void nextShortValueUnbounded()
    {
        checkDistribution( randomValues::nextShortValue );
    }

    @Test
    public void nextShortValueBounded()
    {
        checkDistribution( () -> randomValues.nextShortValue( BOUND ) );
        checkBounded( () -> randomValues.nextShortValue( BOUND ) );
    }

    @Test
    public void nextByteValueUnbounded()
    {
        checkDistribution( randomValues::nextByteValue );
    }

    @Test
    public void nextByteValueBounded()
    {
        checkDistribution( () -> randomValues.nextByteValue( BOUND ) );
        checkBounded( () -> randomValues.nextByteValue( BOUND ) );
    }

    @Test
    public void nextFloatValue()
    {
        checkDistribution( randomValues::nextFloatValue );
    }

    @Test
    public void nextDoubleValue()
    {
        checkDistribution( randomValues::nextDoubleValue );
    }

    @Test
    public void nextNumberValue()
    {
        HashSet<Class<? extends NumberValue>> seen = new HashSet<>( NUMBER_TYPES );

        for ( int i = 0; i < ITERATIONS; i++ )
        {
            NumberValue numberValue = randomValues.nextNumberValue();
            assertThat( NUMBER_TYPES, hasItem( numberValue.getClass() ) );
            seen.remove( numberValue.getClass() );
        }
        assertThat( seen, empty() );
    }

    @Test
    public void nextAlphaNumericString()
    {
        Set<Integer> seenDigits = "ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvxyz0123456789".chars().boxed()
                .collect( Collectors.toSet() );
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            TextValue textValue = randomValues.nextAlphaNumericTextValue( 10, 20 );
            String asString = textValue.stringValue();
            for ( int j = 0; j < asString.length(); j++ )
            {
                int ch = asString.charAt( j );
                assertTrue( "Not a character nor letter: " + ch,
                        Character.isAlphabetic( ch ) || Character.isDigit( ch ) );
                seenDigits.remove( ch );
            }
        }
        assertThat( seenDigits, empty() );
    }

    @Test
    public void nextAsciiString()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            TextValue textValue = randomValues.nextAsciiTextValue( 10, 20 );
            String asString = textValue.stringValue();
            int length = asString.length();
            assertThat( length, greaterThanOrEqualTo( 10 ) );
            assertThat( length, lessThanOrEqualTo( 20 ) );
        }
    }

    @Test
    public void nextString()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            TextValue textValue = randomValues.nextTextValue( 10, 20 );
            String asString = textValue.stringValue();
            int length = asString.codePointCount( 0, asString.length() );
            assertThat( length, greaterThanOrEqualTo( 10 ) );
            assertThat( length, lessThanOrEqualTo( 20 ) );
        }
    }

    @Test
    public void nextArray()
    {
        HashSet<Class<? extends AnyValue>> seen = new HashSet<>( TYPES );
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            ArrayValue arrayValue = randomValues.nextArray();
            assertThat( arrayValue.length(), greaterThanOrEqualTo( 1 ) );
            AnyValue value = arrayValue.value( 0 );
            assertKnownType( value.getClass(), TYPES );
            markSeen( value.getClass(), seen );
        }

        assertThat( seen, empty() );
    }

    @Test
    public void nextValue()
    {
        HashSet<Class<? extends AnyValue>> all = new HashSet<>( TYPES );
        all.add( ArrayValue.class );
        HashSet<Class<? extends AnyValue>> seen = new HashSet<>( all );

        for ( int i = 0; i < ITERATIONS; i++ )
        {
            Value value = randomValues.nextValue();
            assertKnownType( value.getClass(), all );
            markSeen( value.getClass(), seen );
        }

        assertThat( seen, empty() );
    }

    @Test
    public void nextValueOfTypes()
    {
        ValueType[] allTypes = ValueType.values();
        ValueType[] including = randomValues.selection( allTypes, 1, allTypes.length, false );
        HashSet<Class<? extends AnyValue>> seen = new HashSet<>();
        for ( ValueType type : including )
        {
            seen.add( type.valueClass );
        }
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            Value value = randomValues.nextValueOfTypes( including );
            assertValueAmongTypes( including, value );
            markSeen( value.getClass(), seen );
        }
        assertThat( seen, empty() );
    }

    @Test
    public void excluding()
    {
        ValueType[] allTypes = ValueType.values();
        ValueType[] excluding = randomValues.selection( allTypes, 1, allTypes.length, false );
        ValueType[] including = randomValues.excluding( excluding );
        for ( ValueType excludedType : excluding )
        {
            if ( ArrayUtils.contains( including, excludedType ) )
            {
                fail( "Including array " + Arrays.toString( including ) + " contains excluded type " + excludedType );
            }
        }
    }

    @Test
    public void nextBasicMultilingualPlaneTextValue()
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            TextValue value = randomValues.nextBasicMultilingualPlaneTextValue();
            //make sure the value fits in 16bits, meaning that the size of the char[]
            //matches the number of code points.
            assertThat( value.length(), equalTo( value.stringValue().length() ) );
        }
    }

    private void assertValueAmongTypes( ValueType[] types, Value value )
    {
        for ( ValueType type : types )
        {
            if ( type.valueClass.isAssignableFrom( value.getClass() ) )
            {
                return;
            }
        }
        fail( "Value " + value + " was not among types " + Arrays.toString( types ) );
    }

    private void assertKnownType( Class<? extends AnyValue> typeToCheck, Set<Class<? extends AnyValue>> types )
    {
        for ( Class<? extends AnyValue> type : types )
        {
            if ( type.isAssignableFrom( typeToCheck ) )
            {
                return;
            }
        }
        fail( typeToCheck + " is not an expected type " );
    }

    private void markSeen( Class<? extends AnyValue> typeToCheck, Set<Class<? extends AnyValue>> seen )
    {
        seen.removeIf( t -> t.isAssignableFrom( typeToCheck ) );
    }

    private void checkDistribution( Supplier<Value> supplier )
    {
        Set<Value> values = new HashSet<>();
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            Value value = supplier.get();
            assertThat( value, notNullValue() );
            values.add( value );
        }

        assertThat( values.size(), greaterThan( 1 ) );
    }

    private void checkBounded( Supplier<NumberValue> supplier )
    {
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            NumberValue value = supplier.get();
            assertThat( value, notNullValue() );
            assertThat( value.compareTo( ZERO_INT ), greaterThanOrEqualTo( 0 ) );
            assertThat( value.compareTo( UPPER ), lessThan( 0 ) );
        }
    }
}
