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
package org.neo4j.values;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.values.storable.Values.ZERO_INT;
import static org.neo4j.values.storable.Values.longValue;

public class RandomValueTest
{
    private static final int ITERATIONS = 100;
    private final RandomValue randomValue = new RandomValue();
    private final static byte BOUND = 100;
    private final static LongValue UPPER = longValue( BOUND );
    private static final Set<Class<? extends NumberValue>> NUMBER_TYPES = new HashSet<>( Arrays.asList(
            LongValue.class, IntValue.class, ShortValue.class, ByteValue.class, FloatValue.class,
            DoubleValue.class ) );

    @Test
    public void nextLongValueUnbounded()
    {
        checkDistribution( randomValue::nextLongValue );
    }

    @Test
    public void nextLongValueBounded()
    {
        checkDistribution( () -> randomValue.nextLongValue( BOUND ) );
        checkBounded( () -> randomValue.nextLongValue( BOUND ) );
    }

    @Test
    public void nextLongValueBoundedAndShifted()
    {
        Set<Value> values = new HashSet<>();
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            LongValue value = randomValue.nextLongValue( 1337, 1337 + BOUND );
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
        checkDistribution( randomValue::nextBooleanValue );
    }

    @Test
    public void nextIntValueUnbounded()
    {
        checkDistribution( randomValue::nextIntValue );
    }

    @Test
    public void nextIntValueBounded()
    {
        checkDistribution( () -> randomValue.nextIntValue( BOUND ) );
        checkBounded( () -> randomValue.nextIntValue( BOUND ) );
    }

    @Test
    public void nextShortValueUnbounded()
    {
        checkDistribution( randomValue::nextShortValue );
    }

    @Test
    public void nextShortValueBounded()
    {
        checkDistribution( () -> randomValue.nextShortValue( BOUND ) );
        checkBounded( () -> randomValue.nextShortValue( BOUND ) );
    }


    @Test
    public void nextByteValueUnbounded()
    {
        checkDistribution( randomValue::nextByteValue );
    }

    @Test
    public void nextByteValueBounded()
    {
        checkDistribution( () -> randomValue.nextByteValue( BOUND ) );
        checkBounded( () -> randomValue.nextByteValue( BOUND ) );
    }

    @Test
    public void nextFloatValue()
    {
        checkDistribution( randomValue::nextFloatValue );
    }

    @Test
    public void nextDoubleValue()
    {
        checkDistribution( randomValue::nextDoubleValue );
    }

    @Test
    public void nextNumberValue()
    {
        HashSet<Class<? extends NumberValue>> seen = new HashSet<>( NUMBER_TYPES );

        for ( int i = 0; i < ITERATIONS; i++ )
        {
            NumberValue numberValue = randomValue.nextNumberValue();
            assertThat( NUMBER_TYPES, hasItem( numberValue.getClass() ));
            seen.remove( numberValue.getClass() );
        }
        assertThat( seen, empty() );
    }

    @Test
    public void nextDigitString()
    {
        Set<Integer> seenDigits = "0123456789".chars().boxed().collect( Collectors.toSet() );
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            TextValue textValue = randomValue.nextDigitString( 5, 10 );
            String asString = textValue.stringValue();
            for ( int j = 0; j < asString.length(); j++ )
            {
                int ch = asString.charAt( j );
                assertTrue( Character.isDigit( ch ));
                seenDigits.remove( ch );
            }
        }
        assertThat( seenDigits, empty() );
    }

    @Test
    public void nextAlphaString()
    {
        Set<Integer> seenDigits = "ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvxyz".chars().boxed()
                .collect( Collectors.toSet() );
        for ( int i = 0; i < ITERATIONS; i++ )
        {
            TextValue textValue = randomValue.nextAlphaString( 5, 10 );
            String asString = textValue.stringValue();
            for ( int j = 0; j < asString.length(); j++ )
            {
                int ch = asString.charAt( j );
                assertTrue( "Not a character " + ch,  Character.isAlphabetic( ch ));
                seenDigits.remove( ch );
            }
        }
        assertThat( seenDigits, empty() );
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