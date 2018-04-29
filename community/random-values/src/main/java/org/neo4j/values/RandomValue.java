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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static java.lang.Math.abs;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;

public class RandomValue
{
    //TODO make possible to use SplittableRandom
    private final Random random;

    public RandomValue()
    {
        this( ThreadLocalRandom.current() );
    }

    public RandomValue( Random random )
    {
        this.random = random;
    }

    LongValue nextLongValue()
    {
        return longValue( random.nextLong() );
    }

    LongValue nextLongValue( long bound )
    {
        return longValue( nextLong( bound ) );
    }

    public LongValue nextLongValue( long lower, long upper )
    {
        return longValue( nextLong( (upper - lower) + 1L ) + lower );
    }

    public BooleanValue nextBooleanValue()
    {
        return Values.booleanValue( random.nextBoolean() );
    }

    public IntValue nextIntValue( int bound )
    {
        return intValue( random.nextInt( bound ) );
    }

    public IntValue nextIntValue()
    {
        return intValue( random.nextInt() );
    }

    public ShortValue nextShortValue( short bound )
    {
        return shortValue( (short) random.nextInt( bound ) );
    }

    public ShortValue nextShortValue()
    {
        return shortValue( (short) random.nextInt() );
    }

    public ByteValue nextByteValue( byte bound )
    {
        return byteValue( (byte) random.nextInt( bound ) );
    }

    public ByteValue nextByteValue()
    {
        return byteValue( (byte) random.nextInt() );
    }

    public FloatValue nextFloatValue()
    {
        return floatValue( random.nextFloat() );
    }

    public DoubleValue nextDoubleValue()
    {
        return doubleValue( random.nextFloat() );
    }

    public NumberValue nextNumberValue()
    {
        int type = random.nextInt( 6 );
        switch ( type )
        {
        case 0:
            return nextByteValue();
        case 1:
            return nextShortValue();
        case 2:
            return nextIntValue();
        case 3:
            return nextLongValue();
        case 4:
            return nextFloatValue();
        case 5:
            return nextDoubleValue();
        default:
            throw new IllegalArgumentException( "Unknown value type " + type );
        }
    }

    public TextValue nextDigitString( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            bytes[i] = (byte) intBetween( '0', '9' );
        }

        return Values.utf8Value( bytes );
    }

    public TextValue nextAlphaString( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            if ( random.nextBoolean() )
            {
                bytes[i] = (byte) intBetween( 'A', 'Z' );
            }
            else
            {
                bytes[i] = (byte) intBetween( 'a', 'z' );
            }
        }

        return Values.utf8Value( bytes );
    }

    public TextValue nextAlphaNumericString( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            switch ( random.nextInt( 4 ) )
            {
            case 0:
                bytes[i] = (byte) intBetween( 'A', 'Z' );
                break;
            case 1:
                bytes[i] = (byte) intBetween( 'a', 'z' );
                break;
            //We want digits being roughly as frequent as letters
            case 2:
            case 3:
                bytes[i] = (byte) intBetween( '0', '9' );
            }
        }

        return Values.utf8Value( bytes );
    }

    private int intBetween( int min, int max )
    {
        return min + random.nextInt( max - min + 1 );
    }

    private long nextLong( long bound )
    {
        return abs( random.nextLong() ) % bound;
    }
}
