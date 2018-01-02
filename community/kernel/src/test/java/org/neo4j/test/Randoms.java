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
package org.neo4j.test;

import java.lang.reflect.Array;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.helpers.ArrayUtil;

import static java.lang.Integer.bitCount;

/**
 * Set of useful randomizing utilities, for example randomizing a string or property value of random type and value.
 */
public class Randoms
{
    public interface Configuration
    {
        int stringMinLength();

        int stringMaxLength();

        int stringCharacterSets();

        int arrayMinLength();

        int arrayMaxLength();
    }

    public static class Default implements Configuration
    {
        @Override
        public int stringMinLength()
        {
            return 5;
        }

        @Override
        public int stringMaxLength()
        {
            return 20;
        }

        @Override
        public int stringCharacterSets()
        {
            return CSA_LETTERS_AND_DIGITS;
        }

        @Override
        public int arrayMinLength()
        {
            return 1;
        }

        @Override
        public int arrayMaxLength()
        {
            return 10;
        }
    }

    public static final Configuration DEFAULT = new Default();

    public static final int CS_LOWERCASE_LETTERS = 0x1;
    public static final int CS_UPPERCASE_LETTERS = 0x2;
    public static final int CS_DIGITS = 0x3;
    public static final int CS_SYMBOLS = 0x4;

    public static final int CSA_LETTERS = CS_LOWERCASE_LETTERS | CS_UPPERCASE_LETTERS;
    public static final int CSA_LETTERS_AND_DIGITS = CSA_LETTERS | CS_DIGITS;

    private final Random random;
    private final Configuration configuration;

    public Randoms()
    {
        this( ThreadLocalRandom.current(), DEFAULT );
    }

    public Randoms( Random random, Configuration configuration )
    {
        this.random = random;
        this.configuration = configuration;
    }

    public Randoms fork( Configuration configuration )
    {
        return new Randoms( random, configuration );
    }

    public Random random()
    {
        return random;
    }

    public int intBetween( int min, int max )
    {
        return min + random.nextInt( max-min+1 );
    }

    public String string()
    {
        return string( configuration.stringMinLength(), configuration.stringMaxLength(),
                configuration.stringCharacterSets() );
    }

    public String string( int minLength, int maxLength, int characterSets )
    {
        char[] chars = new char[intBetween( minLength, maxLength )];
        for ( int i = 0; i < chars.length; i++ )
        {
            chars[i] = character( characterSets );
        }
        return String.valueOf( chars );
    }

    public char character( int characterSets )
    {
        int setCount = bitCount( characterSets );
        while ( true )
        {
            int bit = 1 << random.nextInt( setCount );
            if ( (characterSets & bit) != 0 )
            {
                switch ( bit )
                {
                case CS_LOWERCASE_LETTERS: return (char) intBetween( 'a', 'z' );
                case CS_UPPERCASE_LETTERS: return (char) intBetween( 'A', 'Z' );
                case CS_DIGITS: return (char) intBetween( '0', '9' );
                case CS_SYMBOLS: return symbol();
                default: throw new IllegalArgumentException( "Unknown character set " + bit );
                }
            }
        }
    }

    @SuppressWarnings( "unchecked" )
    public <T> T[] selection( T[] among, int min, int max, boolean allowDuplicates )
    {
        assert min <= max;
        int length = min + (max-min == 0 ? 0 : random.nextInt( max-min ) );
        T[] result = (T[]) Array.newInstance( among.getClass().getComponentType(), length );
        for ( int i = 0; i < length; i++ )
        {
            while ( true )
            {
                T candidate = among( among );
                if ( !allowDuplicates && ArrayUtil.contains( result, candidate ) )
                {   // Try again
                    continue;
                }
                result[i] = candidate;
                break;
            }
        }
        return result;
    }

    public <T> T among( T[] among )
    {
        return among[random.nextInt( among.length )];
    }

    public Object propertyValue()
    {
        return propertyValue( propertyType( true ) );
    }

    private byte propertyType( boolean allowArrays )
    {
        return (byte) random.nextInt( allowArrays ? 10 : 9 );
    }

    private Object propertyValue( byte type )
    {
        switch ( type )
        {
        case 0: return random.nextBoolean();
        case 1: return (byte)random.nextInt();
        case 2: return (short)random.nextInt();
        case 3: return character( CSA_LETTERS_AND_DIGITS );
        case 4: return random.nextInt();
        case 5: return random.nextLong();
        case 6: return random.nextFloat();
        case 7: return random.nextDouble();
        case 8: return string();
        case 9:
            int length = intBetween( configuration.arrayMinLength(), configuration.arrayMaxLength() );
            byte componentType = propertyType( false );
            Object itemType = propertyValue( componentType );
            Object array = Array.newInstance( itemType.getClass(), length );
            for ( int i = 0; i < length; i++ )
            {
                Array.set( array, i, propertyValue( componentType ) );
            }
            return array;
        default: throw new IllegalArgumentException( "Unknown value type " + type );
        }
    }

    private char symbol()
    {
        int range = random.nextInt( 5 );
        switch ( range )
        {
        case 0: return (char) intBetween( 33,  47 );
        case 1: return (char) intBetween( 58, 64 );
        case 2: return (char) intBetween( 91, 96 );
        case 3: return (char) intBetween( 123, 126 );
        case 4: return ' ';
        default: throw new IllegalArgumentException( "Unknown symbol range " + range );
        }
    }
}
