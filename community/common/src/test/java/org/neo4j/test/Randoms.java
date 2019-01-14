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
package org.neo4j.test;

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static java.lang.Integer.bitCount;
import static java.lang.Math.abs;
import static java.time.ZoneOffset.UTC;

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

    public static final long NANOS_PER_SECOND = 1_000_000_000L;

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
        return min + random.nextInt( max - min + 1 );
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

    public Object array()
    {
        int length = intBetween( configuration.arrayMinLength(), configuration.arrayMaxLength() );
        byte componentType = propertyType( false );
        Object itemType = propertyValue( componentType );
        Object array = Array.newInstance( itemType.getClass(), length );
        for ( int i = 0; i < length; i++ )
        {
            Array.set( array, i, propertyValue( componentType ) );
        }
        return array;
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
                case CS_LOWERCASE_LETTERS:
                    return (char) intBetween( 'a', 'z' );
                case CS_UPPERCASE_LETTERS:
                    return (char) intBetween( 'A', 'Z' );
                case CS_DIGITS:
                    return (char) intBetween( '0', '9' );
                case CS_SYMBOLS:
                    return symbol();
                default:
                    throw new IllegalArgumentException( "Unknown character set " + bit );
                }
            }
        }
    }

    @SuppressWarnings( "unchecked" )
    public <T> T[] selection( T[] among, int min, int max, boolean allowDuplicates )
    {
        assert min <= max;
        int diff = min == max ? 0 : random.nextInt( max - min );
        int length = min + diff;
        T[] result = (T[]) Array.newInstance( among.getClass().getComponentType(), length );
        for ( int i = 0; i < length; i++ )
        {
            while ( true )
            {
                T candidate = among( among );
                if ( !allowDuplicates && contains( result, candidate ) )
                {   // Try again
                    continue;
                }
                result[i] = candidate;
                break;
            }
        }
        return result;
    }

    private static <T> boolean contains( T[] array, T contains )
    {
        for ( T item : array )
        {
            if ( nullSafeEquals( item, contains ) )
            {
                return true;
            }
        }
        return false;
    }

    private static <T> boolean nullSafeEquals( T first, T other )
    {
        return first == null ? other == null : first.equals( other );
    }

    public <T> T among( T[] among )
    {
        return among[random.nextInt( among.length )];
    }

    public <T> T among( List<T> among )
    {
        return among.get( random.nextInt( among.size() ) );
    }

    public <T> void among( List<T> among, Consumer<T> action )
    {
        if ( !among.isEmpty() )
        {
            T item = among( among );
            action.accept( item );
        }
    }

    public Number numberPropertyValue()
    {
        int type = random.nextInt( 6 );
        switch ( type )
        {
        case 0:
            return (byte) random.nextInt();
        case 1:
            return (short) random.nextInt();
        case 2:
            return random.nextInt();
        case 3:
            return random.nextLong();
        case 4:
            return random.nextFloat();
        case 5:
            return random.nextDouble();
        default:
            throw new IllegalArgumentException( "Unknown value type " + type );
        }
    }

    public Object propertyValue()
    {
        return propertyValue( propertyType( true ) );
    }

    private byte propertyType( boolean allowArrays )
    {
        return (byte) random.nextInt( allowArrays ? 17 : 16 );
    }

    // TODO add Point also
    private Object propertyValue( byte type )
    {
        switch ( type )
        {
        case 0:
            return random.nextBoolean();
        case 1:
            return (byte) random.nextInt();
        case 2:
            return (short) random.nextInt();
        case 3:
            return character( CSA_LETTERS_AND_DIGITS );
        case 4:
            return random.nextInt();
        case 5:
            return random.nextLong();
        case 6:
            return random.nextFloat();
        case 7:
            return random.nextDouble();
        case 8:
            return randomDateTime();
        case 9:
            return randomTime();
        case 10:
            return randomDate();
        case 11:
            return randomLocalDateTime();
        case 12:
            return randomLocalTime();
        case 13:
            return randomPeriod();
        case 14:
            return randomDuration();
        case 15:
            return string();
        case 16:
            return array();
        default:
            throw new IllegalArgumentException( "Unknown value type " + type );
        }
    }

    public OffsetTime randomTime()
    {
        return OffsetTime.ofInstant( randomInstant(), UTC);
    }

    public LocalDateTime randomLocalDateTime()
    {
        return LocalDateTime.ofInstant( randomInstant(), UTC);
    }

    public LocalDate randomDate()
    {
        return LocalDate.ofEpochDay( nextLong( LocalDate.MIN.toEpochDay(), LocalDate.MAX.toEpochDay() ) );
    }

    public LocalTime randomLocalTime()
    {
        return LocalTime.ofNanoOfDay( nextLong( LocalTime.MIN.toNanoOfDay(), LocalTime.MAX.toNanoOfDay() ) );
    }

    private Instant randomInstant()
    {
        return Instant.ofEpochSecond( nextLong( LocalDateTime.MIN.toEpochSecond( UTC ), LocalDateTime.MAX.toEpochSecond( UTC ) ),
                nextLong( NANOS_PER_SECOND ) );
    }

    public ZonedDateTime randomDateTime()
    {
        return randomDateTime( UTC );
    }

    public ZonedDateTime randomDateTime( ZoneId zoneId )
    {
        return ZonedDateTime.ofInstant( randomInstant(), zoneId );
    }

    public Period randomPeriod()
    {
        // Based on Java period (years, months and days)
        return Period.of( random.nextInt(), random.nextInt( 12 ), random.nextInt( 28 ) );
    }

    public Duration randomDuration()
    {
        // Based on java duration (seconds)
        return Duration.ofSeconds( nextLong(), nextLong( -999_999_999, 999_999_999 ) );
    }

    private char symbol()
    {
        int range = random.nextInt( 5 );
        switch ( range )
        {
        case 0:
            return (char) intBetween( 33, 47 );
        case 1:
            return (char) intBetween( 58, 64 );
        case 2:
            return (char) intBetween( 91, 96 );
        case 3:
            return (char) intBetween( 123, 126 );
        case 4:
            return ' ';
        default:
            throw new IllegalArgumentException( "Unknown symbol range " + range );
        }
    }

    public long nextLong()
    {
        return random.nextLong();
    }

    /**
     * Returns a random long between 0 and a positive number
     *
     * @param bound upper bound, exclusive
     */
    public long nextLong( long bound )
    {
        return abs( random.nextLong() ) % bound;
    }

    /**
     * Returns a random long between two positive numbers.
     * @param origin lower bound, inclusive
     * @param bound upper bound, inclusive
     */
    public long nextLong( long origin, long bound )
    {
        return nextLong( (bound - origin) + 1L ) + origin;
    }

    public boolean nextBoolean()
    {
        return random.nextBoolean();
    }

    public int nextInt( int bound )
    {
        return random.nextInt( bound );
    }

    public int nextInt()
    {
        return random.nextInt();
    }

    public void nextBytes( byte[] data )
    {
        random.nextBytes( data );
    }

    public float nextFloat()
    {
        return random.nextFloat();
    }
}
