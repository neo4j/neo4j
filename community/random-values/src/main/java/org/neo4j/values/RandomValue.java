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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanArray;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.ShortArray;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Math.abs;
import static java.time.LocalDate.ofEpochDay;
import static java.time.LocalDateTime.ofInstant;
import static java.time.LocalTime.ofNanoOfDay;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;

/**
 * Helper class that generates random values of all supported types.
 */
public class RandomValue
{
    enum Types
    {
        BOOLEAN,
        BYTE,
        SHORT,
        STRING,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        LOCAL_DATE_TIME,
        DATE,
        LOCAL_TIME,
        PERIOD,
        DURATION,
        TIME,
        DATE_TIME,
        CARTESIAN_POINT,
        CARTESIAN_POINT_3D,
        GEOGRAPHIC_POINT,
        GEOGRAPHIC_POINT_3D,
        ARRAY
    }

    private static Types[] TYPES = Types.values();

    public interface Configuration
    {
        int stringMinLength();

        int stringMaxLength();

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

    //TODO make possible to use SplittableRandom
    private final Random random;
    private final Configuration configuration;
    public static final long NANOS_PER_SECOND = 1_000_000_000L;

    public RandomValue()
    {
        this( ThreadLocalRandom.current(), new Default() );
    }

    public RandomValue( Random random )
    {
        this( random, new Default() );
    }

    public RandomValue( Configuration configuration )
    {
        this( ThreadLocalRandom.current(), configuration );
    }

    public RandomValue( Random random, Configuration configuration )
    {
        this.random = random;
        this.configuration = configuration;
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link LongValue}
     *
     * @return the next pseudorandom uniformly distributed {@link LongValue}
     */
    public LongValue nextLongValue()
    {
        return longValue( random.nextLong() );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link LongValue} between 0 (inclusive) and the specified
     * value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return the next pseudorandom, uniformly distributed {@link LongValue}
     * value between zero (inclusive) and {@code bound} (exclusive)
     */
    public LongValue nextLongValue( long bound )
    {
        return longValue( nextLong( bound ) );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link LongValue} between the specified lower bound
     * (inclusive) and the specified
     * upper bound (inclusive)
     *
     * @param lower the lower bound (inclusive).
     * @param upper the upper bound (inclusive).
     * @return the next pseudorandom, uniformly distributed {@link LongValue}
     * value between {@code lower} (inclusive) and {@code upper} (inclusive)
     */
    public LongValue nextLongValue( long lower, long upper )
    {
        return longValue( nextLong( (upper - lower) + 1L ) + lower );
    }


    /**
     * Returns the next pseudorandom uniformly distributed {@link BooleanValue}
     *
     * @return the next pseudorandom uniformly distributed {@link BooleanValue}
     */
    public BooleanValue nextBooleanValue()
    {
        return Values.booleanValue( random.nextBoolean() );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link IntValue}
     *
     * @return the next pseudorandom uniformly distributed {@link IntValue}
     */
    public IntValue nextIntValue()
    {
        return intValue( random.nextInt() );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link IntValue} between 0 (inclusive) and the specified
     * value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return the next pseudorandom, uniformly distributed {@link IntValue}
     * value between zero (inclusive) and {@code bound} (exclusive)
     */
    public IntValue nextIntValue( int bound )
    {
        return intValue( random.nextInt( bound ) );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link ShortValue}
     *
     * @return the next pseudorandom uniformly distributed {@link ShortValue}
     */
    public ShortValue nextShortValue()
    {
        return shortValue( (short) random.nextInt() );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link ShortValue} between 0 (inclusive) and the specified
     * value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return the next pseudorandom, uniformly distributed {@link ShortValue}
     * value between zero (inclusive) and {@code bound} (exclusive)
     */
    public ShortValue nextShortValue( short bound )
    {
        return shortValue( (short) random.nextInt( bound ) );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link ByteValue}
     *
     * @return the next pseudorandom uniformly distributed {@link ByteValue}
     */
    public ByteValue nextByteValue()
    {
        return byteValue( (byte) random.nextInt() );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link ByteValue} between 0 (inclusive) and the specified
     * value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return the next pseudorandom, uniformly distributed {@link ByteValue}
     * value between zero (inclusive) and {@code bound} (exclusive)
     */
    public ByteValue nextByteValue( byte bound )
    {
        return byteValue( (byte) random.nextInt( bound ) );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link FloatValue}
     *
     * @return the next pseudorandom uniformly distributed {@link FloatValue}
     */
    public FloatValue nextFloatValue()
    {
        return floatValue( random.nextFloat() );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link DoubleValue}
     *
     * @return the next pseudorandom uniformly distributed {@link DoubleValue}
     */
    public DoubleValue nextDoubleValue()
    {
        return doubleValue( random.nextFloat() );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link NumberValue}
     *
     * @return the next pseudorandom uniformly distributed {@link NumberValue}
     */
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

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of digits.
     * <p>
     * The length of the text will be between {@link Configuration#stringMinLength()} and
     * {@link Configuration#stringMaxLength()}
     *
     * @return a {@link TextValue} consisting only of digits.
     */
    public TextValue nextDigitString()
    {
        return nextDigitString( configuration.stringMinLength(), configuration.stringMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of digits with a length between the given values.
     *
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of digits with a length between the given values.
     */
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

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of ascii alphabetic characters.
     * <p>
     * The length of the text will be between {@link Configuration#stringMinLength()} and
     * {@link Configuration#stringMaxLength()}
     *
     * @return a {@link TextValue} consisting only of ascii alphabetic characters.
     */
    public TextValue nextAlphaString()
    {
        return nextAlphaString( configuration.stringMinLength(), configuration.stringMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of ascii alphabetic characters.
     *
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of ascii alphabetic characters.
     */
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

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of ascii alphabetic and numerical characters.
     * <p>
     * The length of the text will be between {@link Configuration#stringMinLength()} and
     * {@link Configuration#stringMaxLength()}
     *
     * @return a {@link TextValue} consisting only of ascii alphabetic and numerical characters.
     */
    public TextValue nextAlphaNumericString()
    {
        return nextAlphaNumericString( configuration.stringMinLength(), configuration.stringMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of ascii alphabetic and numerical characters.
     *
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of ascii alphabetic and numerical characters.
     */
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

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of ascii characters.
     * <p>
     * The length of the text will be between {@link Configuration#stringMinLength()} and
     * {@link Configuration#stringMaxLength()}
     *
     * @return a {@link TextValue} consisting only of ascii characters.
     */
    public TextValue nextAsciiString()
    {
        return nextAsciiString( configuration.stringMinLength(), configuration.stringMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of ascii characters.
     *
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of ascii characters.
     */
    public TextValue nextAsciiString( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            bytes[i] = (byte) intBetween( 0, 127 );

        }
        return Values.utf8Value( bytes );
    }

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of printable ascii characters.
     * <p>
     * The length of the text will be between {@link Configuration#stringMinLength()} and
     * {@link Configuration#stringMaxLength()}
     *
     * @return a {@link TextValue} consisting only of printable ascii characters.
     */
    public TextValue nextPrintableAsciiString()
    {
        return nextPrintableAsciiString( configuration.stringMinLength(), configuration.stringMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of printable ascii characters.
     *
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of printable ascii characters.
     */
    public TextValue nextPrintableAsciiString( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            bytes[i] = (byte) intBetween( 32, 126 );

        }
        return Values.utf8Value( bytes );
    }

    /**
     * Returns the next pseudorandom {@link TextValue}.
     * <p>
     * The length of the text will be between {@link Configuration#stringMinLength()} and
     * {@link Configuration#stringMaxLength()}
     *
     * @return a random {@link TextValue}.
     */
    public TextValue nextString()
    {
        return nextString( configuration.stringMinLength(), configuration.stringMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextValue}.
     *
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a random {@link TextValue}.
     */
    public TextValue nextString( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        UTF8StringValueBuilder builder = new UTF8StringValueBuilder( nextPowerOf2( length ) );

        for ( int i = 0; i < length; i++ )
        {
            boolean validCodePoint = false;

            //TODO it is a bit inefficient to generate integer and then retry if we end up in an invalid range
            //instead we could always generate values in a valid range, however there are a lot of ranges with holes
            //so the code will probably become a bit unwieldly
            while ( !validCodePoint )
            {
                int codePoint = intBetween( Character.MIN_CODE_POINT, Character.MAX_CODE_POINT );
                switch ( Character.getType( codePoint ) )
                {
                case Character.UNASSIGNED:
                case Character.PRIVATE_USE:
                case Character.SURROGATE:
                    continue;
                default:
                    builder.addCodePoint( codePoint );
                    validCodePoint = true;
                }
            }
        }
        return builder.build();
    }

    /**
     * Returns the next pseudorandom {@link Value}, distributed uniformly among the supported Value types.
     * <p>
     * The length of strings will be governed by {@link Configuration#stringMinLength()} and
     * {@link Configuration#stringMaxLength()} and
     * the length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link Value}
     */
    public Value nextValue()
    {
        Types type = nextType();
        switch ( type )
        {
        case BOOLEAN:
            return nextBooleanValue();
        case BYTE:
            return nextByteValue();
        case SHORT:
            return nextShortValue();
        case STRING:
            return nextString();
        case INT:
            return nextIntValue();
        case LONG:
            return nextLongValue();
        case FLOAT:
            return nextFloatValue();
        case DOUBLE:
            return nextDoubleValue();
        case LOCAL_DATE_TIME:
            return nextLocalDateTimeValue();
        case DATE:
            return nextDateValue();
        case LOCAL_TIME:
            return nextLocalTimeValue();
        case PERIOD:
            return nextPeriod();
        case DURATION:
            return nextDuration();
        case TIME:
            return nextTimeValue();
        case DATE_TIME:
            return nextDateTimeValue();
        case CARTESIAN_POINT:
            return nextCartesianPoint();
        case CARTESIAN_POINT_3D:
            return nextCartesian3DPoint();
        case GEOGRAPHIC_POINT:
            return nextGeographicPoint();
        case GEOGRAPHIC_POINT_3D:
            return nextGeographic3DPoint();
        case ARRAY:
            return nextArray();

        default:
            throw new IllegalArgumentException( "Unknown value type: " + type );
        }
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue}, distributed uniformly among the supported Value types.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link ArrayValue}
     */
    public ArrayValue nextArray()
    {
        return nextArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue}, distributed uniformly among the supported Value types where
     * the length of the array is given by the provided values.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link ArrayValue}
     */
    public ArrayValue nextArray( int minLength, int maxLength )
    {
        while ( true )
        {
            Types type = nextType();
            switch ( type )
            {
            case BOOLEAN:
                return nextBooleanArray( minLength, maxLength );
            case BYTE:
                return nextByteArray( minLength, maxLength );
            case SHORT:
                return nextShortArray( minLength, maxLength );
            case STRING:
                return nextStringArray( minLength, maxLength );
            case INT:
                return nextIntArray( minLength, maxLength );
            case LONG:
                return nextLongArray( minLength, maxLength );
            case FLOAT:
                return nextFloatArray( minLength, maxLength );
            case DOUBLE:
                return nextDoubleArray( minLength, maxLength );
            case LOCAL_DATE_TIME:
                return nextLocalDateTimeArray( minLength, maxLength );
            case DATE:
                return nextDateArray( minLength, maxLength );
            case LOCAL_TIME:
                return nextLocalTimeArray( minLength, maxLength );
            case PERIOD:
                return nextPeriodArray( minLength, maxLength );
            case DURATION:
                return nextDurationArray( minLength, maxLength );
            case TIME:
                return nextTimeArray( minLength, maxLength );
            case DATE_TIME:
                return nextDateTimeArray( minLength, maxLength );
            case CARTESIAN_POINT:
                return nextCartesianPointArray( minLength, maxLength );
            case CARTESIAN_POINT_3D:
                return nextCartesian3DPointArray( minLength, maxLength );
            case GEOGRAPHIC_POINT:
                return nextGeographicPointArray( minLength, maxLength );
            case GEOGRAPHIC_POINT_3D:
                return nextGeographic3DPointArray( minLength, maxLength );
            case ARRAY://we don't want nested arrays
                continue;
            default:
                throw new IllegalArgumentException( "Unknown value type: " + type );
            }
        }
    }


    /**
     * Returns the next pseudorandom {@link PointArray} of cartesian two-dimensional points.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link PointArray} of cartesian points two-dimensional points.
     */
    public PointArray nextCartesianPointArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        PointValue[] array = new PointValue[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = nextCartesianPoint();
        }
        return Values.pointArray( array );
    }

    /**
     * Returns the next pseudorandom {@link PointArray} of cartesian three-dimensional points.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link PointArray} of cartesian points three-dimensional points.
     */
    public PointArray nextCartesian3DPointArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        PointValue[] array = new PointValue[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = nextCartesian3DPoint();
        }
        return Values.pointArray( array );
    }

    /**
     * Returns the next pseudorandom {@link PointArray} of geographic two-dimensional points.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link PointArray} of geographic two-dimensional points.
     */
    public PointArray nextGeographicPointArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        PointValue[] array = new PointValue[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = nextGeographicPoint();
        }
        return Values.pointArray( array );
    }

    /**
     * Returns the next pseudorandom {@link PointArray} of geographic three-dimensional points.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link PointArray} of geographic three-dimensional points.
     */
    public PointArray nextGeographic3DPointArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        PointValue[] array = new PointValue[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = nextGeographic3DPoint();
        }
        return Values.pointArray( array );
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of local-time elements.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link ArrayValue} of local-time elements.
     */
    public ArrayValue nextLocalTimeArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        LocalTime[] array = new LocalTime[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = ofNanoOfDay( nextLong( LocalTime.MIN.toNanoOfDay(), LocalTime.MAX.toNanoOfDay() ) );
        }
        return Values.localTimeArray( array );
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of time elements.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link ArrayValue} of time elements.
     */
    public ArrayValue nextTimeArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        OffsetTime[] array = new OffsetTime[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = OffsetTime.ofInstant( randomInstant(), UTC );
        }
        return Values.timeArray( array );
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of local date-time elements.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link ArrayValue} of local date-time elements.
     */
    public ArrayValue nextDateTimeArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        ZonedDateTime[] array = new ZonedDateTime[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = ZonedDateTime.ofInstant( randomInstant(), UTC );
        }
        return Values.dateTimeArray( array );
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of local-date-time elements.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link ArrayValue} of local-date-time elements.
     */
    public ArrayValue nextLocalDateTimeArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        LocalDateTime[] array = new LocalDateTime[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = ofInstant( randomInstant(), UTC );
        }
        return Values.localDateTimeArray( array );
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of date elements.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link ArrayValue} of date elements.
     */
    public ArrayValue nextDateArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        LocalDate[] array = new LocalDate[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = ofEpochDay( nextLong( LocalDate.MIN.toEpochDay(), LocalDate.MAX.toEpochDay() ) );
        }
        return Values.dateArray( array );
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of period elements.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link ArrayValue} of period elements.
     */
    public ArrayValue nextPeriodArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        Period[] array = new Period[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = Period.of( random.nextInt(), random.nextInt( 12 ), random.nextInt( 28 ) );
        }
        return Values.durationArray( array );
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of duration elements.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link ArrayValue} of duration elements.
     */
    public ArrayValue nextDurationArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        Duration[] array = new Duration[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = Duration.of( nextLong( DAYS.getDuration().getSeconds() ), ChronoUnit.SECONDS );
        }
        return Values.durationArray( array );
    }

    /**
     * Returns the next pseudorandom {@link DoubleArray}.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link DoubleArray}.
     */
    public DoubleArray nextDoubleArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        double[] doubles = new double[length];
        for ( int i = 0; i < length; i++ )
        {
            doubles[i] = random.nextDouble();
        }
        return Values.doubleArray( doubles );
    }

    /**
     * Returns the next pseudorandom {@link FloatArray}.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link FloatArray}.
     */
    public FloatArray nextFloatArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        float[] floats = new float[length];
        for ( int i = 0; i < length; i++ )
        {
            floats[i] = random.nextFloat();
        }
        return Values.floatArray( floats );
    }

    /**
     * Returns the next pseudorandom {@link LongArray}.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link LongArray}.
     */
    public LongArray nextLongArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        long[] longs = new long[length];
        for ( int i = 0; i < length; i++ )
        {
            longs[i] = random.nextLong();
        }
        return Values.longArray( longs );
    }

    /**
     * Returns the next pseudorandom {@link IntArray}.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link IntArray}.
     */
    public IntArray nextIntArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        int[] ints = new int[length];
        for ( int i = 0; i < length; i++ )
        {
            ints[i] = random.nextInt();
        }
        return Values.intArray( ints );
    }

    /**
     * Returns the next pseudorandom {@link BooleanArray}.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link BooleanArray}.
     */
    public BooleanArray nextBooleanArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        boolean[] booleans = new boolean[length];
        for ( int i = 0; i < length; i++ )
        {
            booleans[i] = random.nextBoolean();
        }
        return Values.booleanArray( booleans );
    }

    /**
     * Returns the next pseudorandom {@link ByteArray}.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link ByteArray}.
     */
    public ByteArray nextByteArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            bytes[i] = (byte) random.nextInt();
        }
        return Values.byteArray( bytes );
    }

    /**
     * Returns the next pseudorandom {@link ShortArray}.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link ShortArray}.
     */
    public ShortArray nextShortArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        short[] shorts = new short[length];
        for ( int i = 0; i < length; i++ )
        {
            shorts[i] = (short) random.nextInt();
        }
        return Values.shortArray( shorts );
    }

    /**
     * Returns the next pseudorandom {@link TextArray}.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link TextArray}.
     */
    public TextArray nextStringArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        String[] strings = new String[length];
        for ( int i = 0; i < length; i++ )
        {
            strings[i] = nextString().stringValue();
        }
        return Values.stringArray( strings );
    }

    /**
     * Returns the next pseudorandom {@link TimeValue}.
     *
     * @return the next pseudorandom {@link TimeValue}.
     */
    public TimeValue nextTimeValue()
    {
        return time( OffsetTime.ofInstant( randomInstant(), UTC ) );
    }

    /**
     * Returns the next pseudorandom {@link LocalDateTimeValue}.
     *
     * @return the next pseudorandom {@link LocalDateTimeValue}.
     */
    public LocalDateTimeValue nextLocalDateTimeValue()
    {
        return localDateTime( ofInstant( randomInstant(), UTC ) );
    }

    /**
     * Returns the next pseudorandom {@link DateValue}.
     *
     * @return the next pseudorandom {@link DateValue}.
     */
    public DateValue nextDateValue()
    {
        return date( ofEpochDay( nextLong( LocalDate.MIN.toEpochDay(), LocalDate.MAX.toEpochDay() ) ) );
    }

    /**
     * Returns the next pseudorandom {@link LocalTimeValue}.
     *
     * @return the next pseudorandom {@link LocalTimeValue}.
     */
    public LocalTimeValue nextLocalTimeValue()
    {
        return localTime( ofNanoOfDay( nextLong( LocalTime.MIN.toNanoOfDay(), LocalTime.MAX.toNanoOfDay() ) ) );
    }

    /**
     * Returns the next pseudorandom {@link DateTimeValue}.
     *
     * @return the next pseudorandom {@link DateTimeValue}.
     */
    public DateTimeValue nextDateTimeValue()
    {
        return datetime( ZonedDateTime.ofInstant( randomInstant(), UTC ) );
    }

    /**
     * Returns the next pseudorandom {@link DurationValue} based on periods.
     *
     * @return the next pseudorandom {@link DurationValue}.
     */
    public DurationValue nextPeriod()
    {
        // Based on Java period (years, months and days)
        return duration( Period.of( random.nextInt(), random.nextInt( 12 ), random.nextInt( 28 ) ) );
    }

    /**
     * Returns the next pseudorandom {@link DurationValue} based on duration.
     *
     * @return the next pseudorandom {@link DurationValue}.
     */
    public DurationValue nextDuration()
    {
        // Based on java duration (seconds)
        return duration( Duration.of( nextLong( DAYS.getDuration().getSeconds() ), ChronoUnit.SECONDS ) );
    }

    /**
     * Returns the next pseudorandom two-dimensional cartesian {@link PointValue}.
     *
     * @return the next pseudorandom two-dimensional cartesian {@link PointValue}.
     */
    public PointValue nextCartesianPoint()
    {
        return Values.pointValue( CoordinateReferenceSystem.Cartesian, random.nextDouble(), random.nextDouble() );
    }

    /**
     * Returns the next pseudorandom three-dimensional cartesian {@link PointValue}.
     *
     * @return the next pseudorandom three-dimensional cartesian {@link PointValue}.
     */
    public PointValue nextCartesian3DPoint()
    {
        return Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, random.nextDouble(),
                random.nextDouble(), random.nextDouble() );
    }

    /**
     * Returns the next pseudorandom two-dimensional geographic {@link PointValue}.
     *
     * @return the next pseudorandom two-dimensional geographic {@link PointValue}.
     */
    public PointValue nextGeographicPoint()
    {
        double longitude = random.nextDouble() * 360.0 - 180.0;
        double latitude = random.nextDouble() * 180.0 - 90.0;
        return Values.pointValue( CoordinateReferenceSystem.WGS84, longitude, latitude );
    }

    /**
     * Returns the next pseudorandom three-dimensional geographic {@link PointValue}.
     *
     * @return the next pseudorandom three-dimensional geographic {@link PointValue}.
     */
    public PointValue nextGeographic3DPoint()
    {
        double longitude = random.nextDouble() * 360.0 - 180.0;
        double latitude = random.nextDouble() * 180.0 - 90.0;
        return Values.pointValue( CoordinateReferenceSystem.WGS84_3D, longitude, latitude,
                random.nextDouble() * 10000 );
    }

    private Instant randomInstant()
    {
        return Instant.ofEpochSecond(
                nextLong( LocalDateTime.MIN.toEpochSecond( UTC ), LocalDateTime.MAX.toEpochSecond( UTC ) ),
                nextLong( NANOS_PER_SECOND ) );
    }

    private int nextPowerOf2( int i )
    {
        return 1 << (32 - Integer.numberOfLeadingZeros( i ));
    }

    private int intBetween( int min, int max )
    {
        return min + random.nextInt( max - min + 1 );
    }

    private long nextLong( long bound )
    {
        return abs( random.nextLong() ) % bound;
    }

    private long nextLong( long origin, long bound )
    {
        return nextLong( (bound - origin) + 1L ) + origin;
    }

    private Types nextType()
    {
        return TYPES[random.nextInt( TYPES.length )];
    }
}
