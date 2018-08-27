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
package org.neo4j.values.storable;

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

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
 * Helper class that generates generator values of all supported types.
 */
public class RandomValues
{
    public enum Types
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

    public interface Configuration
    {
        int stringMinLength();

        int stringMaxLength();

        int arrayMinLength();

        int arrayMaxLength();

        int maxCodePoint();
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

        @Override
        public int maxCodePoint()
        {
            return Character.MAX_CODE_POINT;
        }
    }

    public static final int MAX_16_BIT_CODE_POINT = Character.MIN_SUPPLEMENTARY_CODE_POINT - 1;
    public static Configuration DEFAULT_CONFIGURATION = new Default();
    private static Types[] TYPES = Types.values();
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final Generator generator;
    private final Configuration configuration;

    private RandomValues( Generator generator )
    {
        this( generator, DEFAULT_CONFIGURATION );
    }

    private RandomValues( Generator generator, Configuration configuration )
    {
        this.generator = generator;
        this.configuration = configuration;
    }

    /**
     * Create a {@code RandomValues} with default configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create()
    {
        return new RandomValues( new RandomGenerator( ThreadLocalRandom.current() ) );
    }

    /**
     * Create a {@code RandomValues} with the given configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create( Configuration configuration )
    {
        return new RandomValues( new RandomGenerator( ThreadLocalRandom.current() ), configuration );
    }

    /**
     * Create a {@code RandomValues} using the given {@link Random} with given configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create( Random random, Configuration configuration )
    {
        return new RandomValues( new RandomGenerator( random ), configuration );
    }

    /**
     * Create a {@code RandomValues} using the given {@link Random} with default configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create( Random random )
    {
        return new RandomValues( new RandomGenerator( random ) );
    }

    /**
     * Create a {@code RandomValues} using the given {@link SplittableRandom} with given configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create( SplittableRandom random, Configuration configuration )
    {
        return new RandomValues( new SplittableRandomGenerator( random ), configuration );
    }

    /**
     * Create a {@code RandomValues} using the given {@link SplittableRandom} with default configuration
     *
     * @return a {@code RandomValues} instance
     */
    public static RandomValues create( SplittableRandom random )
    {
        return new RandomValues( new SplittableRandomGenerator( random ) );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link LongValue}
     *
     * @return the next pseudorandom uniformly distributed {@link LongValue}
     */
    public LongValue nextLongValue()
    {
        return longValue( generator.nextLong() );
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
        return Values.booleanValue( generator.nextBoolean() );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@code boolean}
     *
     * @return the next pseudorandom uniformly distributed {@code boolean}
     */
    public boolean nextBoolean()
    {
        return generator.nextBoolean();
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link IntValue}
     *
     * @return the next pseudorandom uniformly distributed {@link IntValue}
     */
    public IntValue nextIntValue()
    {
        return intValue( generator.nextInt() );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@code int}
     *
     * @return the next pseudorandom uniformly distributed {@code int}
     */
    public int nextInt()
    {
        return generator.nextInt();
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
        return intValue( generator.nextInt( bound ) );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@code int} between 0 (inclusive) and the specified
     * value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return the next pseudorandom, uniformly distributed {@code int}
     * value between zero (inclusive) and {@code bound} (exclusive)
     */
    public int nextInt( int bound )
    {
        return generator.nextInt( bound );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link ShortValue}
     *
     * @return the next pseudorandom uniformly distributed {@link ShortValue}
     */
    public ShortValue nextShortValue()
    {
        return shortValue( (short) generator.nextInt() );
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
        return shortValue( (short) generator.nextInt( bound ) );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link ByteValue}
     *
     * @return the next pseudorandom uniformly distributed {@link ByteValue}
     */
    public ByteValue nextByteValue()
    {
        return byteValue( (byte) generator.nextInt() );
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
        return byteValue( (byte) generator.nextInt( bound ) );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link FloatValue} between 0 (inclusive) and the specified
     * 1.0 (exclusive)
     *
     * @return the next pseudorandom uniformly distributed {@link FloatValue}
     */
    public FloatValue nextFloatValue()
    {
        return floatValue( generator.nextFloat() );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@code float} between 0 (inclusive) and the specified
     * 1.0 (exclusive)
     *
     * @return the next pseudorandom uniformly distributed {@code float}
     */
    public float nextFloat()
    {
        return generator.nextFloat();
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link DoubleValue}
     *
     * @return the next pseudorandom uniformly distributed {@link DoubleValue}
     */
    public DoubleValue nextDoubleValue()
    {
        return doubleValue( generator.nextDouble() );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@link NumberValue}
     *
     * @return the next pseudorandom uniformly distributed {@link NumberValue}
     */
    public NumberValue nextNumberValue()
    {
        int type = generator.nextInt( 6 );
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
    public TextValue nextAlphaTextValue()
    {
        return nextAlphaTextValue( configuration.stringMinLength(), configuration.stringMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of ascii alphabetic characters.
     *
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of ascii alphabetic characters.
     */
    public TextValue nextAlphaTextValue( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            if ( generator.nextBoolean() )
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
    public TextValue nextAlphaNumericTextValue()
    {
        return nextAlphaNumericTextValue( configuration.stringMinLength(), configuration.stringMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of ascii alphabetic and numerical characters.
     *
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of ascii alphabetic and numerical characters.
     */
    public TextValue nextAlphaNumericTextValue( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            int nextInt = generator.nextInt( 4 );
            switch ( nextInt )
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
                break;
            default:
                throw new IllegalArgumentException( nextInt + " is not an expected value" );
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
    public TextValue nextAsciiTextValue()
    {
        return nextAsciiTextValue( configuration.stringMinLength(), configuration.stringMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of ascii characters.
     *
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of ascii characters.
     */
    public TextValue nextAsciiTextValue( int minLength, int maxLength )
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
    public TextValue nextPrintableAsciiTextValue()
    {
        return nextPrintableAsciiTextValue( configuration.stringMinLength(), configuration.stringMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextValue} consisting only of printable ascii characters.
     *
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a {@link TextValue} consisting only of printable ascii characters.
     */
    public TextValue nextPrintableAsciiTextValue( int minLength, int maxLength )
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
     * @return a generator {@link TextValue}.
     */
    public TextValue nextTextValue()
    {
        return nextTextValue( configuration.stringMinLength(), configuration.stringMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextValue}.
     *
     * @param minLength the minimum length of the string
     * @param maxLength the maximum length of the string
     * @return a generator {@link TextValue}.
     */
    public TextValue nextTextValue( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        UTF8StringValueBuilder builder = new UTF8StringValueBuilder( nextPowerOf2( length ) );

        for ( int i = 0; i < length; i++ )
        {
            builder.addCodePoint( nextValidCodePoint() );
        }
        return builder.build();
    }

    /**
     * Generate next code point that is valid for composition of a string.
     * Additional limitation on code point range is given by configuration.
     *
     * @return A pseudorandom valid code point
     */
    private int nextValidCodePoint()
    {
        int codePoint;
        int type;
        do
        {
            codePoint = intBetween( Character.MIN_CODE_POINT, configuration.maxCodePoint() );
            type = Character.getType( codePoint );
        }
        while ( type == Character.UNASSIGNED ||
                type == Character.PRIVATE_USE ||
                type == Character.SURROGATE );
        return codePoint;
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
        return nextValue( nextType() );
    }

    /**
     * Returns the next pseudorandom {@link Value} of given type
     * <p>
     * The length of strings will be governed by {@link Configuration#stringMinLength()} and
     * {@link Configuration#stringMaxLength()} and
     * the length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link Value} of given type
     */
    public Value nextValue( Types type )
    {
        switch ( type )
        {
        case BOOLEAN:
            return nextBooleanValue();
        case BYTE:
            return nextByteValue();
        case SHORT:
            return nextShortValue();
        case STRING:
            return nextTextValue();
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
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link PointArray} of cartesian points two-dimensional points.
     */
    public PointArray nextCartesianPointArray()
    {
        return nextCartesianPointArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link PointArray} of cartesian points three-dimensional points.
     */
    public PointArray nextCartesian3DPointArray()
    {
        return nextCartesian3DPointArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link PointArray} of geographic two-dimensional points.
     */
    public PointArray nextGeographicPointArray()
    {
        return nextGeographicPointArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link PointArray} of geographic three-dimensional points.
     */
    public PointArray nextGeographic3DPointArray()
    {
        return nextGeographic3DPointArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link ArrayValue} of local-time elements.
     */
    public ArrayValue nextLocalTimeArray()
    {
        return nextLocalTimeArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        LocalTime[] array = nextLocalTimeArrayRaw( minLength, maxLength );
        return Values.localTimeArray( array );
    }

    public LocalTime[] nextLocalTimeArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        LocalTime[] array = new LocalTime[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = ofNanoOfDay( nextLong( LocalTime.MIN.toNanoOfDay(), LocalTime.MAX.toNanoOfDay() ) );
        }
        return array;
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of time elements.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link ArrayValue} of time elements.
     */
    public ArrayValue nextTimeArray()
    {
        return nextTimeArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        OffsetTime[] array = nextTimeArrayRaw( minLength, maxLength );
        return Values.timeArray( array );
    }

    public OffsetTime[] nextTimeArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        OffsetTime[] array = new OffsetTime[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = OffsetTime.ofInstant( randomInstant(), UTC );
        }
        return array;
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of local date-time elements.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link ArrayValue} of local date-time elements.
     */
    public ArrayValue nextDateTimeArray()
    {
        return nextDateTimeArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        ZonedDateTime[] array = nextDateTimeArrayRaw( minLength, maxLength );
        return Values.dateTimeArray( array );
    }

    public ZonedDateTime[] nextDateTimeArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        ZonedDateTime[] array = new ZonedDateTime[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = ZonedDateTime.ofInstant( randomInstant(), UTC );
        }
        return array;
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of local-date-time elements.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link ArrayValue} of local-date-time elements.
     */
    public ArrayValue nextLocalDateTimeArray()
    {
        return nextLocalDateTimeArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        LocalDateTime[] array = nextLocalDateTimeArrayRaw( minLength, maxLength );
        return Values.localDateTimeArray( array );
    }

    public LocalDateTime[] nextLocalDateTimeArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        LocalDateTime[] array = new LocalDateTime[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = ofInstant( randomInstant(), UTC );
        }
        return array;
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of date elements.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link ArrayValue} of date elements.
     */
    public ArrayValue nextDateArray()
    {
        return nextDateArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        LocalDate[] array = nextDateArrayRaw( minLength, maxLength );
        return Values.dateArray( array );
    }

    public LocalDate[] nextDateArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        LocalDate[] array = new LocalDate[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = ofEpochDay( nextLong( LocalDate.MIN.toEpochDay(), LocalDate.MAX.toEpochDay() ) );
        }
        return array;
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of period elements.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link ArrayValue} of period elements.
     */
    public ArrayValue nextPeriodArray()
    {
        return nextPeriodArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        Period[] array = nextPeriodArrayRaw( minLength, maxLength );
        return Values.durationArray( array );
    }

    public Period[] nextPeriodArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        Period[] array = new Period[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = Period.of( generator.nextInt(), generator.nextInt( 12 ), generator.nextInt( 28 ) );
        }
        return array;
    }

    /**
     * Returns the next pseudorandom {@link ArrayValue} of duration elements.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link ArrayValue} of duration elements.
     */
    public ArrayValue nextDurationArray()
    {
        return nextDurationArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        Duration[] array = nextDurationArrayRaw( minLength, maxLength );
        return Values.durationArray( array );
    }

    public Duration[] nextDurationArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        Duration[] array = new Duration[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = Duration.of( nextLong( DAYS.getDuration().getSeconds() ), ChronoUnit.SECONDS );
        }
        return array;
    }

    /**
     * Returns the next pseudorandom {@link DoubleArray}.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link DoubleArray}.
     */
    public DoubleArray nextDoubleArray()
    {
        return nextDoubleArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        double[] array = nextDoubleArrayRaw( minLength, maxLength );
        return Values.doubleArray( array );
    }

    public double[] nextDoubleArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        double[] doubles = new double[length];
        for ( int i = 0; i < length; i++ )
        {
            doubles[i] = generator.nextDouble();
        }
        return doubles;
    }

    /**
     * Returns the next pseudorandom {@link FloatArray}.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link FloatArray}.
     */
    public FloatArray nextFloatArray()
    {
        return nextFloatArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        float[] array = nextFloatArrayRaw( minLength, maxLength );
        return Values.floatArray( array );
    }

    public float[] nextFloatArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        float[] floats = new float[length];
        for ( int i = 0; i < length; i++ )
        {
            floats[i] = generator.nextFloat();
        }
        return floats;
    }

    /**
     * Returns the next pseudorandom {@link LongArray}.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link LongArray}
     */
    public LongArray nextLongArray()
    {
        return nextLongArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        long[] array = nextLongArrayRaw( minLength, maxLength );
        return Values.longArray( array );
    }

    public long[] nextLongArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        long[] longs = new long[length];
        for ( int i = 0; i < length; i++ )
        {
            longs[i] = generator.nextLong();
        }
        return longs;
    }

    /**
     * Returns the next pseudorandom {@link IntArray}.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link IntArray}.
     */
    public IntArray nextIntArray()
    {
        return nextIntArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        int[] array = nextIntArrayRaw( minLength, maxLength );
        return Values.intArray( array );
    }

    public int[] nextIntArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        int[] ints = new int[length];
        for ( int i = 0; i < length; i++ )
        {
            ints[i] = generator.nextInt();
        }
        return ints;
    }

    /**
     * Returns the next pseudorandom {@link BooleanArray}.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link BooleanArray}.
     */
    public BooleanArray nextBooleanArray()
    {
        return nextBooleanArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        boolean[] array = nextBooleanArrayRaw( minLength, maxLength );
        return Values.booleanArray( array );
    }

    public boolean[] nextBooleanArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        boolean[] booleans = new boolean[length];
        for ( int i = 0; i < length; i++ )
        {
            booleans[i] = generator.nextBoolean();
        }
        return booleans;
    }

    /**
     * Returns the next pseudorandom {@link ByteArray}.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link ByteArray}.
     */
    public ByteArray nextByteArray()
    {
        return nextByteArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        byte[] array = nextByteArrayRaw( minLength, maxLength );
        return Values.byteArray( array );
    }

    public byte[] nextByteArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        byte[] bytes = new byte[length];
        int index = 0;
        while ( index < length )
        {
            //For each random int we get up to four random bytes
            int rand = nextInt();
            int numBytesToShift = Math.min( length - index, Integer.BYTES );

            //byte 4   byte 3   byte 2   byte 1
            //aaaaaaaa bbbbbbbb cccccccc dddddddd
            while ( numBytesToShift > 0 )
            {
                bytes[index++] = (byte) rand;
                numBytesToShift--;
                rand >>= Byte.SIZE;
            }
        }
        return bytes;
    }

    /**
     * Returns the next pseudorandom {@link ShortArray}.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link ShortArray}.
     */
    public ShortArray nextShortArray()
    {
        return nextShortArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
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
        short[] array = nextShortArrayRaw( minLength, maxLength );
        return Values.shortArray( array );
    }

    public short[] nextShortArrayRaw( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        short[] shorts = new short[length];
        for ( int i = 0; i < length; i++ )
        {
            shorts[i] = (short) generator.nextInt();
        }
        return shorts;
    }

    /**
     * Returns the next pseudorandom alpha-numeric {@link TextArray}.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link TextArray}.
     */
    public TextArray nextAlphaNumericStringArray()
    {
        return nextAlphaNumericStringArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
    }

    /**
     * Returns the next pseudorandom alpha-numeric {@link TextArray}.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link TextArray}.
     */
    public TextArray nextAlphaNumericStringArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        String[] strings = new String[length];
        for ( int i = 0; i < length; i++ )
        {
            strings[i] = nextAlphaNumericTextValue().stringValue();
        }
        return Values.stringArray( strings );
    }

    /**
     * Returns the next pseudorandom {@link TextArray}.
     * <p>
     * The length of arrays will be governed by {@link Configuration#arrayMinLength()} and
     * {@link Configuration#arrayMaxLength()}
     *
     * @return the next pseudorandom {@link TextArray}.
     */
    public TextArray nextStringArray()
    {
        return nextStringArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
    }

    /**
     * Returns the next pseudorandom {@link TextArray}.
     *
     * @param minLength the minimum length of the array
     * @param maxLength the maximum length of the array
     * @return the next pseudorandom {@link TextArray}.
     */
    private TextArray nextStringArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        String[] strings = new String[length];
        for ( int i = 0; i < length; i++ )
        {
            strings[i] = nextTextValue().stringValue();
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
        return duration( Period.of( generator.nextInt(), generator.nextInt( 12 ), generator.nextInt( 28 ) ) );
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
     * Returns a randomly selected temporal value spread uniformly over the supported types.
     *
     * @return a randomly selected temporal value
     */
    public Value nextTemporalValue()
    {
        int nextInt = generator.nextInt( 6 );
        switch ( nextInt )
        {
        case 0:
            return nextDateValue();

        case 1:
            return nextLocalDateTimeValue();

        case 2:
            return nextDateTimeValue();

        case 3:
            return nextLocalTimeValue();

        case 4:
            return nextTimeValue();

        case 5:
            return nextDuration();

        default:
            throw new IllegalArgumentException( nextInt + " not a valid temporal type" );
        }
    }

    /**
     * Returns the next pseudorandom two-dimensional cartesian {@link PointValue}.
     *
     * @return the next pseudorandom two-dimensional cartesian {@link PointValue}.
     */
    public PointValue nextCartesianPoint()
    {
        double x = randomCartesianCoordinate();
        double y = randomCartesianCoordinate();
        return Values.pointValue( CoordinateReferenceSystem.Cartesian, x, y );
    }

    /**
     * Returns the next pseudorandom three-dimensional cartesian {@link PointValue}.
     *
     * @return the next pseudorandom three-dimensional cartesian {@link PointValue}.
     */
    public PointValue nextCartesian3DPoint()
    {
        double x = randomCartesianCoordinate();
        double y = randomCartesianCoordinate();
        double z = randomCartesianCoordinate();
        return Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, x, y, z );
    }

    /**
     * Returns the next pseudorandom two-dimensional geographic {@link PointValue}.
     *
     * @return the next pseudorandom two-dimensional geographic {@link PointValue}.
     */
    public PointValue nextGeographicPoint()
    {
        double longitude = randomLongitude();
        double latitude = randomLatitude();
        return Values.pointValue( CoordinateReferenceSystem.WGS84, longitude, latitude );
    }

    /**
     * Returns the next pseudorandom three-dimensional geographic {@link PointValue}.
     *
     * @return the next pseudorandom three-dimensional geographic {@link PointValue}.
     */
    public PointValue nextGeographic3DPoint()
    {
        double longitude = randomLongitude();
        double latitude = randomLatitude();
        double z = randomCartesianCoordinate();
        return Values.pointValue( CoordinateReferenceSystem.WGS84_3D, longitude, latitude, z );
    }

    private double randomLatitude()
    {
        double spatialDefaultMinLatitude = -90;
        double spatialDefaultMaxLatitude = 90;
        return doubleBetween( spatialDefaultMinLatitude, spatialDefaultMaxLatitude );
    }

    private double randomLongitude()
    {
        double spatialDefaultMinLongitude = -180;
        double spatialDefaultMaxLongitude = 180;
        return doubleBetween( spatialDefaultMinLongitude, spatialDefaultMaxLongitude );
    }

    private double randomCartesianCoordinate()
    {
        double spatialDefaultMinExtent = -1000000;
        double spatialDefaultMaxExtent = 1000000;
        return doubleBetween( spatialDefaultMinExtent, spatialDefaultMaxExtent );
    }

    private double doubleBetween( double min, double max )
    {
        return generator.nextDouble() * (max - min) + min;
    }

    /**
     * Returns a randomly selected point value spread uniformly over the supported types of points.
     *
     * @return a randomly selected point value
     */
    public PointValue nextPointValue()
    {
        int nextInt = generator.nextInt( 4 );
        switch ( nextInt )
        {
        case 0:
            return nextCartesianPoint();

        case 1:
            return nextCartesian3DPoint();

        case 2:
            return nextGeographicPoint();

        case 3:
            return nextGeographic3DPoint();

        default:
            throw new IllegalStateException( nextInt + " not a valid point type" );
        }
    }

    /**
     * Returns a random element of the provided array
     *
     * @param among the array to choose a random element from
     * @return a random element of the provided list
     */
    public <T> T among( T[] among )
    {
        return among[generator.nextInt( among.length )];
    }

    /**
     * Returns a random element of the provided list
     *
     * @param among the list to choose a random element from
     * @return a random element of the provided list
     */
    public <T> T among( List<T> among )
    {
        return among.get( generator.nextInt( among.size() ) );
    }

    /**
     * Picks a random element of the provided list and feeds it to the provided {@link Consumer}
     *
     * @param among the list to pick from
     * @param action the consumer to feed values to
     */
    public <T> void among( List<T> among, Consumer<T> action )
    {
        if ( !among.isEmpty() )
        {
            T item = among( among );
            action.accept( item );
        }
    }

    /**
     * Returns a pseudorandom {@code int} between the given lower bound (inclusive) and the upper bound (inclusiv)
     *
     * @param min minimum value that can be chosen (inclusive)
     * @param max maximum value that can be chosen (inclusive)
     * @return a pseudorandom {@code int} in the given inclusive range.
     */
    public int intBetween( int min, int max )
    {
        return min + generator.nextInt( max - min + 1 );
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@code long} between 0 (inclusive) and the specified
     * value (exclusive)
     *
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return the next pseudorandom, uniformly distributed {@code long}
     * value between zero (inclusive) and {@code bound} (exclusive)
     */
    public long nextLong( long bound )
    {
        return abs( generator.nextLong() ) % bound;
    }

    /**
     * Returns the next pseudorandom uniformly distributed {@code long}.
     *
     * @return the next pseudorandom, uniformly distributed {@code long}
     */
    public long nextLong()
    {
        return generator.nextLong();
    }

    /**
     * Returns a random selection of the provided array.
     *
     * @param among the array to pick elements from
     * @param min the minimum number of elements to choose
     * @param max the maximum number of elements to choose
     * @param allowDuplicates if {@code true} the same element can be choosen multiple times
     * @return a random selection of the provided array.
     */
    @SuppressWarnings( "unchecked" )
    public <T> T[] selection( T[] among, int min, int max, boolean allowDuplicates )
    {
        assert min <= max;
        int diff = min == max ? 0 : generator.nextInt( max - min );
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
            if ( Objects.equals( item, contains ) )
            {
                return true;
            }
        }
        return false;
    }

    private Instant randomInstant()
    {
        return Instant.ofEpochSecond(
                nextLong( LocalDateTime.MIN.toEpochSecond( UTC ), LocalDateTime.MAX.toEpochSecond( UTC ) ),
                nextLong( NANOS_PER_SECOND ) );
    }

    private long nextLong( long origin, long bound )
    {
        return nextLong( (bound - origin) + 1L ) + origin;
    }

    private Types nextType()
    {
        return TYPES[generator.nextInt( TYPES.length )];
    }

    private static int nextPowerOf2( int i )
    {
        return 1 << (32 - Integer.numberOfLeadingZeros( i ));
    }
}
