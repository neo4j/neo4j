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

import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
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

public class RandomValue
{
    //TODO make possible to use SplittableRandom
    private final Random random;
    public static final long NANOS_PER_SECOND = 1_000_000_000L;


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

    public TextValue nextString( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        UTF8StringValueBuilder builder = new UTF8StringValueBuilder( nextPowerOf2( length ) );

        for ( int i = 0; i < length; i++ )
        {
            boolean validCodePoint = false;
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

    public TimeValue randomTime()
    {
        return time( OffsetTime.ofInstant( randomInstant(), UTC ) );
    }

    public LocalDateTimeValue randomLocalDateTime()
    {
        return localDateTime( ofInstant( randomInstant(), UTC ) );
    }

    public DateValue randomDate()
    {
        return date( ofEpochDay( nextLong( LocalDate.MIN.toEpochDay(), LocalDate.MAX.toEpochDay() ) ) );
    }

    public LocalTimeValue randomLocalTime()
    {
        return localTime( ofNanoOfDay( nextLong( LocalTime.MIN.toNanoOfDay(), LocalTime.MAX.toNanoOfDay() ) ) );
    }

    public DateTimeValue randomDateTime()
    {
        return datetime( ZonedDateTime.ofInstant( randomInstant(), UTC ) );
    }

    public DurationValue randomPeriod()
    {
        // Based on Java period (years, months and days)
        return duration( Period.of( random.nextInt(), random.nextInt( 12 ), random.nextInt( 28 ) ));
    }

    public DurationValue randomDuration()
    {
        // Based on java duration (seconds)
        return duration( Duration.of( nextLong( DAYS.getDuration().getSeconds() ), ChronoUnit.SECONDS ) );
    }

    public PointValue randomCartesianPoint()
    {
        return Values.pointValue( CoordinateReferenceSystem.Cartesian, random.nextDouble(), random.nextDouble() );
    }

    public PointValue randomCartesian3DPoint()
    {
        return Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, random.nextDouble(),
                random.nextDouble(), random.nextDouble() );
    }

    public PointValue randomWGS84Point()
    {
        double longitude = random.nextDouble() * 360.0 - 180.0;
        double latitude = random.nextDouble() * 180.0 - 90.0;
        return Values.pointValue( CoordinateReferenceSystem.WGS84, longitude, latitude );
    }

    public PointValue randomWGS843DPoint()
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
}
