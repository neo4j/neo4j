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

    private static final int BOOLEAN = 0;
    private static final int BYTE = 1;
    private static final int SHORT = 2;
    private static final int STRING = 3;
    private static final int INT = 4;
    private static final int LONG = 5;
    private static final int FLOAT = 6;
    private static final int DOUBLE = 7;
    private static final int LOCAL_DATE_TIME = 8;
    private static final int DATE = 9;
    private static final int LOCAL_TIME = 10;
    private static final int PERIOD = 11;
    private static final int DURATION = 12;
    private static final int TIME = 13;
    private static final int DATE_TIME = 14;
    private static final int CARTESIAN_POINT = 15;
    private static final int CARTESIAN_POINT_3D = 16;
    private static final int GEOGRAPHIC_POINT = 17;
    private static final int GEOGRAPHIC_POINT_3D = 18;
    private static final int NUMBER_OF_TYPES = 19;

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

    public TextValue nextDigitString()
    {
        return nextDigitString( configuration.stringMinLength(), configuration.stringMaxLength() );
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

    public TextValue nextAlphaString()
    {
        return nextAlphaString( configuration.stringMinLength(), configuration.stringMaxLength() );
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

    public TextValue nextAlphaNumericString()
    {
        return nextAlphaNumericString( configuration.stringMinLength(), configuration.stringMaxLength() );
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

    public TextValue nextAsciiString()
    {
        return nextAsciiString( configuration.stringMinLength(), configuration.stringMaxLength() );
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

    public TextValue nextPrintableAsciiString()
    {
        return nextPrintableAsciiString( configuration.stringMinLength(), configuration.stringMaxLength() );
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

    public TextValue nextString()
    {
        return nextString( configuration.stringMinLength(), configuration.stringMaxLength() );
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

    public ArrayValue nextArray()
    {
        return nextArray( configuration.arrayMinLength(), configuration.arrayMaxLength() );
    }

    public ArrayValue nextArray( int minLength, int maxLength )
    {
        int type = random.nextInt( NUMBER_OF_TYPES );
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

        default:
            throw new IllegalArgumentException( "Unknown value type: " + type );
        }
    }

    public ArrayValue nextCartesianPointArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        PointValue[] array = new PointValue[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = nextCartesianPoint();
        }
        return Values.pointArray( array );
    }

    public ArrayValue nextCartesian3DPointArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        PointValue[] array = new PointValue[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = nextCartesian3DPoint();
        }
        return Values.pointArray( array );
    }

    public ArrayValue nextGeographicPointArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        PointValue[] array = new PointValue[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = nextGeographicPoint();
        }
        return Values.pointArray( array );
    }

    public ArrayValue nextGeographic3DPointArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        PointValue[] array = new PointValue[length];
        for ( int i = 0; i < length; i++ )
        {
            array[i] = nextGeographic3DPoint();
        }
        return Values.pointArray( array );
    }

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

    public ArrayValue nextDoubleArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        double[] doubles = new double[length];
        for ( int i = 0; i < length; i++ )
        {
            doubles[i] = random.nextDouble();
        }
        return Values.doubleArray( doubles );
    }

    public ArrayValue nextFloatArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        float[] floats = new float[length];
        for ( int i = 0; i < length; i++ )
        {
            floats[i] = random.nextFloat();
        }
        return Values.floatArray( floats );
    }

    public ArrayValue nextLongArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        long[] longs = new long[length];
        for ( int i = 0; i < length; i++ )
        {
            longs[i] = random.nextLong();
        }
        return Values.longArray( longs );
    }

    public ArrayValue nextIntArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        int[] ints = new int[length];
        for ( int i = 0; i < length; i++ )
        {
            ints[i] = random.nextInt();
        }
        return Values.intArray( ints );
    }

    public ArrayValue nextBooleanArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        boolean[] booleans = new boolean[length];
        for ( int i = 0; i < length; i++ )
        {
            booleans[i] = random.nextBoolean();
        }
        return Values.booleanArray( booleans );
    }

    public ArrayValue nextByteArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            bytes[i] = (byte) random.nextInt();
        }
        return Values.byteArray( bytes );
    }

    public ArrayValue nextShortArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        short[] shorts = new short[length];
        for ( int i = 0; i < length; i++ )
        {
            shorts[i] = (short) random.nextInt();
        }
        return Values.shortArray( shorts );
    }

    public ArrayValue nextStringArray( int minLength, int maxLength )
    {
        int length = intBetween( minLength, maxLength );
        String[] strings = new String[length];
        for ( int i = 0; i < length; i++ )
        {
            strings[i] = nextString().stringValue();
        }
        return Values.stringArray( strings );
    }


    public TimeValue nextTimeValue()
    {
        return time( OffsetTime.ofInstant( randomInstant(), UTC ) );
    }

    public LocalDateTimeValue nextLocalDateTimeValue()
    {
        return localDateTime( ofInstant( randomInstant(), UTC ) );
    }

    public DateValue nextDateValue()
    {
        return date( ofEpochDay( nextLong( LocalDate.MIN.toEpochDay(), LocalDate.MAX.toEpochDay() ) ) );
    }

    public LocalTimeValue nextLocalTimeValue()
    {
        return localTime( ofNanoOfDay( nextLong( LocalTime.MIN.toNanoOfDay(), LocalTime.MAX.toNanoOfDay() ) ) );
    }

    public DateTimeValue nextDateTimeValue()
    {
        return datetime( ZonedDateTime.ofInstant( randomInstant(), UTC ) );
    }

    public DurationValue nextPeriod()
    {
        // Based on Java period (years, months and days)
        return duration( Period.of( random.nextInt(), random.nextInt( 12 ), random.nextInt( 28 ) ) );
    }

    public DurationValue nextDuration()
    {
        // Based on java duration (seconds)
        return duration( Duration.of( nextLong( DAYS.getDuration().getSeconds() ), ChronoUnit.SECONDS ) );
    }

    public PointValue nextCartesianPoint()
    {
        return Values.pointValue( CoordinateReferenceSystem.Cartesian, random.nextDouble(), random.nextDouble() );
    }

    public PointValue nextCartesian3DPoint()
    {
        return Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, random.nextDouble(),
                random.nextDouble(), random.nextDouble() );
    }

    public PointValue nextGeographicPoint()
    {
        double longitude = random.nextDouble() * 360.0 - 180.0;
        double latitude = random.nextDouble() * 180.0 - 90.0;
        return Values.pointValue( CoordinateReferenceSystem.WGS84, longitude, latitude );
    }

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
}
