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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Values;
import org.neo4j.values.utils.TemporalUtil;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Utility for reading and writing property values from/into a channel. Supports neo4j property types,
 * including arrays.
 */
public abstract class ValueType
{
    private static final Map<Class<?>,ValueType> byClass = new HashMap<>();
    private static final Map<Byte,ValueType> byId = new HashMap<>();
    private static ValueType stringType;
    private static byte next;
    static
    {
        add( new ValueType( Boolean.TYPE, Boolean.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return from.get() == 0 ? Boolean.FALSE : Boolean.TRUE;
            }

            @Override
            public int length( Object value )
            {
                return Byte.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                into.put( (Boolean)value ? (byte)1 : (byte)0 );
            }
        } );
        add( new ValueType( Byte.TYPE, Byte.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return from.get();
            }

            @Override
            public int length( Object value )
            {
                return Byte.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                into.put( (Byte)value );
            }
        } );
        add( new ValueType( Short.TYPE, Short.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return from.getShort();
            }

            @Override
            public int length( Object value )
            {
                return Short.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                into.putShort( (Short)value );
            }
        } );
        add( new ValueType( Character.TYPE, Character.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return (char)from.getInt();
            }

            @Override
            public int length( Object value )
            {
                return Character.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                into.putInt( (Character)value );
            }
        } );
        add( new ValueType( Integer.TYPE, Integer.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return from.getInt();
            }

            @Override
            public int length( Object value )
            {
                return Integer.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                into.putInt( (Integer) value );
            }
        } );
        add( new ValueType( Long.TYPE, Long.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return from.getLong();
            }

            @Override
            public int length( Object value )
            {
                return Long.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                into.putLong( (Long)value );
            }
        } );
        add( new ValueType( Float.TYPE, Float.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return from.getFloat();
            }

            @Override
            public int length( Object value )
            {
                return Float.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                into.putFloat( (Float)value );
            }
        } );
        add( stringType = new ValueType( String.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                int length = from.getInt();
                byte[] bytes = new byte[length]; // TODO wasteful
                from.get( bytes, length );
                return UTF8.decode( bytes );
            }

            @Override
            public int length( Object value )
            {
                return Integer.BYTES + ((String)value).length() * Character.BYTES; // pessimistic
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                byte[] bytes = UTF8.encode( (String)value );
                into.putInt( bytes.length ).put( bytes, bytes.length );
            }
        } );
        add( new ValueType( Double.class, Double.TYPE )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return from.getDouble();
            }

            @Override
            public int length( Object value )
            {
                return Double.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                into.putDouble( (Double)value );
            }
        } );
        add( new ValueType( LocalDate.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return LocalDate.ofEpochDay( from.getLong() );
            }

            @Override
            public int length( Object value )
            {
                return Long.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                into.putLong( ((LocalDate) value).toEpochDay() );
            }
        } );
        add( new ValueType( LocalTime.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return LocalTime.ofNanoOfDay( from.getLong() );
            }

            @Override
            public int length( Object value )
            {
                return Long.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                into.putLong( ((LocalTime) value).toNanoOfDay() );
            }
        } );
        add( new ValueType( LocalDateTime.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return LocalDateTime.ofEpochSecond( from.getLong(), from.getInt(), UTC );
            }

            @Override
            public int length( Object value )
            {
                return Long.BYTES + Integer.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                LocalDateTime ldt = (LocalDateTime) value;
                into.putLong( ldt.toEpochSecond( UTC) );
                into.putInt( ldt.getNano() );
            }
        } );
        add( new ValueType( OffsetTime.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                return OffsetTime.ofInstant( Instant.ofEpochSecond( 0, from.getLong() ), ZoneOffset.ofTotalSeconds( from.getInt() ) );
            }

            @Override
            public int length( Object value )
            {
                return Long.BYTES + Integer.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                OffsetTime ot = (OffsetTime) value;
                into.putLong( TemporalUtil.getNanosOfDayUTC( ot ) );
                into.putInt( ot.getOffset().getTotalSeconds() );
            }
        } );
        add( new ValueType( ZonedDateTime.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                if ( from.get() == (byte) 0 )
                {
                    long epochSecondsUTC = from.getLong();
                    int nanos = from.getInt();
                    int offsetSeconds = from.getInt();
                    return ZonedDateTime.ofInstant( Instant.ofEpochSecond( epochSecondsUTC, nanos ), ZoneOffset.ofTotalSeconds( offsetSeconds ) );
                }
                else
                {
                    long epochSecondsUTC = from.getLong();
                    int nanos = from.getInt();
                    int zoneID = from.getInt();
                    String zone = TimeZones.map( (short) zoneID );
                    return ZonedDateTime.ofInstant( Instant.ofEpochSecond( epochSecondsUTC, nanos ), ZoneId.of( zone ) );
                }
            }

            @Override
            public int length( Object value )
            {
                return 1 + Long.BYTES + Integer.BYTES + Integer.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                ZonedDateTime zonedDateTime = (ZonedDateTime) value;
                long epochSecondUTC = zonedDateTime.toEpochSecond();
                int nano = zonedDateTime.getNano();

                ZoneId zone = zonedDateTime.getZone();
                if ( zone instanceof ZoneOffset )
                {
                    int offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();
                    into.put( (byte) 0 );
                    into.putLong( epochSecondUTC );
                    into.putInt( nano );
                    into.putInt( offsetSeconds );
                }
                else
                {
                    String zoneId = zone.getId();
                    into.put( (byte) 1 );
                    into.putLong( epochSecondUTC );
                    into.putInt( nano );
                    into.putInt( TimeZones.map( zoneId ) );
                }
            }
        } );
        add( new ValueType( DurationValue.class, Duration.class, Period.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                int nanos = from.getInt();
                long seconds = from.getLong();
                long days = from.getLong();
                long months = from.getLong();
                return DurationValue.duration( months, days, seconds, nanos );
            }

            @Override
            public int length( Object value )
            {
                return Integer.BYTES + 3 * Long.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                DurationValue duration;
                if ( value instanceof Duration )
                {
                    duration = DurationValue.duration( (Duration) value );
                }
                else if ( value instanceof Period )
                {
                    duration = DurationValue.duration( (Period) value );
                }
                else
                {
                    duration = (DurationValue) value;
                }
                into.putInt( (int) duration.get( NANOS ) );
                into.putLong( duration.get( SECONDS ) );
                into.putLong( duration.get( DAYS ) );
                into.putLong( duration.get( MONTHS ) );
            }
        } );
        add( new ValueType( PointValue.class )
        {
            @Override
            public Object read( ReadableClosableChannel from ) throws IOException
            {
                int code = from.getInt();
                CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( code );
                int length = from.getInt();
                double[] coordinate = new double[length];
                for ( int i = 0; i < length; i++ )
                {
                    coordinate[i] = from.getDouble();
                }
                return Values.pointValue( crs, coordinate );
            }

            @Override
            public int length( Object value )
            {
                return 2 * Integer.BYTES + ((PointValue) value).coordinate().length * Double.BYTES;
            }

            @Override
            public void write( Object value, FlushableChannel into ) throws IOException
            {
                PointValue pointValue = (PointValue) value;
                // using code is not a future-proof solution like the one we have in the PropertyStore.
                // But then again, this is not used from procution code.
                into.putInt( pointValue.getCoordinateReferenceSystem().getCode() );
                double[] coordinate = pointValue.coordinate();
                into.putInt( coordinate.length );
                for ( double c : coordinate )
                {
                    into.putDouble( c );
                }
            }
        } );
    }
    private static final ValueType arrayType = new ValueType()
    {
        @Override
        public Object read( ReadableClosableChannel from ) throws IOException
        {
            ValueType componentType = typeOf( from.get() );
            int length = from.getInt();
            Object value = Array.newInstance( componentType.componentClass(), length );
            for ( int i = 0; i < length; i++ )
            {
                Array.set( value, i, componentType.read( from ) );
            }
            return value;
        }

        @Override
        public int length( Object value )
        {
            ValueType componentType = typeOf( value.getClass().getComponentType() );
            int arrayLlength = Array.getLength( value );
            int length = Integer.BYTES; // array length
            for ( int i = 0; i < arrayLlength; i++ )
            {
                length += componentType.length( Array.get( value, i ) );
            }
            return length;
        }

        @Override
        public void write( Object value, FlushableChannel into ) throws IOException
        {
            ValueType componentType = typeOf( value.getClass().getComponentType() );
            into.put( componentType.id() );
            int length = Array.getLength( value );
            into.putInt( length );
            for ( int i = 0; i < length; i++ )
            {
                componentType.write( Array.get( value, i ), into );
            }
        }
    };

    private final Class<?>[] classes;
    private final byte id = next++;

    private ValueType( Class<?>... classes )
    {
        this.classes = classes;
    }

    private static void add( ValueType type )
    {
        for ( Class<?> cls : type.classes )
        {
            byClass.put( cls, type );
        }
        byId.put( type.id(), type );
    }

    public static ValueType typeOf( Object value )
    {
        return typeOf( value.getClass() );
    }

    public static ValueType typeOf( Class<?> cls )
    {
        if ( cls.isArray() )
        {
            return arrayType;
        }

        ValueType type = byClass.get( cls );
        assert type != null : "Unrecognized value type " + cls;
        return type;
    }

    public static ValueType typeOf( byte id )
    {
        if ( id == arrayType.id() )
        {
            return arrayType;
        }

        ValueType type = byId.get( id );
        assert type != null : "Unrecognized value type id " + id;
        return type;
    }

    public static ValueType stringType()
    {
        return stringType;
    }

    private Class<?> componentClass()
    {
        return classes[0];
    }

    public final byte id()
    {
        return id;
    }

    public abstract Object read( ReadableClosableChannel from ) throws IOException;

    public abstract int length( Object value );

    public abstract void write( Object value, FlushableChannel into ) throws IOException;
}
