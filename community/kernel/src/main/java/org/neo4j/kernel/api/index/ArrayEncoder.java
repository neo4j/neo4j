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
package org.neo4j.kernel.api.index;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Base64;

import org.neo4j.string.UTF8;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

public final class ArrayEncoder
{
    private static final Base64.Encoder base64Encoder = Base64.getEncoder();

    private ArrayEncoder()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    public static String encode( Value array )
    {
        if ( !Values.isArrayValue( array ) )
        {
            throw new IllegalArgumentException( "Only works with arrays" );
        }

        ValueEncoder encoder = new ValueEncoder();
        array.writeTo( encoder );
        return encoder.result();
    }

    static class ValueEncoder implements ValueWriter<RuntimeException>
    {
        private StringBuilder builder;

        ValueEncoder()
        {
            builder = new StringBuilder();
        }

        public String result()
        {
            return builder.toString();
        }

        @Override
        public void writeNull()
        {
        }

        @Override
        public void writeBoolean( boolean value )
        {
            builder.append( value );
            builder.append( '|' );
        }

        @Override
        public void writeInteger( byte value )
        {
            builder.append( (double)value );
            builder.append( '|' );
        }

        @Override
        public void writeInteger( short value )
        {
            builder.append( (double)value );
            builder.append( '|' );
        }

        @Override
        public void writeInteger( int value )
        {
            builder.append( (double)value );
            builder.append( '|' );
        }

        @Override
        public void writeInteger( long value )
        {
            builder.append( (double)value );
            builder.append( '|' );
        }

        @Override
        public void writeFloatingPoint( float value )
        {
            builder.append( (double)value );
            builder.append( '|' );
        }

        @Override
        public void writeFloatingPoint( double value )
        {
            builder.append( value );
            builder.append( '|' );
        }

        @Override
        public void writeString( String value )
        {
            builder.append( base64Encoder.encodeToString( UTF8.encode( value ) ) );
            builder.append( '|' );
        }

        @Override
        public void writeString( char value )
        {
            builder.append( base64Encoder.encodeToString( UTF8.encode( Character.toString( value ) ) ) );
            builder.append( '|' );
        }

        @Override
        public void writePoint( CoordinateReferenceSystem crs, double[] coordinate ) throws RuntimeException
        {
            builder.append( crs.getTable().getTableId() );
            builder.append( ':' );
            builder.append( crs.getCode() );
            builder.append( ':' );
            int index = 0;
            for ( double c : coordinate )
            {
                if ( index > 0 )
                {
                    builder.append( ';' );
                }
                builder.append( c );
                index++;
            }
            builder.append( '|' );
        }

        @Override
        public void writeDuration( long months, long days, long seconds, int nanos ) throws RuntimeException
        {
            builder.append( DurationValue.duration( months, days, seconds, nanos ).prettyPrint() );
            builder.append( '|' );
        }

        @Override
        public void writeDate( LocalDate localDate ) throws RuntimeException
        {
            builder.append( DateValue.date( localDate ).prettyPrint() );
            builder.append( '|' );
        }

        @Override
        public void writeLocalTime( LocalTime localTime ) throws RuntimeException
        {
            builder.append( LocalTimeValue.localTime( localTime ).prettyPrint() );
            builder.append( '|' );
        }

        @Override
        public void writeTime( OffsetTime offsetTime ) throws RuntimeException
        {
            builder.append( TimeValue.time( offsetTime ).prettyPrint() );
            builder.append( '|' );
        }

        @Override
        public void writeLocalDateTime( LocalDateTime localDateTime ) throws RuntimeException
        {
            builder.append( LocalDateTimeValue.localDateTime( localDateTime ).prettyPrint() );
            builder.append( '|' );
        }

        @Override
        public void writeDateTime( ZonedDateTime zonedDateTime ) throws RuntimeException
        {
            builder.append( DateTimeValue.datetime( zonedDateTime ).prettyPrint() );
            builder.append( '|' );
        }

        @Override
        public void beginArray( int size, ArrayType arrayType )
        {
            if ( size > 0 )
            {
                builder.append( typeChar( arrayType ) );
            }
        }

        @Override
        public void endArray()
        {
        }

        @Override
        public void writeByteArray( byte[] value )
        {
            builder.append( 'D' );
            for ( byte b : value )
            {
                builder.append( (double)b );
                builder.append( '|' );
            }
        }

        private char typeChar( ArrayType arrayType )
        {
            switch ( arrayType )
            {
            case BOOLEAN: return 'Z';
            case BYTE: return 'D';
            case SHORT: return 'D';
            case INT: return 'D';
            case LONG: return 'D';
            case FLOAT: return 'D';
            case DOUBLE: return 'D';
            case CHAR: return 'L';
            case STRING: return 'L';
            case POINT: return 'P';
            case ZONED_DATE_TIME: return 'T';
            case LOCAL_DATE_TIME: return 'T';
            case DATE: return 'T';
            case ZONED_TIME: return 'T';
            case LOCAL_TIME: return 'T';
            case DURATION: return 'A';
            default: throw new UnsupportedOperationException( "Not supported array type: " + arrayType );
            }
        }
    }
}
