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
package org.neo4j.values.storable;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

/**
 * Writer of values.
 * <p>
 * Has functionality to write all supported primitives, as well as arrays and different representations of Strings.
 *
 * @param <E> type of {@link Exception} thrown from writer methods.
 */
public interface ValueWriter<E extends Exception>
{
    enum ArrayType
    {
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        STRING,
        CHAR,
        POINT,
        ZONED_DATE_TIME,
        LOCAL_DATE_TIME,
        DATE,
        ZONED_TIME,
        LOCAL_TIME,
        DURATION
    }

    void writeNull() throws E;

    void writeBoolean( boolean value ) throws E;

    void writeInteger( byte value ) throws E;

    void writeInteger( short value ) throws E;

    void writeInteger( int value ) throws E;

    void writeInteger( long value ) throws E;

    void writeFloatingPoint( float value ) throws E;

    void writeFloatingPoint( double value ) throws E;

    void writeString( String value ) throws E;

    void writeString( char value ) throws E;

    default void writeUTF8( byte[] bytes, int offset, int length ) throws E
    {
        writeString( new String( bytes, offset, length, StandardCharsets.UTF_8 ) );
    }

    void beginArray( int size, ArrayType arrayType ) throws E;

    void endArray() throws E;

    void writeByteArray( byte[] value ) throws E;

    void writePoint( CoordinateReferenceSystem crs, double[] coordinate ) throws E;

    void writeDuration( long months, long days, long seconds, int nanos ) throws E;

    void writeDate( LocalDate localDate ) throws E;

    void writeLocalTime( LocalTime localTime ) throws E;

    void writeTime( OffsetTime offsetTime ) throws E;

    void writeLocalDateTime( LocalDateTime localDateTime ) throws E;

    void writeDateTime( ZonedDateTime zonedDateTime ) throws E;

    class Adapter<E extends Exception> implements ValueWriter<E>
    {
        @Override
        public void writeNull() throws E
        {   // no-op
        }

        @Override
        public void writeBoolean( boolean value ) throws E
        {   // no-op
        }

        @Override
        public void writeInteger( byte value ) throws E
        {   // no-op
        }

        @Override
        public void writeInteger( short value ) throws E
        {   // no-op
        }

        @Override
        public void writeInteger( int value ) throws E
        {   // no-op
        }

        @Override
        public void writeInteger( long value ) throws E
        {   // no-op
        }

        @Override
        public void writeFloatingPoint( float value ) throws E
        {   // no-op
        }

        @Override
        public void writeFloatingPoint( double value ) throws E
        {   // no-op
        }

        @Override
        public void writeString( String value ) throws E
        {   // no-op
        }

        @Override
        public void writeString( char value ) throws E
        {   // no-op
        }

        @Override
        public void beginArray( int size, ArrayType arrayType ) throws E
        {   // no-op
        }

        @Override
        public void endArray() throws E
        {   // no-opa
        }

        @Override
        public void writeByteArray( byte[] value ) throws E
        {   // no-op
        }

        @Override
        public void writePoint( CoordinateReferenceSystem crs, double[] coordinate ) throws E
        {   // no-op
        }

        @Override
        public void writeDuration( long months, long days, long seconds, int nanos )
        {   // no-op
        }

        @Override
        public void writeDate( LocalDate localDate ) throws E
        {   // no-op
        }

        @Override
        public void writeLocalTime( LocalTime localTime ) throws E
        {   // no-op
        }

        @Override
        public void writeTime( OffsetTime offsetTime ) throws E
        {   // no-op
        }

        @Override
        public void writeLocalDateTime( LocalDateTime localDateTime ) throws E
        {   // no-op
        }

        @Override
        public void writeDateTime( ZonedDateTime zonedDateTime ) throws E
        {   // no-op
        }
    }
}
