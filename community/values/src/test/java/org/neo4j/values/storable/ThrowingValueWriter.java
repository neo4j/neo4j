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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.function.Supplier;

public abstract class ThrowingValueWriter<E extends Exception> implements ValueWriter<E>
{
    protected abstract E exception( String method );

    static <E extends Exception> ValueWriter<E> throwing( Supplier<E> exception )
    {
        return new ThrowingValueWriter<E>()
        {
            @Override
            protected E exception( String method )
            {
                return exception.get();
            }
        };
    }

    public abstract static class AssertOnly extends ThrowingValueWriter<RuntimeException>
    {
        @Override
        protected RuntimeException exception( String method )
        {
            throw new AssertionError( method );
        }
    }

    @Override
    public void writeNull() throws E
    {
        throw exception( "writeNull" );
    }

    @Override
    public void writeBoolean( boolean value ) throws E
    {
        throw exception( "writeBoolean" );
    }

    @Override
    public void writeInteger( byte value ) throws E
    {
        throw exception( "writeInteger" );
    }

    @Override
    public void writeInteger( short value ) throws E
    {
        throw exception( "writeInteger" );
    }

    @Override
    public void writeInteger( int value ) throws E
    {
        throw exception( "writeInteger" );
    }

    @Override
    public void writeInteger( long value ) throws E
    {
        throw exception( "writeInteger" );
    }

    @Override
    public void writeFloatingPoint( float value ) throws E
    {
        throw exception( "writeFloatingPoint" );
    }

    @Override
    public void writeFloatingPoint( double value ) throws E
    {
        throw exception( "writeFloatingPoint" );
    }

    @Override
    public void writeString( String value ) throws E
    {
        throw exception( "writeString" );
    }

    @Override
    public void writeString( char value ) throws E
    {
        throw exception( "writeString" );
    }

    @Override
    public void beginArray( int size, ArrayType arrayType ) throws E
    {
        throw exception( "beginArray" );
    }

    @Override
    public void endArray() throws E
    {
        throw exception( "endArray" );
    }

    @Override
    public void writeByteArray( byte[] value ) throws E
    {
        throw exception( "writeByteArray" );
    }

    @Override
    public void writePoint( CoordinateReferenceSystem crs, double[] coordinate ) throws E
    {
        throw exception( "writePoint" );
    }

    @Override
    public void writeDuration( long months, long days, long seconds, int nanos ) throws E
    {
        throw exception( "writeDuration" );
    }

    @Override
    public void writeDate( LocalDate localDate ) throws E
    {
        throw exception( "writeDate" );
    }

    @Override
    public void writeLocalTime( LocalTime localTime ) throws E
    {
        throw exception( "writeLocalTime" );
    }

    @Override
    public void writeTime( OffsetTime offsetTime ) throws E
    {
        throw exception( "writeTime" );
    }

    @Override
    public void writeLocalDateTime( LocalDateTime localDateTime ) throws E
    {
        throw exception( "writeLocalDateTime" );
    }

    @Override
    public void writeDateTime( ZonedDateTime zonedDateTime ) throws E
    {
        throw exception( "writeDateTime" );
    }
}
