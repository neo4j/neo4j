/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

/**
 * Writer of values.
 * <p>
 * Has functionality to write all supported primitives, as well as arrays and different representations of Strings.
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
        CHAR
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

    default void writeString( char[] value ) throws E
    {
        writeString( value, 0, value.length );
    }

    void writeString( char[] value, int offset, int length ) throws E;

    void beginUTF8( int size ) throws E;

    void copyUTF8( long fromAddress, int length ) throws E;

    void endUTF8() throws E;

    void beginArray( int size, ArrayType arrayType ) throws E;

    void endArray() throws E;

    void writeByteArray( byte[] value ) throws E;

    class Adapter implements ValueWriter
    {
        @Override
        public void writeNull()
        {   // no-op
        }

        @Override
        public void writeBoolean( boolean value )
        {   // no-op
        }

        @Override
        public void writeInteger( byte value )
        {   // no-op
        }

        @Override
        public void writeInteger( short value )
        {   // no-op
        }

        @Override
        public void writeInteger( int value )
        {   // no-op
        }

        @Override
        public void writeInteger( long value )
        {   // no-op
        }

        @Override
        public void writeFloatingPoint( float value )
        {   // no-op
        }

        @Override
        public void writeFloatingPoint( double value )
        {   // no-op
        }

        @Override
        public void writeString( String value )
        {   // no-op
        }

        @Override
        public void writeString( char value )
        {   // no-op
        }

        @Override
        public void writeString( char[] value, int offset, int length )
        {   // no-op
        }

        @Override
        public void beginUTF8( int size )
        {   // no-op
        }

        @Override
        public void copyUTF8( long fromAddress, int length )
        {   // no-op
        }

        @Override
        public void endUTF8()
        {   // no-op
        }

        @Override
        public void beginArray( int size, ArrayType arrayType )
        {   // no-op
        }

        @Override
        public void endArray() throws Exception
        {   // no-opa
        }

        @Override
        public void writeByteArray( byte[] value ) throws Exception
        {   // no-op
        }
    }
}
