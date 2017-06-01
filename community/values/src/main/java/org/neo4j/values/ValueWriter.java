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
 *
 * Has functionality to write all supported primitives, as well as arrays and different representations of Strings.
 */
public interface ValueWriter
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

    void writeNull();

    void writeBoolean( boolean value );

    void writeInteger( byte value );

    void writeInteger( short value );

    void writeInteger( int value );

    void writeInteger( long value );

    void writeFloatingPoint( float value );

    void writeFloatingPoint( double value );

    void writeString( String value );

    void writeString( char value );

    default void writeString( char[] value )
    {
        writeString( value, 0, value.length );
    }

    void writeString( char[] value, int offset, int length );

    void beginUTF8( int size );

    void copyUTF8( long fromAddress, int length );

    void endUTF8();

    void beginArray( int size, ArrayType arrayType );

    void endArray();
}
