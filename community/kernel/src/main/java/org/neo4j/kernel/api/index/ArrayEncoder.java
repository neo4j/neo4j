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
package org.neo4j.kernel.api.index;

import java.lang.reflect.Array;
import java.util.Base64;

import org.neo4j.string.UTF8;
import org.neo4j.values.Value;
import org.neo4j.values.ValueWriter;
import org.neo4j.values.Values;

public final class ArrayEncoder
{
    private static final Base64.Encoder base64Encoder = Base64.getEncoder();

    private ArrayEncoder()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    @Deprecated
    public static String encode( Object array )
    {
        if ( !array.getClass().isArray() )
        {
            throw new IllegalArgumentException( "Only works with arrays" );
        }

        StringBuilder builder = new StringBuilder();
        int length = Array.getLength( array );
        String type = "";
        for ( int i = 0; i < length; i++ )
        {
            Object o = Array.get( array, i );
            if ( o instanceof Number )
            {
                type = "D";
                builder.append( ((Number) o).doubleValue() );
            }
            else if ( o instanceof Boolean )
            {
                type = "Z";
                builder.append( o );
            }
            else
            {
                type = "L";
                String str = o.toString();
                builder.append( base64Encoder.encodeToString( UTF8.encode( str ) ) );
            }
            builder.append( "|" );
        }
        return type + builder.toString();
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

    static class ValueEncoder implements ValueWriter
    {
        StringBuilder builder;

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
            builder.append( value );
            builder.append( '|' );
        }

        @Override
        public void writeString( char value )
        {
            builder.append( value );
            builder.append( '|' );
        }

        @Override
        public void writeString( char[] value, int offset, int length )
        {
            builder.append( value, offset, length );
            builder.append( '|' );
        }

        @Override
        public void beginUTF8( int size )
        {
            throw new UnsupportedOperationException( "direct UTF8 encoding is not supported yet!" );
        }

        @Override
        public void copyUTF8( long fromAddress, int length )
        {
            throw new UnsupportedOperationException( "direct UTF8 encoding is not supported yet!" );
        }

        @Override
        public void endUTF8()
        {
            throw new UnsupportedOperationException( "direct UTF8 encoding is not supported yet!" );
        }

        @Override
        public void beginArray( int size, ArrayType arrayType )
        {
            builder.append( typeChar( arrayType ) );
        }

        @Override
        public void endArray()
        {

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
            default: throw new UnsupportedOperationException( "Not supported array type: "+arrayType );
            }
        }
    }
}
