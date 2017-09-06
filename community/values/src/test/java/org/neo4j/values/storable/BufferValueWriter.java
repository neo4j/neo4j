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
package org.neo4j.values.storable;

import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.BufferValueWriter.SpecialKind.BeginArray;
import static org.neo4j.values.storable.BufferValueWriter.SpecialKind.EndArray;
import static org.neo4j.values.storable.BufferValueWriter.SpecialKind.WriteByteArray;
import static org.neo4j.values.storable.BufferValueWriter.SpecialKind.WriteCharArray;

public class BufferValueWriter implements ValueWriter<RuntimeException>
{
    enum SpecialKind
    {
        WriteCharArray,
        WriteByteArray,
        BeginArray,
        EndArray,
    }

    public static class Special
    {
        final SpecialKind kind;
        final String key;

        @Override
        public boolean equals( Object o )
        {
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Special special = (Special) o;
            return kind == special.kind && key.equals( special.key );
        }

        @Override
        public int hashCode()
        {
            return 31 * kind.hashCode() + key.hashCode();
        }

        Special( SpecialKind kind, String key )
        {
            this.kind = kind;
            this.key = key;
        }

        Special( SpecialKind kind, int key )
        {
            this.kind = kind;
            this.key = Integer.toString( key );
        }

        @Override
        public String toString()
        {
            return format( "Special(%s)", key );
        }
    }

    protected List<Object> buffer = new ArrayList<>();

    @SuppressWarnings( "WeakerAccess" )
    public void assertBuffer( Object... writeEvents )
    {
        assertThat( buffer, Matchers.contains( writeEvents ) );
    }

    @Override
    public void writeNull()
    {
        buffer.add( null );
    }

    @Override
    public void writeBoolean( boolean value )
    {
        buffer.add( value );
    }

    @Override
    public void writeInteger( byte value )
    {
        buffer.add( value );
    }

    @Override
    public void writeInteger( short value )
    {
        buffer.add( value );
    }

    @Override
    public void writeInteger( int value )
    {
        buffer.add( value );
    }

    @Override
    public void writeInteger( long value )
    {
        buffer.add( value );
    }

    @Override
    public void writeFloatingPoint( float value )
    {
        buffer.add( value );
    }

    @Override
    public void writeFloatingPoint( double value )
    {
        buffer.add( value );
    }

    @Override
    public void writeString( String value )
    {
        buffer.add( value );
    }

    @Override
    public void writeString( char value )
    {
        buffer.add( value );
    }

    @Override
    public void writeString( char[] value, int offset, int length )
    {
        buffer.add( Specials.charArray( value, offset, length ) );
    }

    @Override
    public void beginArray( int size, ArrayType arrayType )
    {
        buffer.add( Specials.beginArray( size, arrayType ) );
    }

    @Override
    public void endArray()
    {
        buffer.add( Specials.endArray() );
    }

    @Override
    public void writeByteArray( byte[] value ) throws RuntimeException
    {
        buffer.add( Specials.byteArray( value ) );
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class Specials
    {
        public static Special charArray( char[] value, int offset, int length )
        {
            return new Special( WriteCharArray, format( "%d %d %d", Arrays.hashCode( value ), offset, length ) );
        }

        public static Special byteArray( byte[] value )
        {
            return new Special( WriteByteArray, Arrays.hashCode( value ) );
        }

        public static Special beginArray( int size, ArrayType arrayType )
        {
            return new Special( BeginArray, size + arrayType.toString() );
        }

        public static Special endArray()
        {
            return new Special( EndArray, 0 );
        }
    }
}
