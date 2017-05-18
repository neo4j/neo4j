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
package org.neo4j.impl.kernel.api;

import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.impl.kernel.api.result.ValueWriter;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.impl.kernel.api.BufferValueWriter.SpecialKind.BeginList;
import static org.neo4j.impl.kernel.api.BufferValueWriter.SpecialKind.BeginMap;
import static org.neo4j.impl.kernel.api.BufferValueWriter.SpecialKind.BeginUTF8;
import static org.neo4j.impl.kernel.api.BufferValueWriter.SpecialKind.CopyUTF8;
import static org.neo4j.impl.kernel.api.BufferValueWriter.SpecialKind.EndList;
import static org.neo4j.impl.kernel.api.BufferValueWriter.SpecialKind.EndMap;
import static org.neo4j.impl.kernel.api.BufferValueWriter.SpecialKind.EndUTF8;
import static org.neo4j.impl.kernel.api.BufferValueWriter.SpecialKind.Key;
import static org.neo4j.impl.kernel.api.BufferValueWriter.SpecialKind.WriteCharArray;

public class BufferValueWriter implements ValueWriter
{
    enum SpecialKind
    {
        PropKey,
        WriteCharArray,
        BeginUTF8,
        CopyUTF8,
        EndUTF8,
        BeginList,
        EndList,
        BeginMap,
        Key,
        EndMap
    }

    public static class Special
    {
        public final SpecialKind kind;
        public final String key;

        public Special( SpecialKind kind, String key )
        {
            this.kind = kind;
            this.key = key;
        }

        public Special( SpecialKind kind, int key )
        {
            this.kind = kind;
            this.key = Integer.toString( key );
        }
    }

    List<Object> buffer = new ArrayList<>();

    public void assertBuffer( Object... writeEvents )
    {
        assertThat( buffer, Matchers.contains( writeEvents ) );
    }

    @Override
    public void writePropertyKey( int key )
    {
        buffer.add( Specials.propertyKey( key ) );
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
    public void writeInteger( long value )
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
    public void beginUTF8( int size )
    {
        buffer.add( Specials.beginUTF8( size ) );
    }

    @Override
    public void copyUTF8( long fromAddress, int length )
    {
        buffer.add( Specials.copyUTF8( fromAddress, length ) );
    }

    @Override
    public void endUTF8()
    {
        buffer.add( Specials.endUTF8() );
    }

    @Override
    public void beginList( int size )
    {
        buffer.add( Specials.beginList() );
    }

    @Override
    public void endList()
    {
        buffer.add( Specials.endList() );
    }

    @Override
    public void beginMap( int size )
    {
        buffer.add( Specials.beginMap() );

    }

    @Override
    public void writeKey( String key )
    {
        buffer.add( Specials.writeKey( key ) );
    }

    @Override
    public void endMap()
    {
        buffer.add( Specials.endMap() );
    }

    public static class Specials
    {
        public static Special propertyKey( int key )
        {
            return new Special( SpecialKind.PropKey, key );
        }

        public static Special charArray( char[] value, int offset, int length )
        {
            return new Special( WriteCharArray,
                    format( "%d %d %d", Arrays.hashCode( value ), offset, length )
            );
        }

        public static Special beginUTF8( int size )
        {
            return new Special( BeginUTF8, size );
        }

        public static Special copyUTF8( long fromAddress, int length )
        {
            return new Special( CopyUTF8, format( "%d %d", fromAddress, length ) );
        }

        public static Special endUTF8()
        {
            return new Special( EndUTF8, 0 );
        }

        public static Special beginList()
        {
            return new Special( BeginList, 0 );
        }

        public static Special endList()
        {
            return new Special( EndList, 0 );
        }

        public static Special beginMap()
        {
            return new Special( BeginMap, 0 );
        }

        public static Special writeKey( String key )
        {
            return new Special( Key, key );
        }

        public static Special endMap()
        {
            return new Special( EndMap, 0 );
        }
    }
}
