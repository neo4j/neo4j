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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;

import org.neo4j.io.pagecache.PageCursor;

import static java.nio.charset.StandardCharsets.UTF_8;

class SimpleLongLayout extends Layout.Adapter<MutableLong,MutableLong>
{
    private String customNameAsMetaData;

    SimpleLongLayout( String customNameAsMetaData )
    {
        this.customNameAsMetaData = customNameAsMetaData;
    }

    SimpleLongLayout()
    {
        this( "test" );
    }

    @Override
    public int compare( MutableLong o1, MutableLong o2 )
    {
        return Long.compare( o1.longValue(), o2.longValue() );
    }

    @Override
    public MutableLong newKey()
    {
        return new MutableLong();
    }

    @Override
    public MutableLong copyKey( MutableLong key, MutableLong into )
    {
        into.setValue( key.longValue() );
        return into;
    }

    @Override
    public MutableLong newValue()
    {
        return new MutableLong();
    }

    @Override
    public int keySize()
    {
        return Long.BYTES;
    }

    @Override
    public int valueSize()
    {
        return Long.BYTES;
    }

    @Override
    public void writeKey( PageCursor cursor, MutableLong key )
    {
        cursor.putLong( key.longValue() );
    }

    @Override
    public void writeValue( PageCursor cursor, MutableLong value )
    {
        cursor.putLong( value.longValue() );
    }

    @Override
    public void readKey( PageCursor cursor, MutableLong into )
    {
        into.setValue( cursor.getLong() );
    }

    @Override
    public void readValue( PageCursor cursor, MutableLong into )
    {
        into.setValue( cursor.getLong() );
    }

    @Override
    public long identifier()
    {
        return 999;
    }

    @Override
    public int majorVersion()
    {
        return 0;
    }

    @Override
    public int minorVersion()
    {
        return 0;
    }

    @Override
    public void writeMetaData( PageCursor cursor )
    {
        writeString( cursor, customNameAsMetaData );
    }

    private static void writeString( PageCursor cursor, String string )
    {
        byte[] bytes = string.getBytes( UTF_8 );
        cursor.putInt( string.length() );
        cursor.putBytes( bytes );
    }

    @Override
    public void readMetaData( PageCursor cursor )
    {
        String name = readString( cursor );
        if ( name == null )
        {
            return;
        }

        if ( customNameAsMetaData != null )
        {
            if ( !name.equals( customNameAsMetaData ) )
            {
                cursor.setCursorException( "Name '" + name +
                        "' doesn't match expected '" + customNameAsMetaData + "'" );
                return;
            }
        }
        customNameAsMetaData = name;
    }

    private static String readString( PageCursor cursor )
    {
        int length = cursor.getInt();
        if ( length < 0 || length >= cursor.getCurrentPageSize() )
        {
            cursor.setCursorException( "Unexpected length of string " + length );
            return null;
        }

        byte[] bytes = new byte[length];
        cursor.getBytes( bytes );
        return new String( bytes, UTF_8 );
    }
}
