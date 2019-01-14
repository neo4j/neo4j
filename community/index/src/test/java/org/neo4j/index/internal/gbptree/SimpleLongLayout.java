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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;

import org.neo4j.io.pagecache.PageCursor;

import static java.nio.charset.StandardCharsets.UTF_8;

class SimpleLongLayout extends TestLayout<MutableLong,MutableLong>
{
    private final int keyPadding;
    private String customNameAsMetaData;
    private final boolean fixedSize;
    private final int identifier;
    private final int majorVersion;
    private final int minorVersion;

    static class Builder
    {
        private int keyPadding;
        private int identifier = 999;
        private int majorVersion;
        private int minorVersion;
        private String customNameAsMetaData = "test";
        private boolean fixedSize = true;

        Builder withKeyPadding( int keyPadding )
        {
            this.keyPadding = keyPadding;
            return this;
        }

        Builder withIdentifier( int identifier )
        {
            this.identifier = identifier;
            return this;
        }

        Builder withMajorVersion( int majorVersion )
        {
            this.majorVersion = majorVersion;
            return this;
        }

        Builder withMinorVersion( int minorVersion )
        {
            this.minorVersion = minorVersion;
            return this;
        }

        Builder withCustomerNameAsMetaData( String customNameAsMetaData )
        {
            this.customNameAsMetaData = customNameAsMetaData;
            return this;
        }

        Builder withFixedSize( boolean fixedSize )
        {
            this.fixedSize = fixedSize;
            return this;
        }

        SimpleLongLayout build()
        {
            return new SimpleLongLayout( keyPadding, customNameAsMetaData, fixedSize, identifier, majorVersion, minorVersion );
        }
    }

    static Builder longLayout()
    {
        return new Builder();
    }

    SimpleLongLayout( int keyPadding, String customNameAsMetaData, boolean fixedSize, int identifier, int majorVersion, int minorVersion )
    {
        this.keyPadding = keyPadding;
        this.customNameAsMetaData = customNameAsMetaData;
        this.fixedSize = fixedSize;
        this.identifier = identifier;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    @Override
    public int compare( MutableLong o1, MutableLong o2 )
    {
        return Long.compare( o1.longValue(), o2.longValue() );
    }

    @Override
    int compareValue( MutableLong v1, MutableLong v2 )
    {
        return compare( v1, v2 );
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
    public int keySize( MutableLong key )
    {
        // pad the key here to affect the max key count, useful to get odd or even max key count
        return Long.BYTES + keyPadding;
    }

    @Override
    public int valueSize( MutableLong value )
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
    public void readKey( PageCursor cursor, MutableLong into, int keySize )
    {
        into.setValue( cursor.getLong() );
    }

    @Override
    public void readValue( PageCursor cursor, MutableLong into, int valueSize )
    {
        into.setValue( cursor.getLong() );
    }

    @Override
    public boolean fixedSize()
    {
        return fixedSize;
    }

    @Override
    public long identifier()
    {
        return identifier;
    }

    @Override
    public int majorVersion()
    {
        return majorVersion;
    }

    @Override
    public int minorVersion()
    {
        return minorVersion;
    }

    @Override
    public void writeMetaData( PageCursor cursor )
    {
        writeString( cursor, customNameAsMetaData );
        cursor.putInt( keyPadding );
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

        int readKeyPadding = cursor.getInt();
        if ( readKeyPadding != keyPadding )
        {
            cursor.setCursorException( "Key padding " + readKeyPadding + " doesn't match expected " + keyPadding );
        }
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

    @Override
    public MutableLong key( long seed )
    {
        MutableLong key = newKey();
        key.setValue( seed );
        return key;
    }

    @Override
    public MutableLong value( long seed )
    {
        MutableLong value = newValue();
        value.setValue( seed );
        return value;
    }

    @Override
    public long keySeed( MutableLong key )
    {
        return key.getValue();
    }

    @Override
    public long valueSeed( MutableLong value )
    {
        return value.getValue();
    }
}
