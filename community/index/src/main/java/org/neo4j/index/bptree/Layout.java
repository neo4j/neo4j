/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.bptree;

import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

public interface Layout<KEY,VALUE> extends Comparator<KEY>
{
    KEY newKey();

    KEY minKey( KEY into );

    KEY maxKey( KEY into );

    void copyKey( KEY key, KEY into );

    VALUE newValue();

    int keySize();

    int valueSize();

    void writeKey( PageCursor cursor, KEY key );

    void writeValue( PageCursor cursor, VALUE value );

    void readKey( PageCursor cursor, KEY into );

    void readValue( PageCursor cursor, VALUE into );

    /**
     * Used as a checksum for when loading an index after creation, to verify that the same layout is used,
     * as the one it was initially created with.
     * @return a long acting as an identifier, written in the header of an index.
     */
    long identifier();

    int majorVersion();

    int minorVersion();

    void writeMetaData( PageCursor cursor );

    void readMetaData( PageCursor cursor );

    static long namedIdentifier( String name, int checksum )
    {
        char[] chars = name.toCharArray();
        if ( chars.length > 4 )
        {
            throw new IllegalArgumentException( "Maximum 4 character name, was '" + name + "'" );
        }
        long upperInt = 0;
        for ( int i = 0; i < chars.length; i++ )
        {
            byte byteValue = (byte) (((byte) chars[i]) ^ ((byte) (chars[i] >> 8)));
            upperInt <<= 8;
            upperInt |= byteValue;
        }

        return upperInt << Integer.SIZE | (checksum & 0xFFFFFFFF);
    }
}
