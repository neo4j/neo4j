/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.PageCursorUtil.getUnsignedShort;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.putUnsignedShort;

/**
 * Gather utility methods for reading and writing individual dynamic sized
 * keys. It thus define the layout for:
 * - Key pointer in offset array (K*), 2B
 * - Key size, 2B
 * - Value size, 2B
 * - Key tombstone, first bit in key size
 *
 * Relative layout of key and key_value
 * KeyOffset points to the exact offset where key entry or key_value entry
 * can be read.
 * key entry - [keySize 2B|actualKey]
 * key_value entry - [keySize 2B|valueSize 2B|actualKey|actualValue]
 *
 * Tombstone
 * First bit in keySize is used as a tombstone, set to 1 if key is dead.
 * This leaves 15 bits for actual size -> max possible key size is 32768 bytes
 * which is more than page size and therefore be enough.
 */
class DynamicSizeUtil
{
    static final int SIZE_OFFSET = 2;
    static final int SIZE_KEY_SIZE = 2;
    static final int SIZE_VALUE_SIZE = 2;
    static final int SIZE_TOTAL_OVERHEAD = SIZE_OFFSET + SIZE_KEY_SIZE + SIZE_VALUE_SIZE;
    private static final int FLAG_TOMBSTONE = 0x8000;

    static void putKeyOffset( PageCursor cursor, int keyOffset )
    {
        putUnsignedShort( cursor, keyOffset );
    }

    static int readKeyOffset( PageCursor cursor )
    {
        return getUnsignedShort( cursor );
    }

    static void putKeySize( PageCursor cursor, int keySize )
    {
        putUnsignedShort( cursor, keySize );
    }

    static void putValueSize( PageCursor cursor, int valueSize )
    {
        putUnsignedShort( cursor, valueSize );
    }

    /**
     * Read key size including possible tombstone.
     * Check for tombstone with {@link #hasTombstone(int)}.
     * @param cursor On offset from where to read key size.
     * @return Key size including possible tombstone.
     */
    static int readKeySize( PageCursor cursor )
    {
        return getUnsignedShort( cursor );
    }

    static int readValueSize( PageCursor cursor )
    {
        return getUnsignedShort( cursor );
    }

    /**
     * Put a tombstone into key size.
     * @param cursor on offset to key size where tombstone should be put.
     */
    static void putTombstone( PageCursor cursor )
    {
        int offset = cursor.getOffset();
        int keySize = readKeySize( cursor );
        keySize = withTombstoneFlag( keySize );
        cursor.setOffset( offset );
        putKeySize( cursor, keySize );
    }

    /**
     * Check read key size for tombstone.
     * @return True if read key size has tombstone.
     */
    static boolean hasTombstone( int readKeySize )
    {
        return (readKeySize & FLAG_TOMBSTONE) != 0;
    }

    static int stripTombstone( int keySize )
    {
        return keySize & ~FLAG_TOMBSTONE;
    }

    private static int withTombstoneFlag( int keySize )
    {
        assert (keySize & FLAG_TOMBSTONE) == 0 : "Key size " + keySize + " is to large to fit tombstone.";
        return keySize | FLAG_TOMBSTONE;
    }
}
