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
package org.neo4j.index.gbptree;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Provides static methods for getting and manipulating GSP (gen-safe pointer) data.
 * All interaction is made using a {@link PageCursor}. These methods are about a single GSP,
 * whereas the normal use case of a GSP is in pairs ({@link GenSafePointerPair GSPP}).
 * <p>
 * A GSP consists of [generation,pointer,checksum]. Checksum is calculated from generation and pointer.
 * <p>
 * Due to how java has one a single return type and objects produce/is garbage
 * the design of the methods below for reading GSP requires some documentation
 * to be used properly:
 * <p>
 * Caller is responsible for initially setting cursor offset at the start of
 * the GSP to read, then follows a couple of calls, each advancing the cursor
 * offset themselves:
 * <ol>
 * <li>{@link #readGeneration(PageCursor)}</li>
 * <li>{@link #readPointer(PageCursor)}</li>
 * <li>{@link #verifyChecksum(PageCursor, long, long)}</li>
 * </ol>
 */
class GenSafePointer
{
    public static final long MIN_GENERATION = 1L;
    // unsigned int
    public static final long MAX_GENERATION = 0xFFFFFFFFL;
    public static final long GENERATION_MASK = 0xFFFFFFFFL;
    public static final int UNSIGNED_SHORT_MASK = 0xFFFF;

    static final int CHECKSUM_SIZE = 2;
    static final int SIZE =
            4 +             // generation (unsigned int)
            6 +             // pointer (6B long)
            CHECKSUM_SIZE;  // checksum for generation & pointer

    /**
     * Writes GSP at the given {@code offset}, the two fields (generation, pointer) + a checksum will be written.
     *
     * @param cursor {@link PageCursor} to write into.
     * @param generation generation to write.
     * @param pointer pointer to write.
     */
    public static void write( PageCursor cursor, long generation, long pointer )
    {
        if ( generation < MIN_GENERATION || generation > MAX_GENERATION )
        {
            throw new IllegalArgumentException( "Can not write pointer with generation " + generation +
                    " because outside boundary for valid generation." );
        }
        cursor.putInt( (int) generation );
        put6BLong( cursor, pointer );
        cursor.putShort( checksumOf( generation, pointer ) );
    }

    public static long readGeneration( PageCursor cursor )
    {
        return cursor.getInt() & GENERATION_MASK;
    }

    public static long readPointer( PageCursor cursor )
    {
        long result = get6BLong( cursor );
        return result;
    }

    public static short readChecksum( PageCursor cursor )
    {
        return cursor.getShort();
    }

    public static boolean verifyChecksum( PageCursor cursor, long generation, long pointer )
    {
        short checksum = cursor.getShort();
        return checksum == checksumOf( generation, pointer );
    }

    private static long get6BLong( PageCursor cursor )
    {
        long lsb = cursor.getInt() & GENERATION_MASK;
        long msb = cursor.getShort() & UNSIGNED_SHORT_MASK;
        return lsb | (msb << Integer.SIZE);
    }

    private static void put6BLong( PageCursor cursor, long value )
    {
        int lsb = (int) value;
        short msb = (short) (value >>> Integer.SIZE);
        cursor.putInt( lsb );
        cursor.putShort( msb );
    }

    /**
     * Calculates a 2-byte checksum from GSP data.
     *
     * @param generation generation of the pointer.
     * @param pointer pointer itself.
     *
     * @return a {@code short} which is the checksum of the gen-pointer.
     */
    public static short checksumOf( long generation, long pointer )
    {
        short result = 0;
        result ^= ((short) generation) & UNSIGNED_SHORT_MASK;
        result ^= ((short) (generation >>> Short.SIZE)) & UNSIGNED_SHORT_MASK;
        result ^= ((short) pointer) & UNSIGNED_SHORT_MASK;
        result ^= ((short) (pointer >>> Short.SIZE)) & UNSIGNED_SHORT_MASK;
        result ^= ((short) (pointer >>> Integer.SIZE)) & UNSIGNED_SHORT_MASK;
        return result;
    }

    public static boolean isEmpty( long generation, long pointer )
    {
        return generation == 0 && pointer == 0;
    }
}
