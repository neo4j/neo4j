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

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.PageCursorUtil.get6BLong;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.getUnsignedInt;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.put6BLong;

/**
 * Provides static methods for getting and manipulating GSP (generation-safe pointer) data.
 * All interaction is made using a {@link PageCursor}. These methods are about a single GSP,
 * whereas the normal use case of a GSP is in pairs ({@link GenerationSafePointerPair GSPP}).
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
class GenerationSafePointer
{
    private static final int EMPTY_POINTER = 0;
    private static final int EMPTY_GENERATION = 0;

    static final long MIN_GENERATION = 1L;
    // unsigned int
    static final long MAX_GENERATION = 0xFFFFFFFFL;
    static final long GENERATION_MASK = 0xFFFFFFFFL;
    static final long MIN_POINTER = IdSpace.MIN_TREE_NODE_ID;
    static final long MAX_POINTER = 0xFFFF_FFFFFFFFL;
    static final int UNSIGNED_SHORT_MASK = 0xFFFF;

    static final int GENERATION_SIZE = 4;
    static final int POINTER_SIZE = 6;
    static final int CHECKSUM_SIZE = 2;
    static final int SIZE =
            GENERATION_SIZE +
            POINTER_SIZE +
            CHECKSUM_SIZE;

    private GenerationSafePointer()
    {
    }

    /**
     * Writes GSP at the given {@code offset}, the two fields (generation, pointer) + a checksum will be written.
     *
     * @param cursor {@link PageCursor} to write into.
     * @param generation generation to write.
     * @param pointer pointer to write.
     */
    static void write( PageCursor cursor, long generation, long pointer )
    {
        assertGenerationOnWrite( generation );
        assertPointerOnWrite( pointer );
        writeGSP( cursor, generation, pointer );
    }

    private static void writeGSP( PageCursor cursor, long generation, long pointer )
    {
        cursor.putInt( (int) generation );
        put6BLong( cursor, pointer );
        cursor.putShort( checksumOf( generation, pointer ) );
    }

    static void clean( PageCursor cursor )
    {
        writeGSP( cursor, EMPTY_GENERATION, EMPTY_POINTER );
    }

    static void assertGenerationOnWrite( long generation )
    {
        if ( generation < MIN_GENERATION || generation > MAX_GENERATION )
        {
            throw new IllegalArgumentException( "Can not write pointer with generation " + generation +
                    " because outside boundary for valid generation." );
        }
    }

    private static void assertPointerOnWrite( long pointer )
    {
        if ( (pointer > MAX_POINTER || pointer < MIN_POINTER) && TreeNode.isNode( pointer ) )
        {
            throw new IllegalArgumentException( "Can not write pointer " + pointer +
                    " because outside boundary for valid pointer" );
        }
    }

    static long readGeneration( PageCursor cursor )
    {
        return getUnsignedInt( cursor );
    }

    static long readPointer( PageCursor cursor )
    {
        return get6BLong( cursor );
    }

    static short readChecksum( PageCursor cursor )
    {
        return cursor.getShort();
    }

    static boolean verifyChecksum( PageCursor cursor, long generation, long pointer )
    {
        short checksum = cursor.getShort();
        return checksum == checksumOf( generation, pointer );
    }

    // package visible for test purposes
    /**
     * Calculates a 2-byte checksum from GSP data.
     *
     * @param generation generation of the pointer.
     * @param pointer pointer itself.
     *
     * @return a {@code short} which is the checksum of the generation-pointer.
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
        return generation == EMPTY_GENERATION && pointer == EMPTY_POINTER;
    }
}
