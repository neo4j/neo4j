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
 * Manages the physical format of a free-list node, i.e. how bytes about free-list pages
 * are represented out in {@link PageCursor}. High-level view of the format:
 *
 * <pre>
 * [HEADER         ][RELEASED PAGE IDS...                         ]
 * [NODE TYPE][NEXT][GENERATION,ID][GENERATION,ID][...............]
 * </pre>
 *
 * A free-list node is a page in the same {@link org.neo4j.io.pagecache.PagedFile mapped page cache file}
 * as a {@link TreeNode}. They distinguish themselves from one another by a "node type" one-byte header.
 */
class FreelistNode
{
    private static final int PAGE_ID_SIZE = GenerationSafePointer.POINTER_SIZE;
    private static final int BYTE_POS_NEXT = TreeNode.BYTE_POS_NODE_TYPE + Byte.BYTES;
    private static final int HEADER_LENGTH = BYTE_POS_NEXT + PAGE_ID_SIZE;
    private static final int ENTRY_SIZE = GenerationSafePointer.GENERATION_SIZE + PAGE_ID_SIZE;
    static final long NO_PAGE_ID = TreeNode.NO_NODE_FLAG;

    private final int maxEntries;

    FreelistNode( int pageSize )
    {
        this.maxEntries = (pageSize - HEADER_LENGTH) / ENTRY_SIZE;
    }

    static void initialize( PageCursor cursor )
    {
        cursor.putByte( TreeNode.BYTE_POS_NODE_TYPE, TreeNode.NODE_TYPE_FREE_LIST_NODE );
    }

    void write( PageCursor cursor, long unstableGeneration, long pageId, int pos )
    {
        if ( pageId == NO_PAGE_ID )
        {
            throw new IllegalArgumentException( "Tried to write pageId " + pageId + " which means null" );
        }
        assertPos( pos );
        GenerationSafePointer.assertGenerationOnWrite( unstableGeneration );
        cursor.setOffset( entryOffset( pos ) );
        cursor.putInt( (int) unstableGeneration );
        put6BLong( cursor, pageId );
    }

    private void assertPos( int pos )
    {
        if ( pos >= maxEntries )
        {
            throw new IllegalArgumentException( "Pos " + pos + " too big, max entries " + maxEntries );
        }
        if ( pos < 0 )
        {
            throw new IllegalArgumentException( "Negative pos " + pos );
        }
    }

    long read( PageCursor cursor, long stableGeneration, int pos )
    {
        assertPos( pos );
        cursor.setOffset( entryOffset( pos ) );
        long generation = getUnsignedInt( cursor );
        return generation <= stableGeneration ? get6BLong( cursor ) : NO_PAGE_ID;
    }

    private static int entryOffset( int pos )
    {
        return HEADER_LENGTH + pos * ENTRY_SIZE;
    }

    int maxEntries()
    {
        return maxEntries;
    }

    static void setNext( PageCursor cursor, long nextFreelistPage )
    {
        cursor.setOffset( BYTE_POS_NEXT );
        put6BLong( cursor, nextFreelistPage );
    }

    static long next( PageCursor cursor )
    {
        cursor.setOffset( BYTE_POS_NEXT );
        return get6BLong( cursor );
    }
}
