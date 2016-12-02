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

import static org.neo4j.index.gbptree.PageCursorUtil.get6BLong;
import static org.neo4j.index.gbptree.PageCursorUtil.getUnsignedInt;
import static org.neo4j.index.gbptree.PageCursorUtil.put6BLong;

class FreelistNode
{
    static final int PAGE_ID_SIZE = GenSafePointer.POINTER_SIZE;
    static final int BYTE_POS_NEXT = TreeNode.BYTE_POS_NODE_TYPE + Byte.BYTES;
    static final int HEADER_LENGTH = BYTE_POS_NEXT + PAGE_ID_SIZE;
    static final int ENTRY_SIZE = GenSafePointer.GENERATION_SIZE + PAGE_ID_SIZE;
    static final long NO_PAGE_ID = TreeNode.NO_NODE_FLAG;

    private final int maxEntries;

    FreelistNode( int pageSize )
    {
        this.maxEntries = (pageSize - HEADER_LENGTH) / ENTRY_SIZE;
    }

    void initialize( PageCursor cursor )
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
        GenSafePointer.assertGenerationOnWrite( unstableGeneration );
        cursor.setOffset( HEADER_LENGTH + pos * ENTRY_SIZE );
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
        cursor.setOffset( HEADER_LENGTH + pos * ENTRY_SIZE );
        long generation = getUnsignedInt( cursor );
        long result = generation <= stableGeneration ? get6BLong( cursor ) : NO_PAGE_ID;
        return result;
    }

    int maxEntries()
    {
        return maxEntries;
    }

    void setNext( PageCursor cursor, long nextFreelistPage )
    {
        cursor.setOffset( BYTE_POS_NEXT );
        put6BLong( cursor, nextFreelistPage );
    }

    long next( PageCursor cursor )
    {
        cursor.setOffset( BYTE_POS_NEXT );
        return get6BLong( cursor );
    }
}
