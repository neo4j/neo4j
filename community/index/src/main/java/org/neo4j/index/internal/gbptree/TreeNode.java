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

import java.io.IOException;
import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

abstract class TreeNode<KEY,VALUE>
{
    // Shared between all node types: TreeNode and FreelistNode
    static final int BYTE_POS_NODE_TYPE = 0;
    static final byte NODE_TYPE_TREE_NODE = 1;
    static final byte NODE_TYPE_FREE_LIST_NODE = 2;

    static final byte LEAF_FLAG = 1;
    static final byte INTERNAL_FLAG = 0;

    static final long NO_NODE_FLAG = 0;

    static byte nodeType( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_NODE_TYPE );
    }

    abstract void initialize( PageCursor cursor, byte type, long stableGeneration, long unstableGeneration );

    abstract void initializeLeaf( PageCursor cursor, long stableGeneration, long unstableGeneration );

    abstract void initializeInternal( PageCursor cursor, long stableGeneration, long unstableGeneration );

    abstract boolean isLeaf( PageCursor cursor );

    abstract boolean isInternal( PageCursor cursor );

    abstract long generation( PageCursor cursor );

    abstract int keyCount( PageCursor cursor );

    abstract long rightSibling( PageCursor cursor, long stableGeneration, long unstableGeneration );

    abstract long leftSibling( PageCursor cursor, long stableGeneration, long unstableGeneration );

    abstract long successor( PageCursor cursor, long stableGeneration, long unstableGeneration );

    abstract void setGeneration( PageCursor cursor, long generation );

    abstract void setKeyCount( PageCursor cursor, int count );

    abstract void setRightSibling( PageCursor cursor, long rightSiblingId, long stableGeneration, long unstableGeneration );

    abstract void setLeftSibling( PageCursor cursor, long leftSiblingId, long stableGeneration, long unstableGeneration );

    abstract void setSuccessor( PageCursor cursor, long successorId, long stableGeneration, long unstableGeneration );

    abstract long pointerGeneration( PageCursor cursor, long readResult );

    abstract KEY keyAt( PageCursor cursor, KEY into, int pos );

    abstract void insertKeyAt( PageCursor cursor, KEY key, int pos, int keyCount );

    abstract void removeKeyAt( PageCursor cursor, int pos, int keyCount );

    abstract void removeSlotAt( PageCursor cursor, int pos, int itemCount, int baseOffset, int itemSize );

    abstract void setKeyAt( PageCursor cursor, KEY key, int pos );

    abstract VALUE valueAt( PageCursor cursor, VALUE value, int pos );

    abstract void insertValueAt( PageCursor cursor, VALUE value, int pos, int keyCount );

    abstract void removeValueAt( PageCursor cursor, int pos, int keyCount );

    abstract void setValueAt( PageCursor cursor, VALUE value, int pos );

    abstract long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration );

    abstract void insertChildAt( PageCursor cursor, long child, int pos, int keyCount, long stableGeneration,
            long unstableGeneration );

    abstract void removeChildAt( PageCursor cursor, int pos, int keyCount );

    abstract void setChildAt( PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration );

    abstract void writeChild( PageCursor cursor, long child, long stableGeneration, long unstableGeneration );

    abstract void insertKeySlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount );

    abstract void insertValueSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount );

    abstract void insertChildSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount );

    abstract int internalMaxKeyCount();

    abstract int leafMaxKeyCount();

    abstract int keyOffset( int pos );

    abstract int valueOffset( int pos );

    abstract int childOffset( int pos );

    static boolean isNode( long node )
    {
        return GenerationSafePointerPair.pointer( node ) != NO_NODE_FLAG;
    }

    abstract int keySize();

    abstract int valueSize();

    abstract int childSize();

    abstract Comparator<KEY> keyComparator();

    abstract void goTo( PageCursor cursor, String messageOnError, long nodeId ) throws IOException;
}
