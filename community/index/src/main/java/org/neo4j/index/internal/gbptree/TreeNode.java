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

import java.io.IOException;
import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.NO_LOGICAL_POS;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;

/**
 * Methods to manipulate single tree node such as set and get header fields,
 * insert and fetch keys, values and children.
 * <p>
 * DESIGN
 * <p>
 * Using Separate design the internal nodes should look like
 * <pre>
 * # = empty space
 *
 * [                                   HEADER   82B                           ]|[   KEYS   ]|[     CHILDREN      ]
 * [NODETYPE][TYPE][GENERATION][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][SUCCESSOR]|[[KEY]...##]|[[CHILD][CHILD]...##]
 *  0         1     2           6         10            34           58          82
 * </pre>
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 * <p>
 * Calc offset for child i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_INTERNAL + i * SIZE_CHILD
 * <p>
 * Using Separate design the leaf nodes should look like
 *
 * <pre>
 * [                                   HEADER   82B                           ]|[    KEYS  ]|[   VALUES   ]
 * [NODETYPE][TYPE][GENERATION][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][SUCCESSOR]|[[KEY]...##]|[[VALUE]...##]
 *  0         1     2           6         10            34           58          82
 * </pre>
 *
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 * <p>
 * Calc offset for value i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_LEAF + i * SIZE_VALUE
 *
 * @param <KEY> type of key
 * @param <VALUE> type of value
 */
abstract class TreeNode<KEY,VALUE>
{
    enum Type
    {
        LEAF,
        INTERNAL
    }

    // Shared between all node types: TreeNode and FreelistNode
    static final int BYTE_POS_NODE_TYPE = 0;
    static final byte NODE_TYPE_TREE_NODE = 1;
    static final byte NODE_TYPE_FREE_LIST_NODE = 2;

    static final int SIZE_PAGE_REFERENCE = GenerationSafePointerPair.SIZE;
    static final int BYTE_POS_TYPE = BYTE_POS_NODE_TYPE + Byte.BYTES;
    static final int BYTE_POS_GENERATION = BYTE_POS_TYPE + Byte.BYTES;
    static final int BYTE_POS_KEYCOUNT = BYTE_POS_GENERATION + Integer.BYTES;
    static final int BYTE_POS_RIGHTSIBLING = BYTE_POS_KEYCOUNT + Integer.BYTES;
    static final int BYTE_POS_LEFTSIBLING = BYTE_POS_RIGHTSIBLING + SIZE_PAGE_REFERENCE;
    static final int BYTE_POS_SUCCESSOR = BYTE_POS_LEFTSIBLING + SIZE_PAGE_REFERENCE;
    static final int HEADER_LENGTH = BYTE_POS_SUCCESSOR + SIZE_PAGE_REFERENCE;

    static final byte LEAF_FLAG = 1;
    static final byte INTERNAL_FLAG = 0;
    static final long NO_NODE_FLAG = 0;

    final Layout<KEY,VALUE> layout;
    private final int pageSize;

    TreeNode( int pageSize, Layout<KEY,VALUE> layout )
    {
        this.pageSize = pageSize;
        this.layout = layout;
    }

    static byte nodeType( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_NODE_TYPE );
    }

    private static void initialize( PageCursor cursor, byte type, long stableGeneration, long unstableGeneration )
    {
        cursor.putByte( BYTE_POS_NODE_TYPE, NODE_TYPE_TREE_NODE );
        cursor.putByte( BYTE_POS_TYPE, type );
        setGeneration( cursor, unstableGeneration );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
        setLeftSibling( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
        setSuccessor( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
    }

    static void initializeLeaf( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        initialize( cursor, LEAF_FLAG, stableGeneration, unstableGeneration );
    }

    static void initializeInternal( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        initialize( cursor, INTERNAL_FLAG, stableGeneration, unstableGeneration );
    }

    // HEADER METHODS

    static boolean isLeaf( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == LEAF_FLAG;
    }

    static boolean isInternal( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == INTERNAL_FLAG;
    }

    static long generation( PageCursor cursor )
    {
        return cursor.getInt( BYTE_POS_GENERATION ) & GenerationSafePointer.GENERATION_MASK;
    }

    static int keyCount( PageCursor cursor )
    {
        return cursor.getInt( BYTE_POS_KEYCOUNT );
    }

    static long rightSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_RIGHTSIBLING );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    static long leftSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_LEFTSIBLING );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    static long successor( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_SUCCESSOR );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    static void setGeneration( PageCursor cursor, long generation )
    {
        GenerationSafePointer.assertGenerationOnWrite( generation );
        cursor.putInt( BYTE_POS_GENERATION, (int) generation );
    }

    static void setKeyCount( PageCursor cursor, int count )
    {
        if ( count < 0 )
        {
            throw new IllegalArgumentException( "Invalid key count, " + count + ". On tree node " + cursor.getCurrentPageId() + "." );
        }
        cursor.putInt( BYTE_POS_KEYCOUNT, count );
    }

    static void setRightSibling( PageCursor cursor, long rightSiblingId, long stableGeneration,
            long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_RIGHTSIBLING );
        long result = GenerationSafePointerPair.write( cursor, rightSiblingId, stableGeneration, unstableGeneration );
        GenerationSafePointerPair.assertSuccess( result );
    }

    static void setLeftSibling( PageCursor cursor, long leftSiblingId, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_LEFTSIBLING );
        long result = GenerationSafePointerPair.write( cursor, leftSiblingId, stableGeneration, unstableGeneration );
        GenerationSafePointerPair.assertSuccess( result );
    }

    static void setSuccessor( PageCursor cursor, long successorId, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_SUCCESSOR );
        long result = GenerationSafePointerPair.write( cursor, successorId, stableGeneration, unstableGeneration );
        GenerationSafePointerPair.assertSuccess( result );
    }

    long pointerGeneration( PageCursor cursor, long readResult )
    {
        if ( !GenerationSafePointerPair.isRead( readResult ) )
        {
            throw new IllegalArgumentException( "Expected read result, but got " + readResult );
        }
        int offset = GenerationSafePointerPair.generationOffset( readResult );
        int gsppOffset = GenerationSafePointerPair.isLogicalPos( readResult ) ? childOffset( offset ) : offset;
        int gspOffset = GenerationSafePointerPair.resultIsFromSlotA( readResult ) ?
                        gsppOffset : gsppOffset + GenerationSafePointer.SIZE;
        cursor.setOffset( gspOffset );
        return GenerationSafePointer.readGeneration( cursor );
    }

    // BODY METHODS

    abstract KEY keyAt( PageCursor cursor, KEY into, int pos, Type type );

    abstract void insertKeyAndRightChildAt( PageCursor cursor, KEY key, long child, int pos, int keyCount,
            long stableGeneration, long unstableGeneration );

    abstract void insertKeyValueAt( PageCursor cursor, KEY key, VALUE value, int pos, int keyCount );

    // Remove key without removing associated value.
    // Useful for internal nodes and testing.
    abstract void removeKeyAt( PageCursor cursor, int pos, int keyCount, Type type );

    abstract void removeKeyValueAt( PageCursor cursor, int pos, int keyCount );

    abstract void removeKeyAndRightChildAt( PageCursor cursor, int keyPos, int keyCount );

    abstract void removeKeyAndLeftChildAt( PageCursor cursor, int keyPos, int keyCount );

    abstract void setKeyAt( PageCursor cursor, KEY key, int pos, Type type );

    abstract VALUE valueAt( PageCursor cursor, VALUE value, int pos );

    abstract void setValueAt( PageCursor cursor, VALUE value, int pos );

    abstract long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration );

    abstract void setChildAt( PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration );

    static void writeChild( PageCursor cursor, long child, long stableGeneration, long unstableGeneration )
    {
        GenerationSafePointerPair.write( cursor, child, stableGeneration, unstableGeneration );
    }

    abstract int internalMaxKeyCount();

    abstract int leafMaxKeyCount();

    // HELPERS

    abstract boolean reasonableKeyCount( int keyCount );

    abstract int childOffset( int pos );

    static boolean isNode( long node )
    {
        return GenerationSafePointerPair.pointer( node ) != NO_NODE_FLAG;
    }

    Comparator<KEY> keyComparator()
    {
        return layout;
    }

    static void goTo( PageCursor cursor, String messageOnError, long nodeId )
            throws IOException
    {
        PageCursorUtil.goTo( cursor, messageOnError, GenerationSafePointerPair.pointer( nodeId ) );
    }

    @Override
    public String toString()
    {
        return "TreeNode[pageSize:" + pageSize + ", internalMax:" + internalMaxKeyCount() + ", leafMax:" + leafMaxKeyCount() + ", " +
                additionalToString() + "]";
    }

    abstract String additionalToString();

    /* SPLIT, MERGE AND REBALANCE */

    abstract boolean internalOverflow( int keyCount );

    abstract boolean leafOverflow( int keyCount );

    abstract boolean leafUnderflow( int keyCount );

    abstract boolean canRebalanceLeaves( int leftKeyCount, int rightKeyCount );

    abstract boolean canMergeLeaves( int leftKeyCount, int rightKeyCount );

    /**
     * Performs the entry moving part of split in leaf.
     *
     * Keys and values from left are divide between left and right and the new key and value is inserted where it belongs.
     *
     * Key count is updated.
     */
    abstract void doSplitLeaf( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int insertPos,
            KEY newKey,
            VALUE newValue, int middlePos );

    /**
     * Performs the entry moving part of split in internal.
     *
     * Keys and children from left is divided between left and right and the new key and child is inserted where it belongs.
     *
     * Key count is updated.
     */
    abstract void doSplitInternal( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int insertPos,
            KEY newKey,
            long newRightChild, int middlePos, long stableGeneration, long unstableGeneration );

    /**
     * Move all rightmost keys and values in left node from given position to right node.
     *
     * Key count is NOT updated.
     */
    abstract void moveKeyValuesFromLeftToRight( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount,
            int fromPosInLeftNode );

}
