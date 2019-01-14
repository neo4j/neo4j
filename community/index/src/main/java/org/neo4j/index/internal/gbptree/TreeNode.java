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

import java.io.IOException;
import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.NO_LOGICAL_POS;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;

/**
 * Methods to manipulate single tree node such as set and get header fields,
 * insert and fetch keys, values and children.
 */
abstract class TreeNode<KEY,VALUE>
{
    enum Type
    {
        LEAF,
        INTERNAL
    }

    enum Overflow
    {
        YES,
        NO,
        NO_NEED_DEFRAG
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
    static final int BASE_HEADER_LENGTH = BYTE_POS_SUCCESSOR + SIZE_PAGE_REFERENCE;

    static final byte LEAF_FLAG = 1;
    static final byte INTERNAL_FLAG = 0;
    static final long NO_NODE_FLAG = 0;

    static final int NO_KEY_VALUE_SIZE_CAP = -1;

    final Layout<KEY,VALUE> layout;
    final int pageSize;

    TreeNode( int pageSize, Layout<KEY,VALUE> layout )
    {
        this.pageSize = pageSize;
        this.layout = layout;
    }

    static byte nodeType( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_NODE_TYPE );
    }

    private static void writeBaseHeader( PageCursor cursor, byte type, long stableGeneration, long unstableGeneration )
    {
        cursor.putByte( BYTE_POS_NODE_TYPE, NODE_TYPE_TREE_NODE );
        cursor.putByte( BYTE_POS_TYPE, type );
        setGeneration( cursor, unstableGeneration );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
        setLeftSibling( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
        setSuccessor( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
    }

    void initializeLeaf( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        writeBaseHeader( cursor, LEAF_FLAG, stableGeneration, unstableGeneration );
        writeAdditionalHeader( cursor );
    }

    void initializeInternal( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        writeBaseHeader( cursor, INTERNAL_FLAG, stableGeneration, unstableGeneration );
        writeAdditionalHeader( cursor );
    }

    /**
     * Write additional header. When called, cursor should be located directly after base header.
     * Meaning at {@link #BASE_HEADER_LENGTH}.
     */
    abstract void writeAdditionalHeader( PageCursor cursor );

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

    /**
     * Moves data from left to right to open up a gap where data can later be written without overwriting anything.
     * Key count is NOT updated!
     *
     * @param cursor Write cursor on relevant page
     * @param pos Logical position where slots should be inserted, pos is based on baseOffset and slotSize.
     * @param numberOfSlots How many slots to be inserted.
     * @param totalSlotCount How many slots there are in total. (Usually keyCount for keys and values or keyCount+1 for children).
     * @param baseOffset Offset to slot in logical position 0.
     * @param slotSize Size of one single slot.
     */
    static void insertSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int totalSlotCount, int baseOffset,
            int slotSize )
    {
        cursor.shiftBytes( baseOffset + pos * slotSize, (totalSlotCount - pos) * slotSize, numberOfSlots * slotSize );
    }

    /**
     * Moves data from right to left to remove a slot where data that should be deleted currently sits.
     * Key count is NOT updated!
     *
     * @param cursor Write cursor on relevant page
     * @param pos Logical position where slots should be inserted, pos is based on baseOffset and slotSize.
     * @param totalSlotCount How many slots there are in total. (Usually keyCount for keys and values or keyCount+1 for children).
     * @param baseOffset Offset to slot in logical position 0.
     * @param slotSize Size of one single slot.
     */
    static void removeSlotAt( PageCursor cursor, int pos, int totalSlotCount, int baseOffset, int slotSize )
    {
        cursor.shiftBytes( baseOffset + (pos + 1) * slotSize, (totalSlotCount - (pos + 1)) * slotSize, -slotSize );
    }

    abstract KEY keyAt( PageCursor cursor, KEY into, int pos, Type type );

    abstract void keyValueAt( PageCursor cursor, KEY intoKey, VALUE intoValue, int pos );

    abstract void insertKeyAndRightChildAt( PageCursor cursor, KEY key, long child, int pos, int keyCount,
            long stableGeneration, long unstableGeneration );

    abstract void insertKeyValueAt( PageCursor cursor, KEY key, VALUE value, int pos, int keyCount );

    abstract void removeKeyValueAt( PageCursor cursor, int pos, int keyCount );

    abstract void removeKeyAndRightChildAt( PageCursor cursor, int keyPos, int keyCount );

    abstract void removeKeyAndLeftChildAt( PageCursor cursor, int keyPos, int keyCount );

    /**
     * Overwrite key at position with given key.
     * @return True if key was overwritten, false otherwise.
     */
    abstract boolean setKeyAtInternal( PageCursor cursor, KEY key, int pos );

    abstract VALUE valueAt( PageCursor cursor, VALUE value, int pos );

    /**
     * Overwrite value at position with given value.
     * @return True if value was overwritten, false otherwise.
     */
    abstract boolean setValueAt( PageCursor cursor, VALUE value, int pos );

    abstract long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration );

    abstract void setChildAt( PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration );

    static void writeChild( PageCursor cursor, long child, long stableGeneration, long unstableGeneration )
    {
        long write = GenerationSafePointerPair.write( cursor, child, stableGeneration, unstableGeneration );
        GenerationSafePointerPair.assertSuccess( write );
    }

    // HELPERS

    abstract int keyValueSizeCap();

    /**
     * This method can throw and should not be used on read path.
     * Throws {@link IllegalArgumentException} if key and value combined violate key-value size limit.
     */
    abstract void validateKeyValueSize( KEY key, VALUE value );

    abstract boolean reasonableKeyCount( int keyCount );

    abstract boolean reasonableChildCount( int childCount );

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

    /* SPLIT, MERGE AND REBALANCE */

    /**
     * Will internal overflow if inserting new key?
     * @return true if leaf will overflow, else false.
     */
    abstract Overflow internalOverflow( PageCursor cursor, int currentKeyCount, KEY newKey );

    /**
     * Will leaf overflow if inserting new key and value?
     * @return true if leaf will overflow, else false.
     */
    abstract Overflow leafOverflow( PageCursor cursor, int currentKeyCount, KEY newKey, VALUE newValue );

    /**
     * Clean page with leaf node from garbage to make room for further insert without having to split.
     */
    abstract void defragmentLeaf( PageCursor cursor );

    /**
     * Clean page with internal node from garbage to make room for further insert without having to split.
     */
    abstract void defragmentInternal( PageCursor cursor );

    abstract boolean leafUnderflow( PageCursor cursor, int keyCount );

    /**
     * How do we best rebalance left and right leaf?
     * Can we move keys from underflowing left to right so that none of them underflow?
     * @return 0, do nothing. -1, merge. 1-inf, move this number of keys from left to right.
     */
    abstract int canRebalanceLeaves( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount );

    abstract boolean canMergeLeaves( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount );

    /**
     * Calculate where split should be done and move entries between leaves participating in split.
     *
     * Keys and values from left are divide between left and right and the new key and value is inserted where it belongs.
     *
     * Key count is updated.
     */
    abstract void doSplitLeaf( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int insertPos, KEY newKey, VALUE newValue,
            KEY newSplitter );

    /**
     * Performs the entry moving part of split in internal.
     *
     * Keys and children from left is divided between left and right and the new key and child is inserted where it belongs.
     *
     * Key count is updated.
     */
    abstract void doSplitInternal( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int insertPos,
            KEY newKey,
            long newRightChild, long stableGeneration, long unstableGeneration, KEY newSplitter );

    /**
     * Move all rightmost keys and values in left leaf from given position to right leaf.
     *
     * Right leaf will be defragmented.
     *
     * Update keyCount in left and right.
     */
    abstract void moveKeyValuesFromLeftToRight( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount,
            int fromPosInLeftNode );

    /**
     * Copy all keys and values in left leaf and insert to the left in right leaf.
     *
     * Right leaf will be defragmented.
     *
     * Update keyCount in right
     */
    abstract void copyKeyValuesFromLeftToRight( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount );

    // Useful for debugging
    @SuppressWarnings( "unused" )
    void printNode( PageCursor cursor, boolean includeValue, boolean includeAllocSpace, long stableGeneration, long unstableGeneration )
    {   // default no-op
    }
}
