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
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;

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
class TreeNode<KEY,VALUE>
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

    private static final byte LEAF_FLAG = 1;
    static final byte INTERNAL_FLAG = 0;
    static final long NO_NODE_FLAG = 0;

    private final int pageSize;
    private final int internalMaxKeyCount;
    private final int leafMaxKeyCount;
    private final Layout<KEY,VALUE> layout;

    private final int keySize;
    private final int valueSize;

    TreeNode( int pageSize, Layout<KEY,VALUE> layout )
    {
        this.pageSize = pageSize;
        this.layout = layout;
        this.keySize = layout.keySize();
        this.valueSize = layout.valueSize();
        this.internalMaxKeyCount = Math.floorDiv( pageSize - (HEADER_LENGTH + SIZE_PAGE_REFERENCE),
                keySize + SIZE_PAGE_REFERENCE);
        this.leafMaxKeyCount = Math.floorDiv( pageSize - HEADER_LENGTH, keySize + valueSize );

        if ( internalMaxKeyCount < 2 )
        {
            throw new MetadataMismatchException(
                    "For layout %s a page size of %d would only fit %d internal keys, minimum is 2",
                    layout, pageSize, internalMaxKeyCount );
        }
        if ( leafMaxKeyCount < 2 )
        {
            throw new MetadataMismatchException( "A page size of %d would only fit leaf keys, minimum is 2",
                    pageSize, leafMaxKeyCount );
        }
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

    KEY keyAt( PageCursor cursor, KEY into, int pos, Type type )
    {
        cursor.setOffset( keyOffset( pos ) );
        layout.readKey( cursor, into );
        return into;
    }

    // Insert key without associated value.
    // Useful for internal nodes and testing.
    void insertKeyAt( PageCursor cursor, KEY key, int pos, int keyCount, Type type )
    {
        insertKeySlotsAt( cursor, pos, 1, keyCount );
        cursor.setOffset( keyOffset( pos ) );
        layout.writeKey( cursor, key );
    }

    void insertKeyValueAt( PageCursor cursor, KEY key, VALUE value, int pos, int keyCount )
    {
        insertKeyAt( cursor, key, pos, keyCount, Type.LEAF );
        insertValueAt( cursor, value, pos, keyCount );
    }

    // Remove key without removing associated value.
    // Useful for internal nodes and testing.
    void removeKeyAt( PageCursor cursor, int pos, int keyCount, Type type )
    {
        removeSlotAt( cursor, pos, keyCount, keyOffset( 0 ), keySize );
    }

    void removeKeyValueAt( PageCursor cursor, int pos, int keyCount )
    {
        removeKeyAt( cursor, pos, keyCount, Type.LEAF );
        removeValueAt( cursor, pos, keyCount );
    }

    void setKeyAt( PageCursor cursor, KEY key, int pos, Type type )
    {
        cursor.setOffset( keyOffset( pos ) );
        layout.writeKey( cursor, key );
    }

    VALUE valueAt( PageCursor cursor, VALUE value, int pos )
    {
        cursor.setOffset( valueOffset( pos ) );
        layout.readValue( cursor, value );
        return value;
    }

    void setValueAt( PageCursor cursor, VALUE value, int pos )
    {
        cursor.setOffset( valueOffset( pos ) );
        layout.writeValue( cursor, value );
    }

    long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( childOffset( pos ) );
        return read( cursor, stableGeneration, unstableGeneration, pos );
    }

    void insertChildAt( PageCursor cursor, long child, int pos, int keyCount,
            long stableGeneration, long unstableGeneration )
    {
        insertChildSlotsAt( cursor, pos, 1, keyCount );
        setChildAt( cursor, child, pos, stableGeneration, unstableGeneration );
    }

    void removeChildAt( PageCursor cursor, int pos, int keyCount )
    {
        removeSlotAt( cursor, pos, keyCount + 1, childOffset( 0 ), childSize() );
    }

    void setChildAt( PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( childOffset( pos ) );
        writeChild( cursor, child, stableGeneration, unstableGeneration );
    }

    static void writeChild( PageCursor cursor, long child, long stableGeneration, long unstableGeneration )
    {
        GenerationSafePointerPair.write( cursor, child, stableGeneration, unstableGeneration );
    }

    void insertKeyValueSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        insertKeySlotsAt( cursor, pos, numberOfSlots, keyCount );
        insertValueSlotsAt( cursor, pos, numberOfSlots, keyCount );
    }

    // Always insert together with key. Use insertKeyValueAt
    private void insertValueAt( PageCursor cursor, VALUE value, int pos, int keyCount )
    {
        insertValueSlotsAt( cursor, pos, 1, keyCount );
        setValueAt( cursor, value, pos );
    }

    // Always insert together with key. Use removeKeyValueAt
    private void removeValueAt( PageCursor cursor, int pos, int keyCount )
    {
        removeSlotAt( cursor, pos, keyCount, valueOffset( 0 ), valueSize );
    }

    /**
     * Moves items (key/value/child) one step to the right, which means rewriting all items of the particular type
     * from pos - itemCount.
     * itemCount is keyCount for key and value, but keyCount+1 for children.
     */
    private static void insertSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int itemCount, int baseOffset,
            int itemSize )
    {
        for ( int posToMoveRight = itemCount - 1, offset = baseOffset + posToMoveRight * itemSize;
              posToMoveRight >= pos; posToMoveRight--, offset -= itemSize )
        {
            cursor.copyTo( offset, cursor, offset + itemSize * numberOfSlots, itemSize );
        }
    }

    private static void removeSlotAt( PageCursor cursor, int pos, int itemCount, int baseOffset, int itemSize )
    {
        for ( int posToMoveLeft = pos + 1, offset = baseOffset + posToMoveLeft * itemSize;
              posToMoveLeft < itemCount; posToMoveLeft++, offset += itemSize )
        {
            cursor.copyTo( offset, cursor, offset - itemSize, itemSize );
        }
    }

    private void insertKeySlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount, keyOffset( 0 ), keySize );
    }

    private void insertValueSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount, valueOffset( 0 ), valueSize );
    }

    private void insertChildSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount + 1, childOffset( 0 ), childSize() );
    }

    int internalMaxKeyCount()
    {
        return internalMaxKeyCount;
    }

    int leafMaxKeyCount()
    {
        return leafMaxKeyCount;
    }

    // HELPERS

    private int keyOffset( int pos )
    {
        return HEADER_LENGTH + pos * keySize;
    }

    private int valueOffset( int pos )
    {
        return HEADER_LENGTH + leafMaxKeyCount * keySize + pos * valueSize;
    }

    int childOffset( int pos )
    {
        return HEADER_LENGTH + internalMaxKeyCount * keySize + pos * SIZE_PAGE_REFERENCE;
    }

    static boolean isNode( long node )
    {
        return GenerationSafePointerPair.pointer( node ) != NO_NODE_FLAG;
    }

    private int keySize()
    {
        return keySize;
    }

    private int valueSize()
    {
        return valueSize;
    }

    private static int childSize()
    {
        return SIZE_PAGE_REFERENCE;
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
        return "TreeNode[pageSize:" + pageSize + ", internalMax:" + internalMaxKeyCount +
                ", leafMax:" + leafMaxKeyCount + ", keySize:" + keySize + ", valueSize:" + valueSize + "]";
    }

    /* SPLIT, MERGE AND REBALANCE */

    boolean internalOverflow( int keyCount )
    {
        return keyCount > internalMaxKeyCount();
    }

    boolean leafOverflow( int keyCount )
    {
        return keyCount > leafMaxKeyCount();
    }

    boolean leafUnderflow( int keyCount )
    {
        return keyCount < (leafMaxKeyCount() + 1) / 2;
    }

    boolean canRebalanceLeaves( int leftKeyCount, int rightKeyCount )
    {
        return leftKeyCount + rightKeyCount >= leafMaxKeyCount();
    }

    boolean canMergeLeaves( int leftKeyCount, int rightKeyCount )
    {
        return leftKeyCount + rightKeyCount <= leafMaxKeyCount();
    }

    /**
     * Performs the entry moving part of split in leaf.
     *
     * Keys and values from left are divide between left and right and the new key and value is inserted where it belongs.
     *
     * Key count is updated.
     */
    void doSplitLeaf( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int insertPos, KEY newKey,
            VALUE newValue, int middlePos )
    {
        if ( insertPos < middlePos )
        {
            //                  v-------v       copy
            // before _,_,_,_,_,_,_,_,_,_
            // insert _,_,_,X,_,_,_,_,_,_,_
            // middle           ^
            copyKeysAndValues( leftCursor, middlePos - 1, rightCursor, 0, rightKeyCount );
            insertKeyValueAt( leftCursor, newKey, newValue, insertPos, middlePos - 1 );
        }
        else
        {
            //                  v---v           first copy
            //                        v-v       second copy
            // before _,_,_,_,_,_,_,_,_,_
            // insert _,_,_,_,_,_,_,_,X,_,_
            // middle           ^
            int countBeforePos = insertPos - middlePos;
            if ( countBeforePos > 0 )
            {
                // first copy
                copyKeysAndValues( leftCursor, middlePos, rightCursor, 0, countBeforePos );
            }
            insertKeyValueAt( rightCursor, newKey, newValue, countBeforePos, countBeforePos );
            int countAfterPos = leftKeyCount - insertPos;
            if ( countAfterPos > 0 )
            {
                // second copy
                copyKeysAndValues( leftCursor, insertPos, rightCursor, countBeforePos + 1, countAfterPos );
            }
        }
        TreeNode.setKeyCount( leftCursor, middlePos );
        TreeNode.setKeyCount( rightCursor, rightKeyCount );
    }

    /**
     * Performs the entry moving part of split in internal.
     *
     * Keys and children from left is divided between left and right and the new key and child is inserted where it belongs.
     *
     * Key count is updated.
     */
    void doSplitInternal( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int insertPos, KEY newKey,
            long newRightChild, int middlePos, long stableGeneration, long unstableGeneration )
    {
        if ( insertPos < middlePos )
        {
            //                         v-------v       copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,X,_,_,_,_,_,_,_,_
            // insert child -,-,-,x,-,-,-,-,-,-,-,-
            // middle key              ^

            leftCursor.copyTo( keyOffset( middlePos ), rightCursor, keyOffset( 0 ), rightKeyCount * keySize() );
            leftCursor.copyTo( childOffset( middlePos ), rightCursor, childOffset( 0 ), (rightKeyCount + 1) * childSize() );
            insertKeyAt( leftCursor, newKey, insertPos, middlePos - 1, INTERNAL );
            insertChildAt( leftCursor, newRightChild, insertPos + 1, middlePos - 1, stableGeneration, unstableGeneration );
        }
        else
        {
            // pos > middlePos
            //                         v-v          first copy
            //                             v-v-v    second copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,_,_,_,_,_,X,_,_,_
            // insert child -,-,-,-,-,-,-,-,x,-,-,-
            // middle key              ^

            // pos == middlePos
            //                                      first copy
            //                         v-v-v-v-v    second copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,_,_,_,X,_,_,_,_,_
            // insert child -,-,-,-,-,-,x,-,-,-,-,-
            // middle key              ^

            // Keys
            int countBeforePos = insertPos - (middlePos + 1);
            // ... first copy
            if ( countBeforePos > 0 )
            {
                leftCursor.copyTo( keyOffset( middlePos + 1 ), rightCursor, keyOffset( 0 ), countBeforePos * keySize() );
            }
            // ... insert
            if ( countBeforePos >= 0 )
            {
                insertKeyAt( rightCursor, newKey, countBeforePos, countBeforePos, INTERNAL );
            }
            // ... second copy
            int countAfterPos = leftKeyCount - insertPos;
            if ( countAfterPos > 0 )
            {
                leftCursor.copyTo( keyOffset( insertPos ), rightCursor, keyOffset( countBeforePos + 1 ), countAfterPos * keySize() );
            }

            // Children
            countBeforePos = insertPos - middlePos;
            // ... first copy
            if ( countBeforePos > 0 )
            {
                // first copy
                leftCursor.copyTo( childOffset( middlePos + 1 ), rightCursor, childOffset( 0 ), countBeforePos * childSize() );
            }
            // ... insert
            insertChildAt( rightCursor, newRightChild, countBeforePos, countBeforePos, stableGeneration, unstableGeneration );
            // ... second copy
            if ( countAfterPos > 0 )
            {
                leftCursor.copyTo( childOffset( insertPos + 1 ), rightCursor, childOffset( countBeforePos + 1 ),
                        countAfterPos * childSize() );
            }
        }
        TreeNode.setKeyCount( leftCursor, middlePos );
        TreeNode.setKeyCount( rightCursor, rightKeyCount );
    }

    /**
     * Move all rightmost keys and values in left node from given position to right node.
     *
     * Key count is NOT updated.
     */
    void moveKeyValuesFromLeftToRight( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount,
            int fromPosInLeftNode )
    {
        int numberOfKeysToMove = leftKeyCount - fromPosInLeftNode;

        // Push keys and values in right sibling to the right
        insertKeyValueSlotsAt( rightCursor, 0, numberOfKeysToMove, rightKeyCount );

        // Move keys and values from left sibling to right sibling
        copyKeysAndValues( leftCursor, fromPosInLeftNode, rightCursor, 0, numberOfKeysToMove );
    }

    private void copyKeysAndValues( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count )
    {
        fromCursor.copyTo( keyOffset( fromPos ), toCursor, keyOffset( toPos ), count * keySize() );
        fromCursor.copyTo( valueOffset( fromPos ), toCursor, valueOffset( toPos ),count * valueSize() );
    }
}
