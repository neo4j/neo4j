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

import java.io.IOException;
import java.util.Comparator;
import java.util.function.Consumer;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.gbptree.GenSafePointerPair.NO_LOGICAL_POS;
import static org.neo4j.index.gbptree.GenSafePointerPair.read;

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
 * [                            HEADER   82B                        ]|[      KEYS     ]|[     CHILDREN             ]
 * [NODETYPE][TYPE][GEN][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][NEWGEN]|[[KEY][KEY]...##]|[[CHILD][CHILD][CHILD]...##]
 *  0         1     6    10        34            58
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
 * [                            HEADER   82B                        ]|[      KEYS     ]|[     VALUES        ]
 * [NODETYPE][TYPE][GEN][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][NEWGEN]|[[KEY][KEY]...##]|[[VALUE][VALUE]...##]
 *  0         1     6    10        34            58
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
    // Shared between all node types: TreeNode and FreelistNode
    static final int BYTE_POS_NODE_TYPE = 0;
    static final byte NODE_TYPE_TREE_NODE = 1;
    static final byte NODE_TYPE_FREE_LIST_NODE = 2;

    static final int SIZE_PAGE_REFERENCE = GenSafePointerPair.SIZE;
    static final int BYTE_POS_TYPE = BYTE_POS_NODE_TYPE + Byte.BYTES;
    static final int BYTE_POS_GEN = BYTE_POS_TYPE + Byte.BYTES;
    static final int BYTE_POS_KEYCOUNT = BYTE_POS_GEN + Integer.BYTES;
    static final int BYTE_POS_RIGHTSIBLING = BYTE_POS_KEYCOUNT + Integer.BYTES;
    static final int BYTE_POS_LEFTSIBLING = BYTE_POS_RIGHTSIBLING + SIZE_PAGE_REFERENCE;
    static final int BYTE_POS_NEWGEN = BYTE_POS_LEFTSIBLING + SIZE_PAGE_REFERENCE;
    static final int HEADER_LENGTH = BYTE_POS_NEWGEN + SIZE_PAGE_REFERENCE;

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
            throw new MetadataMismatchException( "For layout " + layout + " a page size of " + pageSize +
                    " would only fit " + internalMaxKeyCount + " internal keys, minimum is 2" );
        }
        if ( leafMaxKeyCount < 2 )
        {
            throw new MetadataMismatchException( "A page size of " + pageSize + " would only fit " +
                    leafMaxKeyCount + " leaf keys, minimum is 2" );
        }
    }

    static byte nodeType( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_NODE_TYPE );
    }

    private void initialize( PageCursor cursor, byte type, long stableGeneration, long unstableGeneration )
    {
        cursor.putByte( BYTE_POS_NODE_TYPE, NODE_TYPE_TREE_NODE );
        cursor.putByte( BYTE_POS_TYPE, type );
        setGen( cursor, unstableGeneration );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
        setLeftSibling( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
        setNewGen( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
    }

    void initializeLeaf( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        initialize( cursor, LEAF_FLAG, stableGeneration, unstableGeneration );
    }

    void initializeInternal( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        initialize( cursor, INTERNAL_FLAG, stableGeneration, unstableGeneration );
    }

    // HEADER METHODS

    boolean isLeaf( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == LEAF_FLAG;
    }

    boolean isInternal( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == INTERNAL_FLAG;
    }

    long gen( PageCursor cursor )
    {
        return cursor.getInt( BYTE_POS_GEN ) & GenSafePointer.GENERATION_MASK;
    }

    int keyCount( PageCursor cursor )
    {
        return cursor.getInt( BYTE_POS_KEYCOUNT );
    }

    long rightSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_RIGHTSIBLING );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    long leftSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_LEFTSIBLING );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    long newGen( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_NEWGEN );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    void setGen( PageCursor cursor, long generation )
    {
        GenSafePointer.assertGenerationOnWrite( generation );
        cursor.putInt( BYTE_POS_GEN, (int) generation );
    }

    void setKeyCount( PageCursor cursor, int count )
    {
        cursor.putInt( BYTE_POS_KEYCOUNT, count );
    }

    void setRightSibling( PageCursor cursor, long rightSiblingId, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_RIGHTSIBLING );
        long result = GenSafePointerPair.write( cursor, rightSiblingId, stableGeneration, unstableGeneration );
        GenSafePointerPair.assertSuccess( result );
    }

    void setLeftSibling( PageCursor cursor, long leftSiblingId, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_LEFTSIBLING );
        long result = GenSafePointerPair.write( cursor, leftSiblingId, stableGeneration, unstableGeneration );
        GenSafePointerPair.assertSuccess( result );
    }

    void setNewGen( PageCursor cursor, long newGenId, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_NEWGEN );
        long result = GenSafePointerPair.write( cursor, newGenId, stableGeneration, unstableGeneration );
        GenSafePointerPair.assertSuccess( result );
    }

    long pointerGen( PageCursor cursor, long readResult )
    {
        if ( !GenSafePointerPair.isRead( readResult ) )
        {
            throw new IllegalArgumentException( "Expected read result, but got " + readResult );
        }
        int offset = GenSafePointerPair.genOffset( readResult );
        int gsppOffset = GenSafePointerPair.isLogicalPos( readResult ) ? childOffset( offset ) : offset;
        int gspOffset = GenSafePointerPair.resultIsFromSlotA( readResult ) ?
                gsppOffset : gsppOffset + GenSafePointer.SIZE;
        cursor.setOffset( gspOffset );
        return GenSafePointer.readGeneration( cursor );
    }

    // BODY METHODS

    KEY keyAt( PageCursor cursor, KEY into, int pos )
    {
        cursor.setOffset( keyOffset( pos ) );
        layout.readKey( cursor, into );
        return into;
    }

    void insertKeyAt( PageCursor cursor, KEY key, int pos, int keyCount )
    {
        insertSlotAt( cursor, pos, keyCount, keyOffset( 0 ), keySize );
        cursor.setOffset( keyOffset( pos ) );
        layout.writeKey( cursor, key );
    }

    void removeKeyAt( PageCursor cursor, int pos, int keyCount )
    {
        removeSlotAt( cursor, pos, keyCount, keyOffset( 0 ), keySize );
    }

    private void removeSlotAt( PageCursor cursor, int pos, int keyCount, int baseOffset, int itemSize )
    {
        for ( int posToMoveLeft = pos + 1, offset = baseOffset + posToMoveLeft * itemSize;
                posToMoveLeft < keyCount; posToMoveLeft++, offset += itemSize )
        {
            cursor.copyTo( offset, cursor, offset - itemSize, itemSize );
        }
    }

    /**
     * Moves items (key/value/child) one step to the right, which means rewriting all items of the particular type
     * from pos - keyCount.
     */
    private void insertSlotAt( PageCursor cursor, int pos, int keyCount, int baseOffset, int itemSize )
    {
        for ( int posToMoveRight = keyCount - 1, offset = baseOffset + posToMoveRight * itemSize;
                posToMoveRight >= pos; posToMoveRight--, offset -= itemSize )
        {
            cursor.copyTo( offset, cursor, offset + itemSize, itemSize );
        }
    }

    VALUE valueAt( PageCursor cursor, VALUE value, int pos )
    {
        cursor.setOffset( valueOffset( pos ) );
        layout.readValue( cursor, value );
        return value;
    }

    void insertValueAt( PageCursor cursor, VALUE value, int pos, int keyCount )
    {
        insertSlotAt( cursor, pos, keyCount, valueOffset( 0 ), valueSize );
        setValueAt( cursor, value, pos );
    }

    void removeValueAt( PageCursor cursor, int pos, int keyCount )
    {
        removeSlotAt( cursor, pos, keyCount, valueOffset( 0 ), valueSize );
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
        insertSlotAt( cursor, pos, keyCount + 1, childOffset( 0 ), SIZE_PAGE_REFERENCE );
        setChildAt( cursor, child, pos, stableGeneration, unstableGeneration );
    }

    void setChildAt( PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( childOffset( pos ) );
        writeChild( cursor, child, stableGeneration, unstableGeneration );
    }

    void writeChild( PageCursor cursor, long child, long stableGeneration, long unstableGeneration)
    {
        GenSafePointerPair.write( cursor, child, stableGeneration, unstableGeneration );
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

    int keyOffset( int pos )
    {
        return HEADER_LENGTH + pos * keySize;
    }

    int valueOffset( int pos )
    {
        return HEADER_LENGTH + leafMaxKeyCount * keySize + pos * valueSize;
    }

    int childOffset( int pos )
    {
        return HEADER_LENGTH + internalMaxKeyCount * keySize + pos * SIZE_PAGE_REFERENCE;
    }

    static boolean isNode( long node )
    {
        return GenSafePointerPair.pointer( node ) != NO_NODE_FLAG;
    }

    int keySize()
    {
        return keySize;
    }

    int valueSize()
    {
        return valueSize;
    }

    int childSize()
    {
        return SIZE_PAGE_REFERENCE;
    }

    Comparator<KEY> keyComparator()
    {
        return layout;
    }

    void goTo( PageCursor cursor, String messageOnError, long nodeId )
            throws IOException
    {
        PageCursorUtil.goTo( cursor, messageOnError, GenSafePointerPair.pointer( nodeId ) );
    }

    @Override
    public String toString()
    {
        return "TreeNode[pageSize:" + pageSize + ", internalMax:" + internalMaxKeyCount +
                ", leafMax:" + leafMaxKeyCount + ", keySize:" + keySize + ", valueSize:" + valueSize + "]";
    }
}
