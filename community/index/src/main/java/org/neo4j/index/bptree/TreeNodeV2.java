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
package org.neo4j.index.bptree;

import java.util.Comparator;
import java.util.function.Consumer;

import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Integer.max;
import static java.lang.Integer.min;

public class TreeNodeV2<KEY,VALUE> implements TreeNode<KEY,VALUE>
{
    private static final byte FLAG_TYPE = 0x1; // 1: leaf, 0: internal
    private static final long NULL_ID = -1;
    private static final int SIZE_ID = 5;
    private static final int SIZE_JUMP_ITEM = 1;

    // HEADER
    private static final int BYTE_POS_FLAGS = 0;
    private static final int BYTE_POS_COUNT = BYTE_POS_FLAGS + 1;
    private static final int BYTE_POS_LEFT_SIBLING = BYTE_POS_COUNT + 1;
    private static final int BYTE_POS_RIGHT_SIBLING = BYTE_POS_LEFT_SIBLING + SIZE_ID;
    private static final int STATIC_HEADER_SIZE = BYTE_POS_RIGHT_SIBLING + SIZE_ID; // = 12

    private final int internalMaxKeyCount;
    private final int leafMaxKeyCount;
    private final int headerSize;
    private final Layout<KEY,VALUE> layout;

    private final int keySize;
    private final int valueSize;

    public TreeNodeV2( int pageSize, Layout<KEY,VALUE> layout )
    {
        this.layout = layout;
        this.keySize = layout.keySize();
        this.valueSize = layout.valueSize();

        // TODO temporarily cap key count to 255 as to have a very efficient jump list
        int tentativeInternalMaxKeyCount =
                Math.floorDiv( pageSize - (STATIC_HEADER_SIZE + SIZE_ID), keySize + SIZE_ID + SIZE_JUMP_ITEM );
        int tentativeLeafMaxKeyCount =
                Math.floorDiv( pageSize - STATIC_HEADER_SIZE, keySize + valueSize + SIZE_JUMP_ITEM );

        this.internalMaxKeyCount = min( 0xFF, tentativeInternalMaxKeyCount );
        this.leafMaxKeyCount = min( 0xFF, tentativeLeafMaxKeyCount );
        // TODO: potentially we could have different header length (the jump list) for internal/leaf
        // TODO: we only support 255 items in each node, for small items this would leave wasted space in each page
        this.headerSize = STATIC_HEADER_SIZE + max( internalMaxKeyCount, leafMaxKeyCount ) * SIZE_JUMP_ITEM;
    }

    // ROUTINES

    @Override
    public void initializeLeaf( PageCursor cursor )
    {
        setTypeLeaf( cursor );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NULL_ID );
        setLeftSibling( cursor, NULL_ID );
    }

    @Override
    public void initializeInternal( PageCursor cursor )
    {
        setTypeInternal( cursor );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NULL_ID );
        setLeftSibling( cursor, NULL_ID );
    }

    // HEADER METHODS

    @Override
    public boolean isLeaf( PageCursor cursor )
    {
        int flags = cursor.getByte( BYTE_POS_FLAGS );
        return (flags & FLAG_TYPE) != 0;
    }

    @Override
    public boolean isInternal( PageCursor cursor )
    {
        return !isLeaf( cursor );
    }

    @Override
    public int keyCount( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_COUNT ) & 0xFF; // disregard the sign
    }

    @Override
    public long rightSibling( PageCursor cursor )
    {
        return get5ByteLong( cursor, BYTE_POS_RIGHT_SIBLING );
    }

    @Override
    public long leftSibling( PageCursor cursor )
    {
        return get5ByteLong( cursor, BYTE_POS_LEFT_SIBLING );
    }

    @Override
    public void setTypeLeaf( PageCursor cursor )
    {
        setFlag( cursor, FLAG_TYPE, true );
    }

    private void setFlag( PageCursor cursor, byte mask, boolean value )
    {
        byte flags = cursor.getByte( BYTE_POS_FLAGS );
        if ( value )
        {
            flags |= mask;
        }
        else
        {
            flags &= ~mask;
        }
        cursor.putByte( BYTE_POS_FLAGS, flags );
    }

    @Override
    public void setTypeInternal( PageCursor cursor )
    {
        setFlag( cursor, FLAG_TYPE, false );
    }

    @Override
    public void setKeyCount( PageCursor cursor, int count )
    {
        if ( count > 0xFF )
        {
            throw new IllegalArgumentException( "Max key count " + 0xFF + ", tried to set " + count );
        }
        cursor.putByte( BYTE_POS_COUNT, (byte) count );
    }

    @Override
    public void setRightSibling( PageCursor cursor, long rightSiblingId )
    {
        put5ByteLong( cursor, BYTE_POS_RIGHT_SIBLING, rightSiblingId );
    }

    @Override
    public void setLeftSibling( PageCursor cursor, long leftSiblingId )
    {
        put5ByteLong( cursor, BYTE_POS_LEFT_SIBLING, leftSiblingId );
    }

    // BODY METHODS

    @Override
    public Object newOrder()
    {
        return new ItemOrder( max( internalMaxKeyCount, leafMaxKeyCount ) );
    }

    @Override
    public void getOrder( PageCursor cursor, Object into )
    {
        if ( Knobs.PHYSICALLY_SORTED_JUMP_LIST )
        {
            // We don't need to interpret the order
        }
        else
        {
            ItemOrder order = (ItemOrder) into;
            cursor.setOffset( jumpOffset( 0 ) );
            order.read( cursor, keyCount( cursor ) );
        }
    }

    @Override
    public KEY keyAt( PageCursor cursor, KEY into, int pos, Object order )
    {
        int physicalPos = getPhysicalPos( cursor, pos, order );
        cursor.setOffset( keyOffset( physicalPos ) );
        layout.readKey( cursor, into );
        return into;
    }

    /**
     * Translates a logical position, i.e. where something belongs when regarding sort order,
     * to its physical location in the tree node. Entries aren't actually stored in sorted order,
     * but merely appended in the end and instead there's a jump list in the header containing the order.
     *
     * TODO decide on approach:
     * 1 Have the jump list physically sorted: this will require more data to be stored in WAL log records
     *   since inserting in the middle requires moving all items after it.
     * 2 Have the jump list in same (unsorted) order as the entries and sort this jump list before translating.
     *
     * @param pos logical pos.
     * @return pos translated into physical pos in this tree node-
     */
    private int getPhysicalPos( PageCursor cursor, int pos, Object theOrder )
    {
        ItemOrder order = (ItemOrder) theOrder;

        if ( Knobs.PHYSICALLY_SORTED_JUMP_LIST )
        {
            return cursor.getByte( jumpOffset( pos ) ) & 0xFF;
        }

        return order.physicalPosition( pos );
    }

    private int getPhysicalChildPos( PageCursor cursor, int pos, Object order )
    {
        int physicalPos;
        if ( pos == 0 )
        {
            // This is the special pos which isn't covered by the jump list because it's always going to be
            // in physicalPos:0
            physicalPos = 0;
        }
        else
        {
            // For all other children we use the jump list (although using the key to the left
            // the pos will then be one bigger than the one for the key, so use pos - 1 and then
            // the physical pos you get back we add + 1 to, then we're good, right?
            physicalPos = getPhysicalPos( cursor, pos - 1, order ) + 1;
        }
        return physicalPos;
    }

    private int jumpOffset( int pos )
    {
        return STATIC_HEADER_SIZE + pos * SIZE_JUMP_ITEM;
    }

    private void setPhysicalPos( PageCursor cursor, int pos, int physicalPos, Object theOrder, byte[] tmp )
    {
        assert physicalPos <= 0xFF;

        if ( Knobs.PHYSICALLY_SORTED_JUMP_LIST )
        {
            // TODO ASSUMPTION: about keyCount being physical pos
            int keyCount = physicalPos;
            cursor.setOffset( jumpOffset( pos ) );
            int length = (keyCount - pos) * SIZE_JUMP_ITEM;
            cursor.getBytes( tmp, 0, length );
            cursor.setOffset( jumpOffset( pos + 1 ) );
            cursor.putBytes( tmp, 0, length );
            cursor.putByte( jumpOffset( pos ), (byte) physicalPos );
        }
        else
        {
            cursor.putByte( jumpOffset( physicalPos ), (byte) pos );
            ItemOrder order = (ItemOrder) theOrder;
            order.add( physicalPos, pos );
        }
    }

    @Override
    public void insertKeyAt( PageCursor cursor, KEY key, int pos, int keyCount, Object theOrder, byte[] tmp )
    {
        // TODO dangerous assumption in here: we call this BEFORE incrementing keyCount and only
        // when inserting a new key/value entry. We'll use keyCount as the physical position
        // FIXME this doesn't work for internal nodes since it'll have a the extra child to the right

        int physicalPos = keyCount;
        setPhysicalPos( cursor, pos, physicalPos, theOrder, tmp );
        cursor.setOffset( keyOffset( physicalPos ) );
        layout.writeKey( cursor, key );
    }

    @Override
    public void removeKeyAt( PageCursor cursor, int pos, Object order, byte[] tmp )
    {
        // TODO Simply mark it as not in use here
    }

    @Override
    public VALUE valueAt( PageCursor cursor, VALUE value, int pos, Object order )
    {
        int physicalPos = getPhysicalPos( cursor, pos, order );
        cursor.setOffset( valueOffset( physicalPos ) );
        layout.readValue( cursor, value );
        return value;
    }

    @Override
    public void insertValueAt( PageCursor cursor, VALUE value, int pos, int keyCount, Object order, byte[] tmp )
    {
        // TODO we'd really like to set key/value and key/child pairs together in this TreeNode implementation,
        // because they live together anyway.
        // TODO ASSUMPTION: physical position have already been assigned and now exists in the order
        // i.e. insertKeyAt has already been called, i.e. this is exactly like setValueAt
        setValueAt( cursor, value, pos, order );
    }

    @Override
    public void setValueAt( PageCursor cursor, VALUE value, int pos, Object order )
    {
        int physicalPos = getPhysicalPos( cursor, pos, order );
        cursor.setOffset( valueOffset( physicalPos ) );
        layout.writeValue( cursor, value );
    }

    @Override
    public void removeValueAt( PageCursor cursor, int pos, Object order, byte[] tmp )
    {
        throw new UnsupportedOperationException( "Unsupported a.t.m." );
    }

    @Override
    public long childAt( PageCursor cursor, int pos, Object order )
    {
        int physicalPos = getPhysicalChildPos( cursor, pos, order );
        return get5ByteLong( cursor, childOffset( physicalPos ) );
    }

    @Override
    public void insertChildAt( PageCursor cursor, long child, int pos, int keyCount, Object order, byte[] tmp )
    {
        int physicalPos = getPhysicalChildPos( cursor, pos, order );
        cursor.setOffset( childOffset( physicalPos ) );
        writeChild( cursor, child );
    }

    @Override
    public void setChildAt( PageCursor cursor, long child, int pos, Object order )
    {
        int physicalPos = getPhysicalChildPos( cursor, pos, order );
        put5ByteLong( cursor, childOffset( physicalPos ), child );
    }

    @Override
    public void writeChild( PageCursor cursor, long child )
    {
        put5ByteLong( cursor, cursor.getOffset(), child );
    }

    @Override
    public int internalMaxKeyCount()
    {
        return internalMaxKeyCount;
    }

    @Override
    public int leafMaxKeyCount()
    {
        return leafMaxKeyCount;
    }

    // HELPERS
    // TODO: we could store these together here because when reading them we can't sequentially scan
    // through them anyway. Keeping them together would at least reduce access scatter on write

    @Override
    public int keyOffset( int pos )
    {
        return headerSize + pos * keySize;
    }

    @Override
    public int valueOffset( int pos )
    {
        return headerSize + leafMaxKeyCount * keySize + pos * valueSize;
    }

    @Override
    public int childOffset( int pos )
    {
        return headerSize + internalMaxKeyCount * keySize + pos * SIZE_ID;
    }

    @Override
    public boolean isNode( long node )
    {
        return node != NULL_ID;
    }

    @Override
    public int keySize()
    {
        return keySize;
    }

    @Override
    public int valueSize()
    {
        return valueSize;
    }

    @Override
    public int childSize()
    {
        return SIZE_ID;
    }

    @Override
    public Comparator<KEY> keyComparator()
    {
        return layout;
    }

    private long get5ByteLong( PageCursor cursor, int offset )
    {
        long low4b = cursor.getInt( offset ) & 0xFFFFFFFFL;
        long high1b = cursor.getByte( offset + Integer.BYTES ) & 0xFF;
        long result = low4b | (high1b << 32);
        return result == 0xFFFFFFFFFFL ? NULL_ID : result;
    }

    private void put5ByteLong( PageCursor cursor, int offset, long value )
    {
        cursor.putInt( offset, (int) value );
        cursor.putByte( offset + Integer.BYTES, (byte) (value >>> 32) );
    }

    /**
     * Read all keys with a new record inserted at {@code insertPosition} so that {@code into} will contain
     * them all in physically sorted order. Called only during split.
     */
    private int readRecordsWithInsertRecordInPosition( PageCursor cursor, Consumer<PageCursor> newRecordWriter,
            int insertPosition, int totalNumberOfRecords, int recordSize, int baseRecordOffset, byte[] into,
            boolean childPos /*TODO remove this silly thing*/ )
    {
        Object order = newOrder();
        getOrder( cursor, order );
        for ( int i = 0, pos = 0, intoOffset = 0; i < totalNumberOfRecords; i++ )
        {
            if ( i != insertPosition )
            {
                int physicalPos = childPos
                        ? getPhysicalChildPos( cursor, pos, order )
                        : getPhysicalPos( cursor, pos, order );
                cursor.setOffset( baseRecordOffset + physicalPos * recordSize );
                cursor.getBytes( into, intoOffset, recordSize );
                pos++;
            }
            // else this is the gap we'll fill with the inserted record

            intoOffset += recordSize;
        }

        PageCursor buffer = ByteArrayPageCursor.wrap( into, insertPosition * recordSize, recordSize );
        newRecordWriter.accept( buffer );

        return totalNumberOfRecords * recordSize;
    }

    @Override
    public int readKeysWithInsertRecordInPosition( PageCursor cursor, Consumer<PageCursor> newRecordWriter,
            int insertPosition, int totalNumberOfRecords, byte[] into )
    {
        return readRecordsWithInsertRecordInPosition( cursor, newRecordWriter, insertPosition, totalNumberOfRecords,
                keySize, keyOffset( 0 ), into, false );
    }

    @Override
    public int readValuesWithInsertRecordInPosition( PageCursor cursor, Consumer<PageCursor> newRecordWriter,
            int insertPosition, int totalNumberOfRecords, byte[] into )
    {
        return readRecordsWithInsertRecordInPosition( cursor, newRecordWriter, insertPosition, totalNumberOfRecords,
                valueSize, valueOffset( 0 ), into, false );
    }

    @Override
    public int readChildrenWithInsertRecordInPosition( PageCursor cursor, Consumer<PageCursor> newRecordWriter,
            int insertPosition, int totalNumberOfRecords, byte[] into )
    {
        return readRecordsWithInsertRecordInPosition( cursor, newRecordWriter, insertPosition, totalNumberOfRecords,
                SIZE_ID, childOffset( 0 ), into, true );
    }

    private void writeAll( PageCursor cursor, byte[] source, int sourcePos, int targetPos, int count,
            int baseOffset, int recordSize )
    {
        int arrayOffset = sourcePos * recordSize;
        cursor.setOffset( baseOffset + recordSize * targetPos );
        cursor.putBytes( source, arrayOffset, count * recordSize );
    }

    @Override
    public void writeKeys( PageCursor cursor, byte[] source, int sourcePos, int targetPos, int count )
    {
        // Write the keys + jump list items
        writeAll( cursor, source, sourcePos, targetPos, count, keyOffset( 0 ), keySize );
        // TODO ASSUMPTION: coming in here, the keys are ordered
        ItemOrder order = (ItemOrder) newOrder();
        for ( int i = 0; i < count; i++ )
        {
            setPhysicalPos( cursor, i, i, order, source );
        }
    }

    @Override
    public void writeValues( PageCursor cursor, byte[] source, int sourcePos, int targetPos, int count )
    {
        writeAll( cursor, source, sourcePos, targetPos, count, valueOffset( 0 ), valueSize );
    }

    @Override
    public void writeChildren( PageCursor cursor, byte[] source, int sourcePos, int targetPos, int count )
    {
        writeAll( cursor, source, sourcePos, targetPos, count, childOffset( 0 ), childSize() );
    }
}
