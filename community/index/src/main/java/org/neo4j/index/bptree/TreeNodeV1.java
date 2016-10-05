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

/**
 * Methods to manipulate single node such as set and get header fields,
 * insert and fetch keys, values and children.
 *
 * DESIGN
 *
 * Using Separate design the internal nodes should look like
 *
 * # = empty space
 *
 * [                    HEADER               ]|[      KEYS     ]|[     CHILDREN      ]
 * [TYPE][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING]|[[KEY][KEY]...##]|[[CHILD][CHILD]...##]
 *  0     1         5             13            21
 *
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 *
 * Calc offset for child i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_INTERNAL + i * SIZE_CHILD
 *
 *
 * Using Separate design the leaf nodes should look like
 *
 *
 * [                   HEADER                ]|[      KEYS     ]|[       VALUES      ]
 * [TYPE][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING]|[[KEY][KEY]...##]|[[VALUE][VALUE]...##]
 *  0     1         5             13            21
 *
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 *
 * Calc offset for value i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_LEAF + i * SIZE_VALUE
 *
 * @param <KEY> type of key
 * @param <VALUE> type of value
 */
public class TreeNodeV1<KEY,VALUE> implements TreeNode<KEY,VALUE>
{
    private static final int BYTE_POS_TYPE = 0;
    private static final int BYTE_POS_KEYCOUNT = 1;
    private static final int BYTE_POS_RIGHTSIBLING = 5;
    private static final int BYTE_POS_LEFTSIBLING = 13;
    private static final int HEADER_LENGTH = 21;

    private static final int SIZE_CHILD = Long.BYTES;

    private static final byte LEAF_FLAG = 1;
    private static final byte INTERNAL_FLAG = 0;
    private static final long NO_NODE_FLAG = -1;

    private final int internalMaxKeyCount;
    private final int leafMaxKeyCount;
    private final Layout<KEY,VALUE> layout;

    private final int keySize;
    private final int valueSize;

    public TreeNodeV1( int pageSize, Layout<KEY,VALUE> layout )
    {
        this.layout = layout;
        this.keySize = layout.keySize();
        this.valueSize = layout.valueSize();
        this.internalMaxKeyCount = Math.floorDiv( pageSize - (HEADER_LENGTH + SIZE_CHILD), keySize + SIZE_CHILD);
        this.leafMaxKeyCount = Math.floorDiv( pageSize - HEADER_LENGTH, keySize + valueSize );
    }

    // ROUTINES

    @Override
    public void initializeLeaf( PageCursor cursor )
    {
        setTypeLeaf( cursor );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG );
        setLeftSibling( cursor, NO_NODE_FLAG );
    }

    @Override
    public void initializeInternal( PageCursor cursor )
    {
        setTypeInternal( cursor );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG );
        setLeftSibling( cursor, NO_NODE_FLAG );
    }

    // HEADER METHODS

    @Override
    public boolean isLeaf( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == LEAF_FLAG;
    }

    @Override
    public boolean isInternal( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == INTERNAL_FLAG;
    }

    @Override
    public int keyCount( PageCursor cursor )
    {
        return cursor.getInt( BYTE_POS_KEYCOUNT );
    }

    @Override
    public long rightSibling( PageCursor cursor )
    {
        return cursor.getLong( BYTE_POS_RIGHTSIBLING );
    }

    @Override
    public long leftSibling( PageCursor cursor )
    {
        return cursor.getLong( BYTE_POS_LEFTSIBLING );
    }

    @Override
    public void setTypeLeaf( PageCursor cursor )
    {
        cursor.putByte( BYTE_POS_TYPE, LEAF_FLAG );
    }

    @Override
    public void setTypeInternal( PageCursor cursor )
    {
        cursor.putByte( BYTE_POS_TYPE, INTERNAL_FLAG );
    }

    @Override
    public void setKeyCount( PageCursor cursor, int count )
    {
        cursor.putInt( BYTE_POS_KEYCOUNT, count );
    }

    @Override
    public void setRightSibling( PageCursor cursor, long rightSiblingId )
    {
        cursor.putLong( BYTE_POS_RIGHTSIBLING, rightSiblingId );
    }

    @Override
    public void setLeftSibling( PageCursor cursor, long leftSiblingId )
    {
        cursor.putLong( BYTE_POS_LEFTSIBLING, leftSiblingId );
    }

    // BODY METHODS

    @Override
    public Void newOrder()
    {
        return null;
    }

    @Override
    public void getOrder( PageCursor cursor, Object into )
    {   // Order is implicit from physical order
    }

    @Override
    public KEY keyAt( PageCursor cursor, KEY into, int pos, Object order )
    {
        cursor.setOffset( keyOffset( pos ) );
        layout.readKey( cursor, into );
        return into;
    }

    @Override
    public void insertKeyAt( PageCursor cursor, KEY key, int pos, int keyCount, Object order, byte[] tmp )
    {
        insertSlotAt( cursor, pos, keyCount, keyOffset( 0 ), keySize, tmp );
        cursor.setOffset( keyOffset( pos ) );
        layout.writeKey( cursor, key );
    }

    @Override
    public void removeKeyAt( PageCursor cursor, int pos, Object order, byte[] tmp )
    {
        removeSlotAt( cursor, pos, keyOffset( 0 ), keySize, tmp );
    }

    private void removeSlotAt( PageCursor cursor, int pos, int baseOffset, int itemSize, byte[] tmp )
    {
        int from = pos + 1;
        int count = keyCount( cursor ) - from;
        copyItems( cursor, from, count, baseOffset, itemSize, tmp );
        writeItems( cursor, pos, count, baseOffset, itemSize, tmp );
    }

    /**
     * Moves items (key/value/child) one step to the right, which means rewriting all items of the particular type
     * from pos - keyCount.
     */
    private void insertSlotAt( PageCursor cursor, int pos, int toExcluding, int baseOffset, int itemSize, byte[] tmp )
    {
        // Move all items after pos one step to the right
        int count = toExcluding - pos;
        if ( count > 0 )
        {
            copyItems( cursor, pos, count, baseOffset, itemSize, tmp );
            writeItems( cursor, pos + 1, count, baseOffset, itemSize, tmp );
        }
    }

    private void writeItems( PageCursor cursor, int pos, int count, int baseOffset, int itemSize, byte[] tmp )
    {
        cursor.setOffset( baseOffset + pos * itemSize );
        cursor.putBytes( tmp, 0, count * itemSize );
    }

    private void copyItems( PageCursor cursor, int pos, int count, int baseOffset, int itemSize, byte[] tmp )
    {
        cursor.setOffset( baseOffset + pos * itemSize );
        cursor.getBytes( tmp, 0, count * itemSize );
    }

    @Override
    public VALUE valueAt( PageCursor cursor, VALUE value, int pos, Object order )
    {
        cursor.setOffset( valueOffset( pos ) );
        layout.readValue( cursor, value );
        return value;
    }

    @Override
    public void insertValueAt( PageCursor cursor, VALUE value, int pos, int keyCount, Object order, byte[] tmp )
    {
        insertSlotAt( cursor, pos, keyCount, valueOffset( 0 ), valueSize, tmp );
        setValueAt( cursor, value, pos, order );
    }

    @Override
    public void removeValueAt( PageCursor cursor, int pos, Object order, byte[] tmp )
    {
        removeSlotAt( cursor, pos, valueOffset( 0 ), valueSize, tmp );
    }

    @Override
    public void setValueAt( PageCursor cursor, VALUE value, int pos, Object order )
    {
        cursor.setOffset( valueOffset( pos ) );
        layout.writeValue( cursor, value );
    }

    @Override
    public long childAt( PageCursor cursor, int pos, Object order )
    {
        return cursor.getLong( childOffset( pos ) );
    }

    @Override
    public void insertChildAt( PageCursor cursor, long child, int pos, int keyCount, Object order, byte[] tmp )
    {
        insertSlotAt( cursor, pos, keyCount + 1, childOffset( 0 ), SIZE_CHILD, tmp );
        setChildAt( cursor, child, pos, order );
    }

    @Override
    public void setChildAt( PageCursor cursor, long child, int pos, Object order )
    {
        cursor.putLong( childOffset( pos ), child );
    }

    @Override
    public void writeChild( PageCursor cursor, long child )
    {
        cursor.putLong( child );
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

    @Override
    public int keyOffset( int pos )
    {
        return HEADER_LENGTH + pos * keySize;
    }

    @Override
    public int valueOffset( int pos )
    {
        return HEADER_LENGTH + leafMaxKeyCount * keySize + pos * valueSize;
    }

    @Override
    public int childOffset( int pos )
    {
        return HEADER_LENGTH + internalMaxKeyCount * keySize + pos * SIZE_CHILD;
    }

    @Override
    public boolean isNode( long node )
    {
        return node != NO_NODE_FLAG;
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
        return SIZE_CHILD;
    }

    @Override
    public Comparator<KEY> keyComparator()
    {
        return layout;
    }

    @Override
    public int readKeysWithInsertRecordInPosition( PageCursor cursor, Consumer<PageCursor> newRecordWriter,
            int insertPosition, int totalNumberOfRecords, byte[] into )
    {
        return readRecordsWithInsertRecordInPosition( cursor, newRecordWriter, insertPosition, totalNumberOfRecords,
                keySize, keyOffset( 0 ), into );
    }

    @Override
    public int readValuesWithInsertRecordInPosition( PageCursor cursor, Consumer<PageCursor> newRecordWriter,
            int insertPosition, int totalNumberOfRecords, byte[] into )
    {
        return readRecordsWithInsertRecordInPosition( cursor, newRecordWriter, insertPosition, totalNumberOfRecords,
                valueSize, valueOffset( 0 ), into );
    }

    @Override
    public int readChildrenWithInsertRecordInPosition( PageCursor cursor, Consumer<PageCursor> newRecordWriter,
            int insertPosition, int totalNumberOfRecords, byte[] into )
    {
        return readRecordsWithInsertRecordInPosition( cursor, newRecordWriter, insertPosition, totalNumberOfRecords,
                childSize(), childOffset( 0 ), into );
    }

    /**
     * Leaves cursor on same page as when called. No guarantees on offset.
     *
     * Create a byte[] with totalNumberOfRecords of recordSize from cursor reading from baseRecordOffset
     * with newRecord inserted in insertPosition, with the following records shifted to the right.
     *
     * Simply: Records of size recordSize that can be read from offset baseRecordOffset in page pinned by cursor has
     * some ordering. This ordering is preserved with new record inserted in insertPosition in the returned byte[],
     * NOT in the page.
     *
     * @param cursor                {@link org.neo4j.io.pagecache.PageCursor} pinned to page to read records from
     * @param newRecordWriter       new data to be inserted in insertPosition in returned byte[].
     *                              This is a {@link Consumer} since the type of data can differ (value/child),
     *                              although perhaps this can be done in a better way
     * @param insertPosition        position of newRecord. 0 being before all other records,
     *                              (totalNumberOfRecords - 1) being after all other records
     * @param totalNumberOfRecords  the total number of records to be contained in returned byte[], including newRecord
     * @param recordSize            the size in number of bytes of one record
     * @param baseRecordOffset      the offset from where cursor should start read records
     * @param into                  byte array to copy bytes into
     * @return                      number of bytes copied into the {@code into} byte[],
     *                              that is insertPosition * recordSize
     */
    private int readRecordsWithInsertRecordInPosition( PageCursor cursor, Consumer<PageCursor> newRecordWriter,
            int insertPosition, int totalNumberOfRecords, int recordSize, int baseRecordOffset, byte[] into )
    {
        int length = (totalNumberOfRecords) * recordSize;

        // First read all records

        // Read all records on previous to insertPosition
        cursor.setOffset( baseRecordOffset );
        cursor.getBytes( into, 0, insertPosition * recordSize );

        // Read newRecord
        // TODO: A bit expensive to wrap in a PageCursor tin foil just to write this middle key
        PageCursor buffer = ByteArrayPageCursor.wrap( into, insertPosition * recordSize, recordSize );
        newRecordWriter.accept( buffer );

        // Read all records following insertPosition
        cursor.setOffset( baseRecordOffset + insertPosition * recordSize );
        cursor.getBytes( into, (insertPosition + 1) * recordSize,
                ((totalNumberOfRecords - 1) - insertPosition) * recordSize );
        return length;
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
        writeAll( cursor, source, sourcePos, targetPos, count, keyOffset( 0 ), keySize() );
    }

    @Override
    public void writeValues( PageCursor cursor, byte[] source, int sourcePos, int targetPos, int count )
    {
        writeAll( cursor, source, sourcePos, targetPos, count, valueOffset( 0 ), valueSize() );
    }

    @Override
    public void writeChildren( PageCursor cursor, byte[] source, int sourcePos, int targetPos, int count )
    {
        writeAll( cursor, source, sourcePos, targetPos, count, childOffset( 0 ), childSize() );
    }
}
