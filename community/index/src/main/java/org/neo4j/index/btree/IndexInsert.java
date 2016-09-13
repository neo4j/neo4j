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
package org.neo4j.index.btree;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.index.IdProvider;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Integer.max;

/**
 * Implementation of the insert algorithm in this B+ tree including split.
 * Takes storage format into consideration.
 */
public class IndexInsert
{
    private final IdProvider idProvider;
    private final BTreeNode bTreeNode;
    private final byte[] tmp;

    public IndexInsert( IdProvider idProvider, BTreeNode bTreeNode )
    {
        this.idProvider = idProvider;
        this.bTreeNode = bTreeNode;
        this.tmp = new byte[max( bTreeNode.internalMaxKeyCount(), bTreeNode.leafMaxKeyCount() ) *
                            max( BTreeNode.SIZE_KEY, BTreeNode.SIZE_VALUE )];
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * @param cursor        {@link org.neo4j.io.pagecache.PageCursor} pinned to page where insertion is to be done.
     * @param key           key to be inserted
     * @param value         value to be associated with key
     * @return              {@link SplitResult} from insert to be used caller.
     * @throws IOException  on cursor failure
     */
    public SplitResult insert( PageCursor cursor, long[] key, long[] value ) throws IOException
    {
        if ( bTreeNode.isLeaf( cursor ) )
        {
            return insertInLeaf( cursor, key, value );
        }

        int keyCount = bTreeNode.keyCount( cursor );

        int pos = IndexSearch.search( cursor, bTreeNode, key );

        long currentId = cursor.getCurrentPageId();
        cursor.next( bTreeNode.childAt( cursor, pos ) );

        SplitResult split = insert( cursor, key, value );

        cursor.next( currentId );

        if ( split != null )
        {
            return insertInInternal( cursor, currentId, keyCount, split.primKey, split.right );
        }
        return null;
    }

    /**
     * Leaves cursor at same page as when called. No guaranties on offset.
     *
     * Insertion in internal is always triggered by a split in child.
     * The result of a split is a primary key that is sent upwards in the b+tree and the newly created right child.
     *
     * @param cursor        {@link org.neo4j.io.pagecache.PageCursor} pinned to page containing internal node,
     *                      current node
     * @param nodeId        id of current node
     * @param keyCount      the key count of current node
     * @param primKey       the primary key to be inserted
     * @param rightChild    the right child of primKey
     * @return              {@link SplitResult} from insert to be used caller.
     * @throws IOException  on cursor failure
     */
    private SplitResult insertInInternal( PageCursor cursor, long nodeId, int keyCount, long[] primKey, long rightChild )
            throws IOException
    {
        if ( keyCount < bTreeNode.internalMaxKeyCount() )
        {
            // No overflow
            int pos = IndexSearch.search( cursor, bTreeNode, primKey );

            // Insert and move keys
            int tmpLength = bTreeNode.keysFromTo( cursor, pos, keyCount, tmp );
            bTreeNode.setKeyAt( cursor, primKey, pos );
            bTreeNode.setKeysAt( cursor, tmp, pos + 1, tmpLength );

            // Insert and move children
            tmpLength = bTreeNode.childrenFromTo( cursor, pos + 1, keyCount + 1, tmp );
            bTreeNode.setChildAt( cursor, rightChild, pos + 1 );
            bTreeNode.setChildrenAt( cursor, tmp, pos + 2, tmpLength );

            // Increase key count
            bTreeNode.setKeyCount( cursor, keyCount + 1 );

            return null;
        }

        // Overflow
        return splitInternal( cursor, nodeId, primKey, rightChild, keyCount );
    }

    /**
     *
     * Leaves cursor at same page as when called. No guarantees on offset.
     *
     * Split in internal node caused by an insertion of primKey and newRightChild
     *
     * @param cursor        {@link org.neo4j.io.pagecache.PageCursor} pinned to page containing internal node, fullNode.
     * @param fullNode      id of node to be split.
     * @param primKey       primary key to be inserted, causing the split
     * @param newRightChild right child of primKey
     * @param keyCount      key count for fullNode
     * @return              {@link SplitResult} from insert to be used caller.
     * @throws IOException  on cursor failure
     */
    private SplitResult splitInternal( PageCursor cursor, long fullNode, long[] primKey, long newRightChild,
            int keyCount )

            throws IOException
    {
        long newRight = idProvider.acquireNewId();

        // First fullNode (left node). Then move on to right node.

        // Need old right sibling to set for new right sibling later
        long oldRight = bTreeNode.rightSibling( cursor );

        // Update right sibling for splitting node
        bTreeNode.setRightSibling( cursor, newRight );

        // Find position to insert new key
        int pos = IndexSearch.search( cursor, bTreeNode, primKey );

        // Arrays to temporarily store keys and children in sorted order.
        byte[] allKeysIncludingNewPrimKey = readRecordsWithInsertRecordInPosition( cursor, primKey, pos, keyCount+1,
                BTreeNode.SIZE_KEY, bTreeNode.keyOffset( 0 ) );
        byte[] allChildrenIncludingNewRightChild = readRecordsWithInsertRecordInPosition( cursor,
                new long[]{newRightChild}, pos+1, keyCount+2, BTreeNode.SIZE_CHILD, bTreeNode.childOffset( 0 ) );


        int keyCountAfterInsert = keyCount + 1;
        int middle = keyCountAfterInsert / 2; // Floor division

        int arrayOffset;
        if ( pos < middle )
        {
            // Write keys to left
            arrayOffset = pos * BTreeNode.SIZE_KEY;
            cursor.setOffset( bTreeNode.keyOffset( pos ) );
            cursor.putBytes( allKeysIncludingNewPrimKey, arrayOffset, (middle - pos) * BTreeNode.SIZE_KEY );

            cursor.setOffset( bTreeNode.childOffset( pos + 1 ) );
            arrayOffset = (pos + 1) * BTreeNode.SIZE_CHILD;
            cursor.putBytes( allChildrenIncludingNewRightChild, arrayOffset, (middle - pos) * BTreeNode.SIZE_VALUE );
        }

        bTreeNode.setKeyCount( cursor, middle );

        // Everything in left node should now be updated
        // Ready to start with right node

        cursor.next( newRight );

        // Initialize
        bTreeNode.initializeInternal( cursor );

        // Siblings
        bTreeNode.setRightSibling( cursor, oldRight );
        bTreeNode.setLeftSibling( cursor, fullNode );

        // Keys
        arrayOffset = (middle + 1) * BTreeNode.SIZE_KEY; // NOTE: (middle + 1) don't include middle
        cursor.setOffset( bTreeNode.keyOffset( 0 ) );
        cursor.putBytes( allKeysIncludingNewPrimKey, arrayOffset, allKeysIncludingNewPrimKey.length - arrayOffset );

        // Children
        arrayOffset = (middle + 1) * BTreeNode.SIZE_CHILD;
        cursor.setOffset( bTreeNode.childOffset( 0 ) );
        cursor.putBytes( allChildrenIncludingNewRightChild, arrayOffset,
                allChildrenIncludingNewRightChild.length - arrayOffset );

        // Key count
        // NOTE: Not keyCountAfterInsert because middle key is not kept at this level
        bTreeNode.setKeyCount( cursor, keyCount - middle );

        // Extract middle key (prim key)
        arrayOffset = middle * BTreeNode.SIZE_KEY;
        ByteBuffer buffer = ByteBuffer.wrap( allKeysIncludingNewPrimKey, arrayOffset, BTreeNode.SIZE_KEY );
        long[] newPrimKey = new long[2];
        newPrimKey[0] = buffer.getLong();
        newPrimKey[1] = buffer.getLong();

        // Populate split result
        SplitResult split = new SplitResult();
        split.primKey = newPrimKey;
        split.left = fullNode;
        split.right = newRight;

        // Move cursor back to left
        cursor.next( fullNode );

        return split;
    }

    /**
     * Leaves cursor at same page as when called. No guaranties on offset.
     *
     * Split in leaf node caused by an insertion of key and value
     *
     * @param cursor        {@link org.neo4j.io.pagecache.PageCursor} pinned to page containing leaf node targeted for
     *                      insertion.
     * @param key           key to be inserted
     * @param value         value to be associated with key
     * @return              {@link SplitResult} from insert to be used caller.
     * @throws IOException  on cursor failure
     */
    private SplitResult insertInLeaf( PageCursor cursor, long[] key, long[] value ) throws IOException
    {
        int keyCount = bTreeNode.keyCount( cursor );

        if ( keyCount < bTreeNode.leafMaxKeyCount() )
        {
            // No overflow, insert key and value
            int pos = IndexSearch.search( cursor, bTreeNode, key );

            // Insert and move keys
            int tmpLength = bTreeNode.keysFromTo( cursor, pos, keyCount, tmp );
            bTreeNode.setKeyAt( cursor, key, pos );
            bTreeNode.setKeysAt( cursor, tmp, pos + 1, tmpLength );

            // Insert and move values
            tmpLength = bTreeNode.valuesFromTo( cursor, pos, keyCount, tmp );
            bTreeNode.setValueAt( cursor, value, pos );
            bTreeNode.setValuesAt( cursor, tmp, pos + 1, tmpLength );

            // Increase key count
            bTreeNode.setKeyCount( cursor, keyCount + 1 );

            return null; // No split has occurred
        }

        // Overflow, split leaf
        return splitLeaf( cursor, key, value, keyCount );
    }

    /**
     * Leaves cursor at same page as when called. No guaranties on offset.
     * Cursor is expected to be pointing to full leaf.
     * @param cursor        cursor pointing into full (left) leaf that should be split in two.
     * @param newKey        key to be inserted
     * @param newValue      value to be inserted (in association with key)
     * @return              {@link SplitResult} with necessary information to inform parent
     * @throws IOException  if cursor.next( newRight ) fails
     */
    private SplitResult splitLeaf( PageCursor cursor, long[] newKey, long[] newValue, int keyCount ) throws IOException
    {
        // To avoid moving cursor between pages we do all operations on left node first.
        // Save data that needs transferring and then add it to right node.

        // UPDATE SIBLINGS
        //
        // Before split
        // newRight is leaf node to be inserted between left and oldRight
        // [left] -> [oldRight]
        //
        //     [newRight]
        //
        // After split
        // [left] -> [newRight] -> [oldRight]
        //

        long left = cursor.getCurrentPageId();
        long newRight = idProvider.acquireNewId();

        long oldRight = bTreeNode.rightSibling( cursor );
        bTreeNode.setRightSibling( cursor, newRight );

        // BALANCE KEYS AND VALUES
        // Two different scenarios
        // Before split
        // [key1]<=[key2]<=[key3]<=[key4]<=[key5]   (<= greater than or equal to)
        //                           ^
        //                           |
        //                      pos  |
        // [newKey] -----------------
        //
        // After split
        // Left
        // [key1]<=[key2]<=[key3]
        //
        // Right
        // [newKey][key4][key5]
        //
        // Before split
        // [key1]<=[key2]<=[key3]<=[key4]<=[key5]   (<= greater than or equal to)
        //   ^
        //   | pos
        //   |
        // [newKey]
        //
        // After split
        // Left
        // [newKey]<=[key1]<=[key2]
        //
        // Right
        // [key3][key4][key5]
        //

        // Position where newKey / newValue is to be inserted
        int pos = IndexSearch.search( cursor, bTreeNode, newKey );

        // arrays to temporarily store all keys and values
        byte[] allKeysIncludingNewKey = readRecordsWithInsertRecordInPosition( cursor, newKey, pos,
                bTreeNode.leafMaxKeyCount() + 1, BTreeNode.SIZE_KEY, bTreeNode.keyOffset( 0 ) );
        byte[] allValuesIncludingNewValue = readRecordsWithInsertRecordInPosition( cursor, newValue, pos,
                bTreeNode.leafMaxKeyCount() + 1, BTreeNode.SIZE_VALUE, bTreeNode.valueOffset( 0 ) );

        int keyCountAfterInsert = keyCount + 1;
        int middle = keyCountAfterInsert / 2; // Floor division

        // allKeysIncludingNewKey should now contain all keys in sorted order and
        // allValuesIncludingNewValue should now contain all values in same order as corresponding keys
        // and are ready to be split between left and newRight.

        // If pos < middle. Write shifted values to left node. Else, don't write anything.
        if ( pos < middle )
        {
            int arrayOffset = pos * BTreeNode.SIZE_KEY;
            cursor.setOffset( bTreeNode.keyOffset( pos ) );
            cursor.putBytes( allKeysIncludingNewKey, arrayOffset, (middle - pos) * BTreeNode.SIZE_KEY );

            cursor.setOffset( bTreeNode.valueOffset( pos ) );
            arrayOffset = pos * BTreeNode.SIZE_VALUE;
            cursor.putBytes( allValuesIncludingNewValue, arrayOffset, (middle - pos) * BTreeNode.SIZE_VALUE );
        }

        // Key count
        bTreeNode.setKeyCount( cursor, middle );

        // We now have everything we need to start working on newRight
        // and everything that needs to be updated in left has been so.

        // Initialize
        cursor.next( newRight );
        bTreeNode.initializeLeaf( cursor );

        // Siblings
        bTreeNode.setRightSibling( cursor, oldRight );
        bTreeNode.setLeftSibling( cursor, left );

        // Keys
        int arrayOffset = middle * BTreeNode.SIZE_KEY;
        cursor.setOffset( bTreeNode.keyOffset( 0 ) );
        cursor.putBytes( allKeysIncludingNewKey, arrayOffset, allKeysIncludingNewKey.length - arrayOffset );

        // Values
        arrayOffset = middle * BTreeNode.SIZE_VALUE;
        cursor.setOffset( bTreeNode.valueOffset( 0 ) );
        cursor.putBytes( allValuesIncludingNewValue, arrayOffset, allValuesIncludingNewValue.length - arrayOffset );

        // Key count
        bTreeNode.setKeyCount( cursor, keyCountAfterInsert - middle );

        SplitResult split = new SplitResult();
        split.left = left;
        split.right = newRight;
        split.primKey = bTreeNode.keyAt( cursor, new long[2], 0 );

        // Move cursor back to left
        cursor.next( left );

        return split;
    }

    /**
     * Leaves cursor on same page as when called. No guaranties on offset.
     *
     * Create a byte[] with totalNumberOfRecords of recordSize from cursor reading from baseRecordOffset
     * with newRecord inserted in insertPosition, with the following records shifted to the right.
     *
     * Simply: Records of size recordSize that can be read from offset baseRecordOffset in page pinned by cursor has
     * some ordering. This ordering is preserved with new record inserted in insertPosition in the returned byte[],
     * NOT in the page.
     *
     * @param cursor                {@link org.neo4j.io.pagecache.PageCursor} pinned to page to read records from
     * @param newRecord             new record to be inserted in insertPosition in returned byte[]
     * @param insertPosition        position of newRecord. 0 being before all other records,
     *                              (totalNumberOfRecords - 1) being after all other records
     * @param totalNumberOfRecords  the total number of records to be contained in returned byte[], including newRecord
     * @param recordSize            the size in number of bytes of one record
     * @param baseRecordOffset      the offset from where cursor should start read records
     * @return                      a byte[] with records in same order as read by cursor with newRecord in position
     *                              insertPosition, that is data[ insertPosition * recordSize ]
     */
    private byte[] readRecordsWithInsertRecordInPosition( PageCursor cursor, long[] newRecord, int insertPosition,
            int totalNumberOfRecords, int recordSize, int baseRecordOffset )
    {
        byte[] allRecordsIncludingNewRecord = new byte[(totalNumberOfRecords) * recordSize];

        // First read all records

        // Read all records on previous to insertPosition
        cursor.setOffset( baseRecordOffset );
        cursor.getBytes( allRecordsIncludingNewRecord, 0, insertPosition * recordSize );

        // Read newRecord
        ByteBuffer buffer = ByteBuffer.wrap( allRecordsIncludingNewRecord, insertPosition * recordSize, recordSize );
        for ( int i = 0; i < newRecord.length; i++ )
        {
            buffer.putLong( newRecord[i] );
        }

        // Read all records following insertPosition
        cursor.setOffset( baseRecordOffset + insertPosition * recordSize );
        cursor.getBytes( allRecordsIncludingNewRecord, (insertPosition + 1) * recordSize,
                ((totalNumberOfRecords - 1) - insertPosition) * recordSize );
        return allRecordsIncludingNewRecord;
    }
}
