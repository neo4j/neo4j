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

import java.io.IOException;
import java.util.Comparator;
import org.neo4j.index.Modifier;
import org.neo4j.index.ValueAmender;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Integer.max;
import static java.lang.Integer.min;

/**
 * Implementation of the insert algorithm in this B+ tree including split.
 * Takes storage format into consideration.
 *
 * General approach to have concurrent readers see correct tree structure
 * Changes involved in splitting a leaf (L = leaf page to split, R` = L's current right sibling):
 * <ol>
 * <li>Acquire new page id R</li>
 * <li>Copy "right-hand" keys/values to R and set key count</li>
 * <li>Set L's right sibling to R</li>
 * <li>Set key count of L to new "left-hand" key count</li>
 * <li>Write new key/values in L</li>
 * </ol>
 * Reader may have to compensate its reading to cope with following scenario
 * (key/value abstracted into E for simplicity, right bracket ends by keyCount):
 * SCENARIO1 (new key ends up in right leaf)
 * <pre>
 * - L[E1,E2,E4,E5]
 *           ^
 *   Reader have read E1-E2 and is about to read E4
 *
 * - Split happens where E3 is inserted and the leaf needs to be split, which modifies the tree into:
 *   L[E1,E2] -> R[E3,E4,E5]
 *
 *   During this split, reader could see this state:
 *   L[E1,E2,E4,E5] -> R[E3,E4,E5]
 *           ^  ^           x  x
 *   Reader will need to ignore lower keys than already seen (TODO how to do in non-unique index?)
 * </pre>
 * SCENARIO2 (new key ends up in left leaf)
 * <pre>
 * - L[E1,E2,E4,E5,E6]
 *           ^
 *   Reader have read E1-E2 and is about to read E4
 *
 * - Split happens where E3 is inserted and the leaf needs to be split, which modifies the tree into:
 *   L[E1,E2,E3] -> R[E4,E5,E6]
 *
 *   There's no bad intermediate state
 * </pre>
 *
 * @param <KEY> type of internal/leaf keys
 * @param <VALUE> type of leaf values
 */
public class IndexModifier<KEY,VALUE>
{
    private final IdProvider idProvider;
    private final TreeNode<KEY,VALUE> bTreeNode;
    private final byte[] tmp;
    private final byte[] tmp2;
    private final byte[] tmp3;
    private final SplitResult<KEY> internalSplitResult = new SplitResult<>();
    private final SplitResult<KEY> leafSplitResult = new SplitResult<>();
    private final Layout<KEY,VALUE> layout;
    private final KEY readKey;
    private final VALUE readValue;
    // Cache during current modifications for order of items in tree nodes
    private final Object[] orders = new Object[20];

    public IndexModifier( IdProvider idProvider, TreeNode<KEY,VALUE> bTreeNode, Layout<KEY,VALUE> layout )
    {
        this.idProvider = idProvider;
        this.bTreeNode = bTreeNode;
        this.layout = layout;
        this.tmp = new byte[max( bTreeNode.internalMaxKeyCount(), bTreeNode.leafMaxKeyCount() ) *
                            max( layout.keySize(), layout.valueSize() )];
        this.tmp2 = new byte[tmp.length + layout.keySize()];
        this.tmp3 = new byte[tmp.length + layout.valueSize()];
        this.internalSplitResult.primKey = layout.newKey();
        this.leafSplitResult.primKey = layout.newKey();
        this.readKey = layout.newKey();
        this.readValue = layout.newValue();
        for ( int i = 0; i < orders.length; i++ )
        {
            orders[i] = bTreeNode.newOrder();
        }
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * @param cursor        {@link org.neo4j.io.pagecache.PageCursor} pinned to page where insertion is to be done.
     * @param key           key to be inserted
     * @param value         value to be associated with key
     * @param amender       {@link ValueAmender} for deciding what to do with existing keys
     * @param options       options for this insert
     * @return              {@link SplitResult} from insert to be used caller.
     * @throws IOException  on cursor failure
     */
    public SplitResult<KEY> insert( PageCursor cursor, KEY key, VALUE value, ValueAmender<VALUE> amender,
            Modifier.Options options ) throws IOException
    {
        return insert( cursor, key, value, amender, options, 0 );
    }

    private SplitResult<KEY> insert( PageCursor cursor, KEY key, VALUE value, ValueAmender<VALUE> amender,
            Modifier.Options options, int level ) throws IOException
    {
        if ( bTreeNode.isLeaf( cursor ) )
        {
            return insertInLeaf( cursor, key, value, amender, options, level );
        }

        int keyCount = bTreeNode.keyCount( cursor );
        bTreeNode.getOrder( cursor, orders[level] );
        int pos = positionOf( search( cursor, key, orders[level] ) );

        long currentId = cursor.getCurrentPageId();
        cursor.next( bTreeNode.childAt( cursor, pos, orders[level] ) );

        SplitResult<KEY> split = insert( cursor, key, value, amender, options, level + 1 );

        cursor.next( currentId );

        if ( split != null )
        {
            // update left child (key remains and pos is known by this very method)
            if ( Knobs.SPLIT_KEEPS_SOURCE_INTACT )
            {
                bTreeNode.setChildAt( cursor, split.left, pos, orders[level] );
            }
            return insertInInternal( cursor, currentId, keyCount, split.primKey, split.right, options, level );
        }
        return null;
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
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
    private SplitResult<KEY> insertInInternal( PageCursor cursor, long nodeId, int keyCount,
            KEY primKey, long rightChild, Modifier.Options options, int level ) throws IOException
    {
        if ( keyCount < bTreeNode.internalMaxKeyCount() )
        {
            // No overflow
            int pos = positionOf( search( cursor, primKey, orders[level] ) );

            bTreeNode.insertKeyAt( cursor, primKey, pos, orders[level], tmp );
            // NOTE pos+1 since we never insert a new child before child(0) because its key is really
            // the one from the parent.
            bTreeNode.insertChildAt( cursor, rightChild, pos + 1, orders[level], tmp );

            // Increase key count
            bTreeNode.setKeyCount( cursor, keyCount + 1 );

            return null;
        }

        // Overflow
        return splitInternal( cursor, nodeId, primKey, rightChild, keyCount, options, level );
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
    private SplitResult<KEY> splitInternal( PageCursor cursor, long fullNode, KEY primKey, long newRightChild,
            int keyCount, Modifier.Options options, int level ) throws IOException
    {
        long current = cursor.getCurrentPageId();
        long oldLeft = bTreeNode.leftSibling( cursor );
        long newLeft = Knobs.SPLIT_KEEPS_SOURCE_INTACT ? idProvider.acquireNewId() : current;
        long oldRight = bTreeNode.rightSibling( cursor );
        long newRight = idProvider.acquireNewId();

        // Find position to insert new key
        int pos = positionOf( search( cursor, primKey, orders[level] ) );

        // Arrays to temporarily store keys and children in sorted order.
        bTreeNode.readKeysWithInsertRecordInPosition( cursor,
                c -> layout.writeKey( c, primKey ), pos, keyCount+1, tmp2 );
        bTreeNode.readChildrenWithInsertRecordInPosition( cursor,
                c -> bTreeNode.writeChild( c, newRightChild ), pos+1, keyCount+2, tmp3 );

        int keyCountAfterInsert = keyCount + 1;
        int middle = middle( keyCountAfterInsert, options.splitLeftChildSize() );

        SplitResult<KEY> split = internalSplitResult;
        split.left = newLeft;
        split.right = newRight;

        {   // Update new right
            // NOTE: don't include middle
            cursor.next( newRight );
            bTreeNode.initializeInternal( cursor );
            bTreeNode.setRightSibling( cursor, oldRight );
            bTreeNode.setLeftSibling( cursor, newLeft );
            bTreeNode.writeKeys( cursor, tmp2, middle + 1, 0, keyCountAfterInsert - (middle + 1) );
            bTreeNode.writeChildren( cursor, tmp3, middle + 1, 0,
                    keyCountAfterInsert - middle /*there's one more child than key to copy*/ );
            bTreeNode.setKeyCount( cursor, keyCount - middle );

            // Extract middle key (prim key)
            int arrayOffset = middle * bTreeNode.keySize();
            PageCursor buffer = ByteArrayPageCursor.wrap( tmp2, arrayOffset, bTreeNode.keySize() );

            // Populate split result
            layout.readKey( buffer, split.primKey );
        }

        if ( Knobs.SPLIT_KEEPS_SOURCE_INTACT )
        {   // Update new left
            cursor.next( newLeft );
            bTreeNode.initializeInternal( cursor );
            bTreeNode.setRightSibling( cursor, newRight );
            bTreeNode.setLeftSibling( cursor, oldLeft );
            bTreeNode.writeKeys( cursor, tmp2, 0, 0, middle );
            bTreeNode.writeChildren( cursor, tmp3, 0, 0, middle + 1 );
            bTreeNode.setKeyCount( cursor, middle );

            if ( bTreeNode.isNode( oldRight ) )
            {   // Update old right
                cursor.next( oldRight );
                bTreeNode.setLeftSibling( cursor, newRight );
            }

            if ( bTreeNode.isNode( oldLeft ) )
            {   // Update old left
                cursor.next( oldLeft );
                bTreeNode.setRightSibling( cursor, newLeft );
            }

            // Move back to current TODO do we really need this?
            cursor.next( current );
        }
        else
        {   // Update left node
            // Move cursor back to left
            cursor.next( fullNode );
            bTreeNode.setKeyCount( cursor, middle );
            if ( pos < middle )
            {
                // Write keys to left
                int arrayOffset = pos * bTreeNode.keySize();
                cursor.setOffset( bTreeNode.keyOffset( pos ) );
                cursor.putBytes( tmp2, arrayOffset, (middle - pos) * bTreeNode.keySize() );

                cursor.setOffset( bTreeNode.childOffset( pos + 1 ) );
                arrayOffset = (pos + 1) * bTreeNode.childSize();
                cursor.putBytes( tmp3, arrayOffset, (middle - pos) * bTreeNode.childSize() );
            }

            bTreeNode.setRightSibling( cursor, newRight );
        }

        return split;
    }

    private int middle( int keyCountAfterInsert, float splitLeftChildSize )
    {
        int middle = (int) (keyCountAfterInsert * splitLeftChildSize); // Floor division
        middle = max( 1, middle );
        middle = min( keyCountAfterInsert - 1, middle );
        return middle;
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     *
     * Split in leaf node caused by an insertion of key and value
     *
     * @param cursor        {@link org.neo4j.io.pagecache.PageCursor} pinned to page containing leaf node targeted for
     *                      insertion.
     * @param key           key to be inserted
     * @param value         value to be associated with key
     * @param amender       {@link ValueAmender} for deciding what to do with existing keys
     * @param options       options for this insert
     * @param level
     * @return              {@link SplitResult} from insert to be used caller.
     * @throws IOException  on cursor failure
     */
    private SplitResult<KEY> insertInLeaf( PageCursor cursor, KEY key, VALUE value, ValueAmender<VALUE> amender,
            Modifier.Options options, int level ) throws IOException
    {
        int keyCount = bTreeNode.keyCount( cursor );
        bTreeNode.getOrder( cursor, orders[level] );
        int search = search( cursor, key, orders[level] );
        int pos = positionOf( search );
        if ( isHit( search ) )
        {
            // this key already exists, what shall we do? ask the amender
            bTreeNode.valueAt( cursor, readValue, pos-1, orders[level] );
            VALUE amendedValue = amender.amend( readValue, value );
            if ( amendedValue != null )
            {
                // simple, just write the amended value right in there
                bTreeNode.setValueAt( cursor, amendedValue, pos-1, orders[level] );
                return null; // No split has occurred
            }
            // else fall-through to normal insert
        }

        if ( keyCount < bTreeNode.leafMaxKeyCount() )
        {
            // No overflow, insert key and value
            bTreeNode.insertKeyAt( cursor, key, pos, orders[level], tmp );
            bTreeNode.insertValueAt( cursor, value, pos, orders[level], tmp );
            bTreeNode.setKeyCount( cursor, keyCount + 1 );

            return null; // No split has occurred
        }

        // Overflow, split leaf
        return splitLeaf( cursor, key, value, amender, keyCount, options, level );
    }

    private static int positionOf( int searchResult )
    {
        int pos = searchResult & 0x7FFFFFFF;
        return pos == 0x7FFFFFFF ? -1 : pos;
    }

    private static boolean isHit( int searchResult )
    {
        return (searchResult & 0x80000000) != 0;
    }

    private static int searchResult( int pos, boolean hit )
    {
        return (pos & 0x7FFFFFFF) | ((hit ? 1 : 0) << 31);
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * Cursor is expected to be pointing to full leaf.
     * @param cursor        cursor pointing into full (left) leaf that should be split in two.
     * @param newKey        key to be inserted
     * @param newValue      value to be inserted (in association with key)
     * @param amender       {@link ValueAmender} for deciding what to do with existing keys
     * @param keyCount      number of keys in this leaf (it was already read anyway)
     * @param options       options for this insert
     * @return              {@link SplitResult} with necessary information to inform parent
     * @throws IOException  if cursor.next( newRight ) fails
     */
    private SplitResult<KEY> splitLeaf( PageCursor cursor, KEY newKey, VALUE newValue, ValueAmender<VALUE> amender,
            int keyCount, Modifier.Options options, int level ) throws IOException
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

        long current = cursor.getCurrentPageId();
        long oldLeft = bTreeNode.leftSibling( cursor );
        long newLeft = Knobs.SPLIT_KEEPS_SOURCE_INTACT ? idProvider.acquireNewId() : current;
        long oldRight = bTreeNode.rightSibling( cursor );
        long newRight = idProvider.acquireNewId();

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

        // CONCURRENCY
        // To have readers see correct state at all times, the order of updates must be:
        // 1. Acquire new page id R
        // 2. Copy "right-hand" keys/values to R and set key count
        // 3. Set L's right sibling to R
        // 4. Set key count of L to new "left-hand" key count
        // 5. Write new key/values into L

        // Position where newKey / newValue is to be inserted
        int pos = positionOf( search( cursor, newKey, orders[level] ) );

        // arrays to temporarily store all keys and values
        bTreeNode.readKeysWithInsertRecordInPosition( cursor,
                c -> layout.writeKey( c, newKey ), pos, bTreeNode.leafMaxKeyCount() + 1, tmp2 );
        bTreeNode.readValuesWithInsertRecordInPosition( cursor,
                c -> layout.writeValue( c, newValue ), pos, bTreeNode.leafMaxKeyCount() + 1, tmp3 );

        int keyCountAfterInsert = keyCount + 1;
        int middle = middle( keyCountAfterInsert, options.splitLeftChildSize() );

        // allKeysIncludingNewKey should now contain all keys in sorted order and
        // allValuesIncludingNewValue should now contain all values in same order as corresponding keys
        // and are ready to be split between left and newRight.

        // We now have everything we need to start working on newRight
        // and everything that needs to be updated in left has been so.

        SplitResult<KEY> split = leafSplitResult;
        split.left = newLeft;
        split.right = newRight;

        if ( middle == pos )
        {
            layout.copyKey( newKey, split.primKey );
        }
        else
        {
            bTreeNode.keyAt( cursor, split.primKey, pos < middle ? middle - 1 : middle, orders[level] );
        }

        {   // Update new right
            cursor.next( newRight );
            bTreeNode.initializeLeaf( cursor );
            bTreeNode.setRightSibling( cursor, oldRight );
            bTreeNode.setLeftSibling( cursor, newLeft );
            bTreeNode.writeKeys( cursor, tmp2, middle, 0, keyCountAfterInsert - middle );
            bTreeNode.writeValues( cursor, tmp3, middle, 0, keyCountAfterInsert - middle );
            bTreeNode.setKeyCount( cursor, keyCountAfterInsert - middle );
        }

        if ( Knobs.SPLIT_KEEPS_SOURCE_INTACT )
        {   // Update new left
            cursor.next( newLeft );
            bTreeNode.initializeLeaf( cursor );
            bTreeNode.setRightSibling( cursor, newRight );
            bTreeNode.setLeftSibling( cursor, oldLeft );
            bTreeNode.writeKeys( cursor, tmp2, 0, 0, middle );
            bTreeNode.writeValues( cursor, tmp3, 0, 0, middle );
            bTreeNode.setKeyCount( cursor, middle );

            if ( bTreeNode.isNode( oldRight ) )
            {   // Update old right
                cursor.next( oldRight );
                bTreeNode.setLeftSibling( cursor, newRight );
            }

            if ( bTreeNode.isNode( oldLeft ) )
            {   // Update old left
                cursor.next( oldLeft );
                bTreeNode.setRightSibling( cursor, newLeft );
            }

            cursor.next( current );
        }
        else
        {   // Update left child
            cursor.next( current );
            bTreeNode.setKeyCount( cursor, middle );
            // If pos < middle. Write shifted values to left node. Else, don't write anything.
            if ( pos < middle )
            {
                bTreeNode.writeKeys( cursor, tmp2, pos, pos, middle - pos );
                bTreeNode.writeValues( cursor, tmp3, pos, pos, middle - pos );
            }
            bTreeNode.setRightSibling( cursor, newRight );
        }

        return split;
    }

    /**
     * Leaves cursor on same page as when called. No guarantees on offset.
     *
     * Search for keyAtPos such that key <= keyAtPos. Return first position of keyAtPos (not offset),
     * or key count if no such key exist.
     *
     * On insert, key should be inserted at pos.
     * On seek in internal, child at pos should be followed from internal node.
     * On seek in leaf, value at pos is correct if keyAtPos is equal to key.
     *
     * Simple implementation, linear search.
     *
     * //TODO: Implement binary search
     *
     * @param cursor    {@link PageCursor} pinned to page with node (internal or leaf does not matter)
     * @param key       long[] of length 2 where key[0] is id and key[1] is property value
     * @param orders2
     * @return          first position i for which Node.KEY_COMPARATOR.compare( key, Node.keyAt( i ) <= 0;
     */
    private int search( PageCursor cursor, KEY key, Object order )
    {
        int keyCount = bTreeNode.keyCount( cursor );

        if ( keyCount == 0 )
        {
            return searchResult( 0, false );
        }

        int lower = 0;
        int higher = keyCount-1;
        int pos;
        boolean hit = false;

        // Compare key with lower and higher and sort out special cases
        Comparator<KEY> comparator = bTreeNode.keyComparator();
        int comparedHigher = comparator.compare( key, bTreeNode.keyAt( cursor, readKey, higher, order ) );
        if ( comparedHigher >= 0 )
        {
            pos = keyCount;
            if ( comparedHigher == 0 )
            {
                hit = true;
            }
        }
        else if ( comparator.compare( key, bTreeNode.keyAt( cursor, readKey, lower, order ) ) < 0 )
        {
            pos = 0;
        }
        else
        {
            // Start binary search
            // If key <= keyAtPos -> move higher to pos
            // If key > keyAtPos -> move lower to pos+1
            // Terminate when lower == higher
            while ( lower < higher )
            {
                pos = (lower + higher) / 2;
                switch ( comparator.compare( key, bTreeNode.keyAt( cursor, readKey, pos, order ) ) )
                {
                case -1:
                    higher = pos;
                    break;
                case 0:
                    hit = true;
                    // fall-through
                case 1:
                    lower = pos+1;
                    break;
                default:
                    throw new IllegalArgumentException( "Unexpected compare value" );
                }
            }
            if ( lower != higher )
            {
                throw new IllegalStateException( "Something went terribly wrong. The binary search terminated in an " +
                                                 "unexpected way." );
            }
            pos = lower;
        }
        return searchResult( pos, hit );
    }

    public VALUE remove( PageCursor cursor, KEY key ) throws IOException
    {
        return remove( cursor, key, 0 );
    }

    private VALUE remove( PageCursor cursor, KEY key, int level ) throws IOException
    {
        if ( bTreeNode.isLeaf( cursor ) )
        {
            return removeFromLeaf( cursor, key, level + 1 );
        }

        bTreeNode.getOrder( cursor, orders[level] );
        int search = search( cursor, key, orders[level] );
        int pos = positionOf( search );
        cursor.next( bTreeNode.childAt( cursor, pos, orders[level] ) );
        return remove( cursor, key, level + 1 );
    }

    private VALUE removeFromLeaf( PageCursor cursor, KEY key, int level )
    {
        int keyCount = bTreeNode.keyCount( cursor );
        bTreeNode.getOrder( cursor, orders[level] );

        // No overflow, insert key and value
        int search = search( cursor, key, orders[level] );
        int pos = positionOf( search );
        boolean hit = isHit( search );
        if ( !hit )
        {
            return null;
        }
        pos--;

        // Remove key/value
        bTreeNode.removeKeyAt( cursor, pos, orders[level], tmp );
        bTreeNode.valueAt( cursor, readValue, pos, orders[level] );
        bTreeNode.removeValueAt( cursor, pos, orders[level], tmp );

        // Decrease key count
        bTreeNode.setKeyCount( cursor, keyCount - 1 );

        return readValue;
    }
}
