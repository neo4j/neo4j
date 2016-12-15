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

import org.neo4j.index.IndexWriter;
import org.neo4j.index.ValueMerger;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static org.neo4j.index.gbptree.KeySearch.isHit;
import static org.neo4j.index.gbptree.KeySearch.positionOf;
import static org.neo4j.index.gbptree.KeySearch.search;
import static org.neo4j.index.gbptree.PageCursorUtil.goTo;

/**
 * Implementation of GB+ tree insert/remove algorithms.
 * <p>
 * Changes involved in splitting a leaf (L = leaf page to split, R` = L's current right sibling):
 * <ol>
 * <li>Acquire new page id R</li>
 * <li>Copy "right-hand" keys/values to R and set key count</li>
 * <li>Set L's right sibling to R</li>
 * <li>Set key count of L to new "left-hand" key count</li>
 * <li>Write new key/values in L</li>
 * </ol>
 * <p>
 * Reader concurrent with writer may have to compensate its reading to cope with following scenario
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
 *   Reader will need to ignore lower keys already seen, assuming unique keys
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
class InternalTreeLogic<KEY,VALUE>
{
    private final IdProvider idProvider;
    private final TreeNode<KEY,VALUE> bTreeNode;
    private final byte[] tmpForKeys;
    private final byte[] tmpForValues;
    private final byte[] tmpForChildren;
    private final Layout<KEY,VALUE> layout;
    private final KEY readKey;
    private final VALUE readValue;

    InternalTreeLogic( IdProvider idProvider, TreeNode<KEY,VALUE> bTreeNode, Layout<KEY,VALUE> layout )
    {
        this.idProvider = idProvider;
        this.bTreeNode = bTreeNode;
        this.layout = layout;
        int maxKeyCount = max( bTreeNode.internalMaxKeyCount(), bTreeNode.leafMaxKeyCount() );
        this.tmpForKeys = new byte[(maxKeyCount + 1) * layout.keySize()];
        this.tmpForValues = new byte[(maxKeyCount + 1) * layout.valueSize()];
        this.tmpForChildren = new byte[(maxKeyCount + 2) * bTreeNode.childSize()];
        this.readKey = layout.newKey();
        this.readValue = layout.newValue();
    }

    /**
     * Insert {@code key} and associate it with {@code value} if {@code key} does not already exist in
     * tree.
     * <p>
     * If {@code key} already exists in tree, {@code valueMerger} will be used to decide how to merge existing value
     * with {@code value}.
     * <p>
     * Insert may cause structural changes in the tree in form of splits and or new generation of nodes being created.
     * Note that a split in a leaf can propagate all the way up to root node.
     * <p>
     * Structural changes in tree that need to propagate to the level above will be reported through the provided
     * {@link StructurePropagation} by overwriting state. This is safe because structure changes happens one level
     * at the time.
     * {@link StructurePropagation} is provided from outside to minimize garbage.
     * <p>
     * When this method returns, {@code structurePropagation} will be populated with information about split or new
     * gen version of root. This needs to be handled by caller.
     * <p>
     * Leaves cursor at same page as when called. No guarantees on offset.
     *
     * @param cursor {@link PageCursor} pinned to page where insertion is to be done.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param key key to be inserted
     * @param value value to be associated with key
     * @param valueMerger {@link ValueMerger} for deciding what to do with existing keys
     * @param options options for this insert
     * @param stableGeneration stable generation, i.e. generations <= this generation are considered stable.
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @throws IOException on cursor failure
     */
    void insert( PageCursor cursor, StructurePropagation<KEY> structurePropagation, KEY key, VALUE value,
            ValueMerger<VALUE> valueMerger, IndexWriter.Options options,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        if ( bTreeNode.isLeaf( cursor ) )
        {
            insertInLeaf( cursor, structurePropagation, key, value, valueMerger, options,
                    stableGeneration, unstableGeneration );
            return;
        }

        int keyCount = bTreeNode.keyCount( cursor );
        int searchResult = search( cursor, bTreeNode, key, readKey, keyCount );
        int pos = positionOf( searchResult );
        if ( isHit( searchResult ) )
        {
            pos++;
        }

        long currentId = cursor.getCurrentPageId();
        long childId = bTreeNode.childAt( cursor, pos, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( childId, false );

        bTreeNode.goTo( cursor, "child", childId );

        insert( cursor, structurePropagation, key, value, valueMerger, options, stableGeneration, unstableGeneration );

        bTreeNode.goTo( cursor, "parent", currentId );

        if ( structurePropagation.hasNewGen )
        {
            structurePropagation.hasNewGen = false;
            bTreeNode.setChildAt( cursor, structurePropagation.left, pos, stableGeneration, unstableGeneration );
        }
        if ( structurePropagation.hasSplit )
        {
            structurePropagation.hasSplit = false;
            insertInInternal( cursor, structurePropagation, keyCount, structurePropagation.primKey,
                    structurePropagation.right, options, stableGeneration, unstableGeneration );
        }
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * <p>
     * Insertion in internal is always triggered by a split in child.
     * The result of a split is a primary key that is sent upwards in the b+tree and the newly created right child.
     *
     * @param cursor {@link PageCursor} pinned to page containing internal node,
     * current node
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param keyCount the key count of current node
     * @param primKey the primary key to be inserted
     * @param rightChild the right child of primKey
     * @throws IOException on cursor failure
     */
    private void insertInInternal( PageCursor cursor, StructurePropagation<KEY> structurePropagation, int keyCount,
            KEY primKey, long rightChild, IndexWriter.Options options, long stableGeneration, long unstableGeneration )
            throws IOException
    {
        createUnstableVersionIfNeeded( cursor, structurePropagation, stableGeneration, unstableGeneration );
        if ( keyCount < bTreeNode.internalMaxKeyCount() )
        {
            // No overflow
            int pos = positionOf( search( cursor, bTreeNode, primKey, readKey, keyCount ) );

            bTreeNode.insertKeyAt( cursor, primKey, pos, keyCount );
            // NOTE pos+1 since we never insert a new child before child(0) because its key is really
            // the one from the parent.
            bTreeNode.insertChildAt( cursor, rightChild, pos + 1, keyCount, stableGeneration, unstableGeneration );

            // Increase key count
            bTreeNode.setKeyCount( cursor, keyCount + 1 );

            return;
        }

        // Overflow
        splitInternal( cursor, structurePropagation, primKey, rightChild, keyCount, options,
                stableGeneration, unstableGeneration );
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * <p>
     * Split in internal node caused by an insertion of primKey and newRightChild
     *
     * @param cursor {@link PageCursor} pinned to page containing internal node, fullNode.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param primKey primary key to be inserted, causing the split
     * @param newRightChild right child of primKey
     * @param keyCount key count for fullNode
     * @throws IOException on cursor failure
     */
    private void splitInternal( PageCursor cursor, StructurePropagation<KEY> structurePropagation, KEY primKey,
            long newRightChild, int keyCount, IndexWriter.Options options,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        long current = cursor.getCurrentPageId();
        long oldRight = bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( oldRight, true );
        long newRight = idProvider.acquireNewId( stableGeneration, unstableGeneration );

        // Find position to insert new key
        int pos = positionOf( search( cursor, bTreeNode, primKey, readKey, keyCount ) );

        // Arrays to temporarily store keys and children in sorted order.
        bTreeNode.readKeysWithInsertRecordInPosition( cursor,
                c -> layout.writeKey( c, primKey ), pos, keyCount+1, tmpForKeys );
        bTreeNode.readChildrenWithInsertRecordInPosition( cursor,
                c -> bTreeNode.writeChild( c, newRightChild, stableGeneration, unstableGeneration ),
                pos+1, keyCount+2, tmpForChildren );

        int keyCountAfterInsert = keyCount + 1;
        int middlePos = middle( keyCountAfterInsert, options.splitRetentionFactor() );

        structurePropagation.hasSplit = true;
        structurePropagation.left = current;
        structurePropagation.right = newRight;

        {   // Update new right
            // NOTE: don't include middle
            goTo( cursor, "new right sibling in split", newRight );
            bTreeNode.initializeInternal( cursor, stableGeneration, unstableGeneration );
            bTreeNode.setRightSibling( cursor, oldRight, stableGeneration, unstableGeneration );
            bTreeNode.setLeftSibling( cursor, current, stableGeneration, unstableGeneration );
            bTreeNode.writeKeys( cursor, tmpForKeys, middlePos + 1, 0, keyCountAfterInsert - (middlePos + 1) );
            bTreeNode.writeChildren( cursor, tmpForChildren, middlePos + 1, 0,
                    keyCountAfterInsert - middlePos /*there's one more child than key to copy*/ );
            bTreeNode.setKeyCount( cursor, keyCount - middlePos );

            // Extract middle key (prim key)
            int middleOffset = middlePos * bTreeNode.keySize();
            PageCursor buffer = ByteArrayPageCursor.wrap( tmpForKeys, middleOffset, bTreeNode.keySize() );

            // Populate split result
            layout.readKey( buffer, structurePropagation.primKey );
        }

        // Update old right with new left sibling (newRight)
        if ( TreeNode.isNode( oldRight ) )
        {
            bTreeNode.goTo( cursor, "old right sibling", oldRight );
            bTreeNode.setLeftSibling( cursor, newRight, stableGeneration, unstableGeneration );
        }

        // Update left node
        // Move cursor back to left
        bTreeNode.goTo( cursor, "left", current );
        bTreeNode.setKeyCount( cursor, middlePos );
        if ( pos < middlePos )
        {
            // Write keys to left
            int arrayOffset = pos * bTreeNode.keySize();
            cursor.setOffset( bTreeNode.keyOffset( pos ) );
            cursor.putBytes( tmpForKeys, arrayOffset, (middlePos - pos) * bTreeNode.keySize() );

            cursor.setOffset( bTreeNode.childOffset( pos + 1 ) );
            arrayOffset = (pos + 1) * bTreeNode.childSize();
            cursor.putBytes( tmpForChildren, arrayOffset, (middlePos - pos) * bTreeNode.childSize() );
        }

        bTreeNode.setRightSibling( cursor, newRight, stableGeneration, unstableGeneration );
    }

    private static int middle( int keyCountAfterInsert, float splitLeftChildSize )
    {
        int middle = (int) (keyCountAfterInsert * splitLeftChildSize); // Floor division
        middle = max( 1, middle );
        middle = min( keyCountAfterInsert - 1, middle );
        return middle;
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * <p>
     * Split in leaf node caused by an insertion of key and value
     *
     * @param cursor {@link PageCursor} pinned to page containing leaf node targeted for
     * insertion.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param key key to be inserted
     * @param value value to be associated with key
     * @param valueMerger {@link ValueMerger} for deciding what to do with existing keys
     * @param options options for this insert
     * @throws IOException on cursor failure
     */
    private void insertInLeaf( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            KEY key, VALUE value, ValueMerger<VALUE> valueMerger,
            IndexWriter.Options options, long stableGeneration, long unstableGeneration ) throws IOException
    {
        int keyCount = bTreeNode.keyCount( cursor );
        int search = search( cursor, bTreeNode, key, readKey, keyCount );
        int pos = positionOf( search );
        if ( isHit( search ) )
        {
            // this key already exists, what shall we do? ask the valueMerger
            bTreeNode.valueAt( cursor, readValue, pos );
            VALUE mergedValue = valueMerger.merge( readValue, value );
            if ( mergedValue != null )
            {
                createUnstableVersionIfNeeded( cursor, structurePropagation, stableGeneration, unstableGeneration );
                // simple, just write the merged value right in there
                bTreeNode.setValueAt( cursor, mergedValue, pos );
            }
            return; // No split has occurred
        }

        createUnstableVersionIfNeeded( cursor, structurePropagation, stableGeneration, unstableGeneration );

        if ( keyCount < bTreeNode.leafMaxKeyCount() )
        {
            // No overflow, insert key and value
            bTreeNode.insertKeyAt( cursor, key, pos, keyCount );
            bTreeNode.insertValueAt( cursor, value, pos, keyCount );
            bTreeNode.setKeyCount( cursor, keyCount + 1 );

            return; // No split has occurred
        }
        // Overflow, split leaf
        splitLeaf( cursor, structurePropagation, key, value, keyCount, options, stableGeneration, unstableGeneration );
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * Cursor is expected to be pointing to full leaf.
     *
     * @param cursor cursor pointing into full (left) leaf that should be split in two.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param newKey key to be inserted
     * @param newValue value to be inserted (in association with key)
     * @param keyCount number of keys in this leaf (it was already read anyway)
     * @param options options for this insert
     * @throws IOException on cursor failure
     */
    private void splitLeaf( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            KEY newKey, VALUE newValue, int keyCount, IndexWriter.Options options,
            long stableGeneration, long unstableGeneration ) throws IOException
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
        long oldRight = bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( oldRight, true );
        long newRight = idProvider.acquireNewId( stableGeneration, unstableGeneration );

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
        int pos = positionOf( search( cursor, bTreeNode, newKey, readKey, keyCount ) );

        // arrays to temporarily store all keys and values
        bTreeNode.readKeysWithInsertRecordInPosition( cursor,
                c -> layout.writeKey( c, newKey ), pos, bTreeNode.leafMaxKeyCount() + 1, tmpForKeys );
        bTreeNode.readValuesWithInsertRecordInPosition( cursor,
                c -> layout.writeValue( c, newValue ), pos, bTreeNode.leafMaxKeyCount() + 1, tmpForValues );

        int keyCountAfterInsert = keyCount + 1;
        int middlePos = middle( keyCountAfterInsert, options.splitRetentionFactor() );

        // allKeysIncludingNewKey should now contain all keys in sorted order and
        // allValuesIncludingNewValue should now contain all values in same order as corresponding keys
        // and are ready to be split between left and newRight.

        // We now have everything we need to start working on newRight
        // and everything that needs to be updated in left has been so.

        structurePropagation.hasSplit = true;
        structurePropagation.left = current;
        structurePropagation.right = newRight;

        if ( middlePos == pos )
        {
            layout.copyKey( newKey, structurePropagation.primKey );
        }
        else
        {
            bTreeNode.keyAt( cursor, structurePropagation.primKey, pos < middlePos ? middlePos - 1 : middlePos );
        }

        {   // Update new right
            goTo( cursor, "new right sibling in split", newRight );
            bTreeNode.initializeLeaf( cursor, stableGeneration, unstableGeneration );
            bTreeNode.setRightSibling( cursor, oldRight, stableGeneration, unstableGeneration );
            bTreeNode.setLeftSibling( cursor, current, stableGeneration, unstableGeneration );
            bTreeNode.writeKeys( cursor, tmpForKeys, middlePos, 0, keyCountAfterInsert - middlePos );
            bTreeNode.writeValues( cursor, tmpForValues, middlePos, 0, keyCountAfterInsert - middlePos );
            bTreeNode.setKeyCount( cursor, keyCountAfterInsert - middlePos );
        }

        // Update old right with new left sibling (newRight)
        if ( TreeNode.isNode( oldRight ) )
        {
            bTreeNode.goTo( cursor, "old right sibling", oldRight );
            bTreeNode.setLeftSibling( cursor, newRight, stableGeneration, unstableGeneration );
        }

        // Update left child
        bTreeNode.goTo( cursor, "left", current );
        bTreeNode.setKeyCount( cursor, middlePos );
        // If pos < middle. Write shifted values to left node. Else, don't write anything.
        if ( pos < middlePos )
        {
            bTreeNode.writeKeys( cursor, tmpForKeys, pos, pos, middlePos - pos );
            bTreeNode.writeValues( cursor, tmpForValues, pos, pos, middlePos - pos );
        }
        bTreeNode.setRightSibling( cursor, newRight, stableGeneration, unstableGeneration );
    }

    /**
     * Remove given {@code key} and associated value from tree if it exists. The removed value will be stored in
     * provided {@code into} which will be returned for convenience.
     * <p>
     * If the given {@code key} does not exist in tree, return {@code null}.
     * <p>
     * Structural changes in tree that need to propagate to the level above will be reported through the provided
     * {@link StructurePropagation} by overwriting state. This is safe because structure changes happens one level
     * at the time.
     * {@link StructurePropagation} is provided from outside to minimize garbage.
     * <p>
     * Leaves cursor at same page as when called. No guarantees on offset.
     *
     * @param cursor {@link PageCursor} pinned to page where remove should traversing tree from.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param key key to be removed
     * @param into {@code VALUE} instance to write removed value to
     * @param stableGeneration stable generation, i.e. generations <= this generation are considered stable.
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @return Provided {@code into}, populated with removed value for convenience if {@code key} was removed.
     * Otherwise {@code null}.
     * @throws IOException on cursor failure
     */
    VALUE remove( PageCursor cursor, StructurePropagation<KEY> structurePropagation, KEY key, VALUE into,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        if ( bTreeNode.isLeaf( cursor ) )
        {
            return removeFromLeaf( cursor, structurePropagation, key, into, stableGeneration, unstableGeneration );
        }

        int keyCount = bTreeNode.keyCount( cursor );
        int search = search( cursor, bTreeNode, key, readKey, keyCount );
        int pos = positionOf( search );
        if ( isHit( search ) )
        {
            pos++;
        }

        long currentId = cursor.getCurrentPageId();
        long childId = bTreeNode.childAt( cursor, pos, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( childId, false );
        bTreeNode.goTo( cursor, "child", childId );

        VALUE result = remove( cursor, structurePropagation, key, into, stableGeneration, unstableGeneration );

        bTreeNode.goTo( cursor, "parent", currentId );
        if ( structurePropagation.hasNewGen )
        {
            structurePropagation.hasNewGen = false;
            bTreeNode.setChildAt( cursor, structurePropagation.left, pos, stableGeneration, unstableGeneration );
        }

        return result;
    }

    /**
     * Remove given {@code key} and associated value from tree if it exists. The removed value will be stored in
     * provided {@code into} which will be returned for convenience.
     * <p>
     * If the given {@code key} does not exist in tree, return {@code null}.
     * <p>
     * Leaves cursor at same page as when called. No guarantees on offset.
     *
     * @param cursor {@link PageCursor} pinned to page where remove is to be done.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param key key to be removed
     * @param into {@code VALUE} instance to write removed value to
     * @param stableGeneration stable generation, i.e. generations <= this generation are considered stable.
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @return Provided {@code into}, populated with removed value for convenience if {@code key} was removed.
     * Otherwise {@code null}.
     * @throws IOException on cursor failure
     */
    private VALUE removeFromLeaf( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            KEY key, VALUE into, long stableGeneration, long unstableGeneration ) throws IOException
    {
        int keyCount = bTreeNode.keyCount( cursor );

        // No overflow, insert key and value
        int search = search( cursor, bTreeNode, key, readKey, keyCount );
        int pos = positionOf( search );
        boolean hit = isHit( search );
        if ( !hit )
        {
            return null;
        }

        // Remove key/value
        createUnstableVersionIfNeeded( cursor, structurePropagation, stableGeneration, unstableGeneration );

        bTreeNode.removeKeyAt( cursor, pos, keyCount );
        bTreeNode.valueAt( cursor, into, pos );
        bTreeNode.removeValueAt( cursor, pos, keyCount );

        // Decrease key count
        bTreeNode.setKeyCount( cursor, keyCount - 1 );

        return into;
    }

    /**
     * Create a new node and copy content from current node (where {@code cursor} sits) if current node is not already
     * of {@code unstableGeneration}.
     * <p>
     * Neighboring nodes' sibling pointers will be updated to point to new node.
     * <p>
     * Current node will be updated with new gen pointer to new node.
     * <p>
     * {@code structurePropagation} will be updated with information about this new node so that it can report to
     * level above.
     *
     * @param cursor {@link PageCursor} pinned to page containing node to potentially create a new version of
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param stableGeneration stable generation, i.e. generations <= this generation are considered stable.
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @throws IOException on cursor failure
     */
    private void createUnstableVersionIfNeeded( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        long oldGenId = cursor.getCurrentPageId();
        long nodeGen = bTreeNode.gen( cursor );
        if ( nodeGen == unstableGeneration )
        {
            // Don't copy
            return;
        }

        // Do copy
        long newGenId = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        try ( PageCursor newGenCursor = cursor.openLinkedCursor( newGenId ) )
        {
            goTo( newGenCursor, "new gen", newGenId );
            cursor.copyTo( 0, newGenCursor, 0, cursor.getCurrentPageSize() );
            bTreeNode.setGen( newGenCursor, unstableGeneration );
            bTreeNode.setNewGen( newGenCursor, TreeNode.NO_NODE_FLAG, stableGeneration, unstableGeneration );
        }

        // Insert new gen pointer in old stable version
        //   (stableNode)
        //        |
        //    [newgen]
        //        |
        //        v
        // (newUnstableNode)
        bTreeNode.setNewGen( cursor, newGenId, stableGeneration, unstableGeneration );

        // Redirect sibling pointers
        //               ---------[leftSibling]---------(stableNode)----------[rightSibling]---------
        //              |                                     |                                      |
        //              |                                  [newgen]                                  |
        //              |                                     |                                      |
        //              v                                     v                                      v
        // (leftSiblingOfStableNode) -[rightSibling]-> (newUnstableNode) <-[leftSibling]- (rightSiblingOfStableNode)
        long leftSibling = bTreeNode.leftSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( leftSibling, true );
        long rightSibling = bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( rightSibling, true );
        if ( TreeNode.isNode( leftSibling ) )
        {
            bTreeNode.goTo( cursor, "left sibling in split", leftSibling );
            bTreeNode.setRightSibling( cursor, newGenId, stableGeneration, unstableGeneration );
        }
        if ( TreeNode.isNode( rightSibling ) )
        {
            bTreeNode.goTo( cursor, "right sibling in split", rightSibling );
            bTreeNode.setLeftSibling( cursor, newGenId, stableGeneration, unstableGeneration );
        }

        // Leave cursor at new tree node
        bTreeNode.goTo( cursor, "new gen", newGenId );

        // Propagate structure change
        structurePropagation.hasNewGen = true;
        structurePropagation.left = newGenId;

        idProvider.releaseId( stableGeneration, unstableGeneration, oldGenId );
    }
}
