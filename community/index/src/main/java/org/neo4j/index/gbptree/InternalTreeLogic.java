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
import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.index.ValueMerger;
import org.neo4j.io.pagecache.PageCursor;

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
    private final Layout<KEY,VALUE> layout;
    private final KEY primKeyPlaceHolder;
    private final KEY readKey;
    private final VALUE readValue;

    /**
     * Current path down the tree
     * - level:-1 is uninitialized (so that a call to {@link #initialize(PageCursor)} is required)
     * - level: 0 is at root
     * - level: 1 is at first level below root
     * ... a.s.o
     *
     * Calling {@link #insert(PageCursor, StructurePropagation, Object, Object, ValueMerger, long, long)}
     * or {@link #remove(PageCursor, StructurePropagation, Object, Object, long, long)} leaves the cursor
     * at the last updated page (tree node id) and remembers the path down the tree to where it is.
     * Further inserts/removals will move the cursor from its current position to where the next change will
     * take place using as few page pins as possible.
     */
    @SuppressWarnings( "unchecked" )
    private Level<KEY>[] levels = new Level[0]; // grows on demand
    private int currentLevel = -1;

    /**
     * Keeps information about one level in a path down the tree where the {@link PageCursor} is currently at.
     *
     * @param <KEY> type of keys in the tree.
     */
    private static class Level<KEY>
    {
        // For comparing keys
        private final Comparator<KEY> layout;
        // Id of the tree node id this level of the path
        private long treeNodeId;

        // Child position which was selected from parent to get to this level
        private int childPos;
        // Lower bound of key range this level covers
        private final KEY lower;
        // Whether or not the lower bound is fixed or open-ended (far left in the tree)
        private boolean lowerIsOpenEnded;
        // Upper bound of key range this level covers
        private final KEY upper;
        // Whether or not the upper bound is fixed or open-ended (far right in the tree)
        private boolean upperIsOpenEnded;

        Level( Layout<KEY,?> layout )
        {
            this.layout = layout;
            this.lower = layout.newKey();
            this.upper = layout.newKey();
        }

        /**
         * Returns whether or not the key range of this level of the path covers the given {@code key}.
         *
         * @param key KEY to check.
         * @return {@code true} if key is within the key range if this level, otherwise {@code false}.
         */
        boolean covers( KEY key )
        {
            if ( !lowerIsOpenEnded && layout.compare( key, lower ) < 0 )
            {
                return false;
            }
            if ( !upperIsOpenEnded && layout.compare( key, upper ) >= 0 )
            {
                return false;
            }
            return true;
        }
    }

    InternalTreeLogic( IdProvider idProvider, TreeNode<KEY,VALUE> bTreeNode, Layout<KEY,VALUE> layout )
    {
        this.idProvider = idProvider;
        this.bTreeNode = bTreeNode;
        this.layout = layout;
        this.primKeyPlaceHolder = layout.newKey();
        this.readKey = layout.newKey();
        this.readValue = layout.newValue();

        // an arbitrary depth slightly bigger than an unimaginably big tree
        ensureStackCapacity( 10 );
    }

    private void ensureStackCapacity( int depth )
    {
        if ( depth > levels.length )
        {
            int oldStackLength = levels.length;
            levels = Arrays.copyOf( levels, depth );
            for ( int i = oldStackLength; i < depth; i++ )
            {
                levels[i] = new Level<>( layout );
            }
        }
    }

    protected void initialize( PageCursor cursorAtRoot )
    {
        currentLevel = 0;
        Level<KEY> level = levels[currentLevel];
        level.treeNodeId = cursorAtRoot.getCurrentPageId();
        level.lowerIsOpenEnded = true;
        level.upperIsOpenEnded = true;
    }

    private boolean popLevel( PageCursor cursor ) throws IOException
    {
        currentLevel--;
        if ( currentLevel >= 0 )
        {
            Level<KEY> level = levels[currentLevel];
            bTreeNode.goTo( cursor, "parent", level.treeNodeId );
            return true;
        }
        return false;
    }

    /**
     * Moves the cursor to the correct leaf for {@code key}, taking the current path into consideration
     * and moving the cursor as few hops as possible to get from the current position to the target position,
     * e.g given tree:
     *
     * <pre>
     *              [A]
     *       ------/ | \------
     *      /        |        \
     *    [B]       [C]       [D]
     *   / | \     / | \     / | \
     * [E][F][G] [H][I][J] [K][L][M]
     * </pre>
     *
     * Examples:
     * <p>
     *
     * inserting a key into J (path A,C,J) after previously have inserted a key into F (path A,B,F):
     * <p>
     * <ol>
     * <li>Seeing that F doesn't cover new key</li>
     * <li>Popping stack, seeing that B doesn't cover new key (only by asking existing information in path)</li>
     * <li>Popping stack, seeing that A covers new key (only by asking existing information in path)</li>
     * <li>Binary search A to select C to go down to</li>
     * <li>Binary search C to select J to go down to</li>
     * </ol>
     * <p>
     * inserting a key into G (path A,B,G) after previously have inserted a key into F (path A,B,F):
     * <p>
     * <ol>
     * <li>Seeing that F doesn't cover new key</li>
     * <li>Popping stack, seeing that B covers new key (only by asking existing information in path)</li>
     * <li>Binary search B to select G to go down to</li>
     * </ol>
     *
     * The closer keys are together from one change to the next, the fewer page pins and searches needs
     * to be performed to get there.
     *
     * @param cursor {@link PageCursor} to move to the correct location.
     * @param key KEY to make change for.
     * @param stableGeneration stable generation.
     * @param unstableGeneration unstable generation.
     * @throws IOException on {@link PageCursor} error.
     */
    private void moveToCorrectLeaf( PageCursor cursor, KEY key, long stableGeneration, long unstableGeneration )
            throws IOException
    {
        int previousLevel = currentLevel;
        while ( !levels[currentLevel].covers( key ) )
        {
            currentLevel--;
        }
        if ( currentLevel != previousLevel )
        {
            goTo( cursor, "parent", levels[currentLevel].treeNodeId );
        }

        while ( bTreeNode.isInternal( cursor ) )
        {
            // We still need to go down further, but we're on the right path
            int keyCount = bTreeNode.keyCount( cursor );
            int searchResult = search( cursor, bTreeNode, key, readKey, keyCount );
            int childPos = positionOf( searchResult );
            if ( isHit( searchResult ) )
            {
                childPos++;
            }

            Level<KEY> parentLevel = levels[currentLevel];
            currentLevel++;
            ensureStackCapacity( currentLevel + 1 );
            Level<KEY> level = levels[currentLevel];

            // Restrict the key range as the cursor moves down to the next level
            level.childPos = childPos;
            level.lowerIsOpenEnded = childPos == 0 &&
                    !TreeNode.isNode( bTreeNode.leftSibling( cursor, stableGeneration, unstableGeneration ) );
            if ( !level.lowerIsOpenEnded )
            {
                if ( childPos == 0 )
                {
                    layout.copyKey( parentLevel.lower, level.lower );
                    level.lowerIsOpenEnded = parentLevel.lowerIsOpenEnded;
                }
                else
                {
                    bTreeNode.keyAt( cursor, level.lower, childPos - 1 );
                }
            }
            level.upperIsOpenEnded = childPos >= keyCount &&
                    !TreeNode.isNode( bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration ) );
            if ( !level.upperIsOpenEnded )
            {
                if ( childPos == keyCount )
                {
                    layout.copyKey( parentLevel.upper, level.upper );
                    level.upperIsOpenEnded = parentLevel.upperIsOpenEnded;
                }
                else
                {
                    bTreeNode.keyAt( cursor, level.upper, childPos );
                }
            }

            long childId = bTreeNode.childAt( cursor, childPos, stableGeneration, unstableGeneration );
            PointerChecking.checkPointer( childId, false );

            bTreeNode.goTo( cursor, "child", childId );
            level.treeNodeId = cursor.getCurrentPageId();
        }

        assert bTreeNode.isLeaf( cursor ) : "Ended up on a tree node which isn't a leaf after moving cursor towards " +
                key + ", cursor is at " + cursor.getCurrentPageId();
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
     * Leaves cursor at the page which was last updated. No guarantees on offset.
     *
     * @param cursor {@link PageCursor} pinned to root of tree (if first insert/remove since
     * {@link #initialize(PageCursor)}) or at where last insert/remove left it.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param key key to be inserted
     * @param value value to be associated with key
     * @param valueMerger {@link ValueMerger} for deciding what to do with existing keys
     * @param stableGeneration stable generation, i.e. generations <= this generation are considered stable.
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @throws IOException on cursor failure
     */
    void insert( PageCursor cursor, StructurePropagation<KEY> structurePropagation, KEY key, VALUE value,
            ValueMerger<VALUE> valueMerger, long stableGeneration, long unstableGeneration ) throws IOException
    {
        assert cursorIsAtExpectedLocation( cursor );
        moveToCorrectLeaf( cursor, key, stableGeneration, unstableGeneration );

        insertInLeaf( cursor, structurePropagation, key, value, valueMerger, stableGeneration, unstableGeneration );

        while ( structurePropagation.hasNewGen || structurePropagation.hasSplit )
        {
            int pos = levels[currentLevel].childPos;
            if ( !popLevel( cursor ) )
            {
                // Root split, let that be handled outside
                break;
            }

            if ( structurePropagation.hasNewGen )
            {
                structurePropagation.hasNewGen = false;
                bTreeNode.setChildAt( cursor, structurePropagation.left, pos, stableGeneration, unstableGeneration );
            }
            if ( structurePropagation.hasSplit )
            {
                structurePropagation.hasSplit = false;
                insertInInternal( cursor, structurePropagation, bTreeNode.keyCount( cursor ), structurePropagation.primKey,
                        structurePropagation.right, stableGeneration, unstableGeneration );
            }
        }
    }

    /**
     * Asserts that cursor is where it's expected to be at, compared to current level.
     *
     * @param cursor
     * @return {@code true} so that it can be called in an {@code assert} statement.
     */
    private boolean cursorIsAtExpectedLocation( PageCursor cursor )
    {
        assert currentLevel >= 0 : "Uninitialized tree logic, currentLevel:" + currentLevel;
        assert cursor.getCurrentPageId() == levels[currentLevel].treeNodeId : "Expected cursor to be at page:" +
                    levels[currentLevel].treeNodeId + " at level:" + currentLevel + ", but was at page:" +
                    cursor.getCurrentPageId();
        return true;
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * <p>
     * Insertion in internal is always triggered by a split in child.
     * The result of a split is a primary key that is sent upwards in the b+tree and the newly created right child.
     *
     * @param cursor {@link PageCursor} pinned to page containing internal node, current node
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param keyCount the key count of current node
     * @param primKey the primary key to be inserted
     * @param rightChild the right child of primKey
     * @throws IOException on cursor failure
     */
    private void insertInInternal( PageCursor cursor, StructurePropagation<KEY> structurePropagation, int keyCount,
            KEY primKey, long rightChild, long stableGeneration, long unstableGeneration )
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
        // We will overwrite primKey in structurePropagation, so copy it over to a place holder
        layout.copyKey( structurePropagation.primKey, primKeyPlaceHolder );
        splitInternal( cursor, structurePropagation, primKeyPlaceHolder, rightChild, keyCount,
                stableGeneration, unstableGeneration );
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * <p>
     * Split in internal node caused by an insertion of primKey and newRightChild
     *
     * @param cursor {@link PageCursor} pinned to page containing internal node, full node.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param primKey primary key to be inserted, causing the split
     * @param newRightChild right child of primKey
     * @param keyCount key count for fullNode
     * @throws IOException on cursor failure
     */
    private void splitInternal( PageCursor cursor, StructurePropagation<KEY> structurePropagation, KEY primKey,
            long newRightChild, int keyCount, long stableGeneration, long unstableGeneration ) throws IOException
    {
        long current = cursor.getCurrentPageId();
        long oldRight = bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( oldRight, true );
        long newRight = idProvider.acquireNewId( stableGeneration, unstableGeneration );

        // Find position to insert new key
        int pos = positionOf( search( cursor, bTreeNode, primKey, readKey, keyCount ) );

        int keyCountAfterInsert = keyCount + 1;
        int middlePos = middle( keyCountAfterInsert );

        // Update structurePropagation
        structurePropagation.hasSplit = true;
        structurePropagation.left = current;
        structurePropagation.right = newRight;
        if ( middlePos == pos )
        {
            layout.copyKey( primKey, structurePropagation.primKey );
        }
        else
        {
            bTreeNode.keyAt( cursor, structurePropagation.primKey, pos < middlePos ? middlePos - 1 : middlePos );
        }

        // Update new right
        try ( PageCursor rightCursor = cursor.openLinkedCursor( newRight ) )
        {
            goTo( rightCursor, "new right sibling in split", newRight );
            bTreeNode.initializeInternal( rightCursor, stableGeneration, unstableGeneration );
            bTreeNode.setRightSibling( rightCursor, oldRight, stableGeneration, unstableGeneration );
            bTreeNode.setLeftSibling( rightCursor, current, stableGeneration, unstableGeneration );
            int rightKeyCount = keyCountAfterInsert - middlePos - 1; // -1 because don't keep prim key in internal

            if ( pos < middlePos )
            {
                //                         v-------v       copy
                // before key    _,_,_,_,_,_,_,_,_,_
                // before child -,-,-,-,-,-,-,-,-,-,-
                // insert key    _,_,X,_,_,_,_,_,_,_,_
                // insert child -,-,-,x,-,-,-,-,-,-,-,-
                // middle key              ^

                // children
                cursor.copyTo( bTreeNode.keyOffset( middlePos ), rightCursor, bTreeNode.keyOffset( 0 ),
                        rightKeyCount * bTreeNode.keySize() );
                cursor.copyTo( bTreeNode.childOffset( middlePos ), rightCursor, bTreeNode.childOffset( 0 ),
                        (rightKeyCount + 1) * bTreeNode.childSize() );
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
                int countBeforePos = pos - (middlePos + 1);
                // ... first copy
                if ( countBeforePos > 0 )
                {
                    cursor.copyTo( bTreeNode.keyOffset( middlePos + 1 ), rightCursor, bTreeNode.keyOffset( 0 ),
                            countBeforePos * bTreeNode.keySize() );
                }
                // ... insert
                if ( countBeforePos >= 0 )
                {
                    bTreeNode.insertKeyAt( rightCursor, primKey, countBeforePos, countBeforePos );
                }
                // ... second copy
                int countAfterPos = keyCount - pos;
                if ( countAfterPos > 0 )
                {
                    cursor.copyTo( bTreeNode.keyOffset( pos ), rightCursor,
                            bTreeNode.keyOffset( countBeforePos + 1 ), countAfterPos * bTreeNode.keySize() );
                }

                // Children
                countBeforePos = pos - middlePos;
                // ... first copy
                if ( countBeforePos > 0 )
                {
                    // first copy
                    cursor.copyTo( bTreeNode.childOffset( middlePos + 1 ), rightCursor, bTreeNode.childOffset( 0 ),
                            countBeforePos * bTreeNode.childSize() );
                }
                // ... insert
                bTreeNode.insertChildAt( rightCursor, newRightChild, countBeforePos, countBeforePos,
                        stableGeneration, unstableGeneration );
                // ... second copy
                if ( countAfterPos > 0 )
                {
                    cursor.copyTo( bTreeNode.childOffset( pos + 1 ), rightCursor,
                            bTreeNode.childOffset( countBeforePos + 1 ), countAfterPos * bTreeNode.childSize() );
                }
            }
            bTreeNode.setKeyCount( rightCursor, rightKeyCount );
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
            bTreeNode.insertKeyAt( cursor, primKey, pos, middlePos - 1 );
            bTreeNode.insertChildAt( cursor, newRightChild, pos + 1, middlePos - 1,
                    stableGeneration, unstableGeneration );
        }

        bTreeNode.setRightSibling( cursor, newRight, stableGeneration, unstableGeneration );
    }

    private static int middle( int keyCountAfterInsert )
    {
        return keyCountAfterInsert / 2;
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * <p>
     * Split in leaf node caused by an insertion of key and value
     *
     * @param cursor {@link PageCursor} pinned to page containing leaf node targeted for insertion.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param key key to be inserted
     * @param value value to be associated with key
     * @param valueMerger {@link ValueMerger} for deciding what to do with existing keys
     * @throws IOException on cursor failure
     */
    private void insertInLeaf( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            KEY key, VALUE value, ValueMerger<VALUE> valueMerger,
            long stableGeneration, long unstableGeneration ) throws IOException
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
        splitLeaf( cursor, structurePropagation, key, value, keyCount, stableGeneration, unstableGeneration );
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
     * @throws IOException on cursor failure
     */
    private void splitLeaf( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            KEY newKey, VALUE newValue, int keyCount, long stableGeneration, long unstableGeneration )
                    throws IOException
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
        int keyCountAfterInsert = keyCount + 1;
        int middlePos = middle( keyCountAfterInsert );

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

        // Update new right
        try ( PageCursor rightCursor = cursor.openLinkedCursor( newRight ) )
        {
            goTo( rightCursor, "new right sibling in split", newRight );
            bTreeNode.initializeLeaf( rightCursor, stableGeneration, unstableGeneration );
            bTreeNode.setRightSibling( rightCursor, oldRight, stableGeneration, unstableGeneration );
            bTreeNode.setLeftSibling( rightCursor, current, stableGeneration, unstableGeneration );
            int rightKeyCount = keyCountAfterInsert - middlePos;

            if ( pos < middlePos )
            {
                //                  v-------v       copy
                // before _,_,_,_,_,_,_,_,_,_
                // insert _,_,_,X,_,_,_,_,_,_,_
                // middle           ^
                copyKeysAndValues( cursor, middlePos - 1, rightCursor, 0, rightKeyCount );
            }
            else
            {
                //                  v---v           first copy
                //                        v-v       second copy
                // before _,_,_,_,_,_,_,_,_,_
                // insert _,_,_,_,_,_,_,_,X,_,_
                // middle           ^
                int countBeforePos = pos - middlePos;
                if ( countBeforePos > 0 )
                {
                    // first copy
                    copyKeysAndValues( cursor, middlePos, rightCursor, 0, countBeforePos );
                }
                bTreeNode.insertKeyAt( rightCursor, newKey, countBeforePos, countBeforePos );
                bTreeNode.insertValueAt( rightCursor, newValue, countBeforePos, countBeforePos );
                int countAfterPos = keyCount - pos;
                if ( countAfterPos > 0 )
                {
                    // second copy
                    copyKeysAndValues( cursor, pos, rightCursor, countBeforePos + 1, countAfterPos );
                }
            }
            bTreeNode.setKeyCount( rightCursor, rightKeyCount );
        }

        // Update old right with new left sibling (newRight)
        if ( TreeNode.isNode( oldRight ) )
        {
            try ( PageCursor oldRightCursor = cursor.openLinkedCursor( oldRight ) )
            {
                bTreeNode.goTo( oldRightCursor, "old right sibling", oldRight );
                bTreeNode.setLeftSibling( oldRightCursor, newRight, stableGeneration, unstableGeneration );
            }
        }

        // Update left child
        // If pos < middle. Write shifted values to left node. Else, don't write anything.
        if ( pos < middlePos )
        {
            bTreeNode.insertKeyAt( cursor, newKey, pos, middlePos - 1 );
            bTreeNode.insertValueAt( cursor, newValue, pos, middlePos - 1 );
        }
        bTreeNode.setKeyCount( cursor, middlePos );
        bTreeNode.setRightSibling( cursor, newRight, stableGeneration, unstableGeneration );
    }

    private void copyKeysAndValues( PageCursor cursor, int fromPos, PageCursor rightCursor, int toPos, int count )
    {
        cursor.copyTo( bTreeNode.keyOffset( fromPos ), rightCursor, bTreeNode.keyOffset( toPos ),
                count * bTreeNode.keySize() );
        cursor.copyTo( bTreeNode.valueOffset( fromPos ), rightCursor, bTreeNode.valueOffset( toPos ),
                count * bTreeNode.valueSize() );
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
     * Leaves cursor at the page which was last updated. No guarantees on offset.
     *
     * @param cursor {@link PageCursor} pinned to root of tree (if first insert/remove since
     * {@link #initialize(PageCursor)}) or at where last insert/remove left it.
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
        assert cursorIsAtExpectedLocation( cursor );
        moveToCorrectLeaf( cursor, key, stableGeneration, unstableGeneration );

        if ( !removeFromLeaf( cursor, structurePropagation, key, into, stableGeneration, unstableGeneration ) )
        {
            return null;
        }

        while ( structurePropagation.hasNewGen )
        {
            int pos = levels[currentLevel].childPos;
            if ( !popLevel( cursor ) )
            {
                // Root split, let that be handled outside
                break;
            }

            if ( structurePropagation.hasNewGen )
            {
                structurePropagation.hasNewGen = false;
                bTreeNode.setChildAt( cursor, structurePropagation.left, pos, stableGeneration, unstableGeneration );
            }
        }

        return into;
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
     * @return {@code true} if key was removed, otherwise {@code false}.
     * @throws IOException on cursor failure
     */
    private boolean removeFromLeaf( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            KEY key, VALUE into, long stableGeneration, long unstableGeneration ) throws IOException
    {
        int keyCount = bTreeNode.keyCount( cursor );

        // No overflow, insert key and value
        int search = search( cursor, bTreeNode, key, readKey, keyCount );
        int pos = positionOf( search );
        boolean hit = isHit( search );
        if ( !hit )
        {
            return false;
        }

        // Remove key/value
        createUnstableVersionIfNeeded( cursor, structurePropagation, stableGeneration, unstableGeneration );

        bTreeNode.removeKeyAt( cursor, pos, keyCount );
        bTreeNode.valueAt( cursor, into, pos );
        bTreeNode.removeValueAt( cursor, pos, keyCount );

        // Decrease key count
        bTreeNode.setKeyCount( cursor, keyCount - 1 );
        return true;
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
