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
import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.index.internal.gbptree.TreeNode.Overflow;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.KeySearch.isHit;
import static org.neo4j.index.internal.gbptree.KeySearch.positionOf;
import static org.neo4j.index.internal.gbptree.PointerChecking.assertNoSuccessor;
import static org.neo4j.index.internal.gbptree.StructurePropagation.KeyReplaceStrategy.BUBBLE;
import static org.neo4j.index.internal.gbptree.StructurePropagation.KeyReplaceStrategy.REPLACE;
import static org.neo4j.index.internal.gbptree.StructurePropagation.UPDATE_LEFT_CHILD;
import static org.neo4j.index.internal.gbptree.StructurePropagation.UPDATE_MID_CHILD;
import static org.neo4j.index.internal.gbptree.StructurePropagation.UPDATE_RIGHT_CHILD;
import static org.neo4j.index.internal.gbptree.TreeNode.Overflow.NO_NEED_DEFRAG;
import static org.neo4j.index.internal.gbptree.TreeNode.Overflow.YES;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;

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
    private final KEY newKeyPlaceHolder;
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
            boolean insideLower = lowerIsOpenEnded || layout.compare( key, lower ) >= 0;
            boolean insideHigher = upperIsOpenEnded || layout.compare( key, upper ) < 0;
            return insideLower && insideHigher;
        }
    }

    InternalTreeLogic( IdProvider idProvider, TreeNode<KEY,VALUE> bTreeNode, Layout<KEY,VALUE> layout )
    {
        this.idProvider = idProvider;
        this.bTreeNode = bTreeNode;
        this.layout = layout;
        this.newKeyPlaceHolder = layout.newKey();
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
            TreeNode.goTo( cursor, "parent", level.treeNodeId );
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
            TreeNode.goTo( cursor, "parent", levels[currentLevel].treeNodeId );
        }

        while ( TreeNode.isInternal( cursor ) )
        {
            // We still need to go down further, but we're on the right path
            int keyCount = TreeNode.keyCount( cursor );
            int searchResult = search( cursor, INTERNAL, key, readKey, keyCount );
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
                    !TreeNode.isNode( TreeNode.leftSibling( cursor, stableGeneration, unstableGeneration ) );
            if ( !level.lowerIsOpenEnded )
            {
                if ( childPos == 0 )
                {
                    layout.copyKey( parentLevel.lower, level.lower );
                    level.lowerIsOpenEnded = parentLevel.lowerIsOpenEnded;
                }
                else
                {
                    bTreeNode.keyAt( cursor, level.lower, childPos - 1, INTERNAL );
                }
            }
            level.upperIsOpenEnded = childPos >= keyCount &&
                    !TreeNode.isNode( TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration ) );
            if ( !level.upperIsOpenEnded )
            {
                if ( childPos == keyCount )
                {
                    layout.copyKey( parentLevel.upper, level.upper );
                    level.upperIsOpenEnded = parentLevel.upperIsOpenEnded;
                }
                else
                {
                    bTreeNode.keyAt( cursor, level.upper, childPos, INTERNAL );
                }
            }

            long childId = bTreeNode.childAt( cursor, childPos, stableGeneration, unstableGeneration );
            PointerChecking.checkPointer( childId, false );

            TreeNode.goTo( cursor, "child", childId );
            level.treeNodeId = cursor.getCurrentPageId();

            assert assertNoSuccessor( cursor, stableGeneration, unstableGeneration );
        }

        assert TreeNode.isLeaf( cursor ) : "Ended up on a tree node which isn't a leaf after moving cursor towards " +
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
     * generation version of root. This needs to be handled by caller.
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
            ValueMerger<KEY,VALUE> valueMerger, long stableGeneration, long unstableGeneration ) throws IOException
    {
        assert cursorIsAtExpectedLocation( cursor );
        bTreeNode.validateKeyValueSize( key, value );
        moveToCorrectLeaf( cursor, key, stableGeneration, unstableGeneration );

        insertInLeaf( cursor, structurePropagation, key, value, valueMerger, stableGeneration, unstableGeneration );

        handleStructureChanges( cursor, structurePropagation, stableGeneration, unstableGeneration );
    }

    private int search( PageCursor cursor, TreeNode.Type type, KEY key, KEY readKey, int keyCount )
    {
        int searchResult = KeySearch.search( cursor, bTreeNode, type, key, readKey, keyCount );
        KeySearch.assertSuccess( searchResult );
        return searchResult;
    }

    /**
     * Asserts that cursor is where it's expected to be at, compared to current level.
     *
     * @param cursor {@link PageCursor} to check.
     * @return {@code true} so that it can be called in an {@code assert} statement.
     */
    private boolean cursorIsAtExpectedLocation( PageCursor cursor )
    {
        assert currentLevel >= 0 : "Uninitialized tree logic, currentLevel:" + currentLevel;
        long currentPageId = cursor.getCurrentPageId();
        long expectedPageId = levels[currentLevel].treeNodeId;
        assert currentPageId == expectedPageId : "Expected cursor to be at page:" +
                expectedPageId + " at level:" + currentLevel + ", but was at page:" +
                currentPageId;
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
        createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                stableGeneration, unstableGeneration );

        doInsertInInternal( cursor, structurePropagation, keyCount, primKey, rightChild, stableGeneration, unstableGeneration );
    }

    private void doInsertInInternal( PageCursor cursor, StructurePropagation<KEY> structurePropagation, int keyCount, KEY primKey,
            long rightChild, long stableGeneration, long unstableGeneration ) throws IOException
    {
        Overflow overflow = bTreeNode.internalOverflow( cursor, keyCount, primKey );
        if ( overflow == YES )
        {
            // Overflow
            // We will overwrite rightKey in structurePropagation, so copy it over to a place holder
            layout.copyKey( primKey, newKeyPlaceHolder );
            splitInternal( cursor, structurePropagation, newKeyPlaceHolder, rightChild, keyCount,
                    stableGeneration, unstableGeneration );
            return;
        }

        if ( overflow == NO_NEED_DEFRAG )
        {
            bTreeNode.defragmentInternal( cursor );
        }

        // No overflow
        int pos = positionOf( search( cursor, INTERNAL, primKey, readKey, keyCount ) );
        bTreeNode.insertKeyAndRightChildAt( cursor, primKey, rightChild, pos, keyCount, stableGeneration, unstableGeneration );
        // Increase key count
        TreeNode.setKeyCount( cursor, keyCount + 1 );
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * <p>
     * Split in internal node caused by an insertion of rightKey and newRightChild
     *
     * @param cursor {@link PageCursor} pinned to page containing internal node, full node.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param newKey new key to be inserted together with newRightChild, causing the split
     * @param newRightChild new child to be inserted to the right of newKey
     * @param keyCount key count for fullNode
     * @throws IOException on cursor failure
     */
    private void splitInternal( PageCursor cursor, StructurePropagation<KEY> structurePropagation, KEY newKey,
            long newRightChild, int keyCount, long stableGeneration, long unstableGeneration ) throws IOException
    {
        long current = cursor.getCurrentPageId();
        long oldRight = TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( oldRight, true );
        long newRight = idProvider.acquireNewId( stableGeneration, unstableGeneration );

        // Find position to insert new key
        int pos = positionOf( search( cursor, INTERNAL, newKey, readKey, keyCount ) );

        // Update structurePropagation
        structurePropagation.hasRightKeyInsert = true;
        structurePropagation.midChild = current;
        structurePropagation.rightChild = newRight;

        try ( PageCursor rightCursor = cursor.openLinkedCursor( newRight ) )
        {
            // Initialize new right
            TreeNode.goTo( rightCursor, "new right sibling in split", newRight );
            bTreeNode.initializeInternal( rightCursor, stableGeneration, unstableGeneration );
            TreeNode.setRightSibling( rightCursor, oldRight, stableGeneration, unstableGeneration );
            TreeNode.setLeftSibling( rightCursor, current, stableGeneration, unstableGeneration );

            // Do split
            bTreeNode.doSplitInternal( cursor, keyCount, rightCursor, pos, newKey, newRightChild, stableGeneration, unstableGeneration,
                    structurePropagation.rightKey );
        }

        // Update old right with new left sibling (newRight)
        if ( TreeNode.isNode( oldRight ) )
        {
            try ( PageCursor oldRightCursor = cursor.openLinkedCursor( oldRight ) )
            {
                TreeNode.goTo( oldRightCursor, "old right sibling", oldRight );
                TreeNode.setLeftSibling( oldRightCursor, newRight, stableGeneration, unstableGeneration );
            }
        }

        // Update left node with new right sibling
        TreeNode.setRightSibling( cursor, newRight, stableGeneration, unstableGeneration );
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
    private void insertInLeaf( PageCursor cursor, StructurePropagation<KEY> structurePropagation, KEY key, VALUE value,
            ValueMerger<KEY,VALUE> valueMerger, long stableGeneration, long unstableGeneration ) throws IOException
    {
        int keyCount = TreeNode.keyCount( cursor );
        int search = search( cursor, LEAF, key, readKey, keyCount );
        int pos = positionOf( search );
        if ( isHit( search ) )
        {
            overwriteValue( cursor, structurePropagation, key, value, valueMerger, pos, keyCount, stableGeneration, unstableGeneration );
            return;
        }

        createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD, stableGeneration, unstableGeneration );

        doInsertInLeaf( cursor, structurePropagation, key, value, pos, keyCount, stableGeneration, unstableGeneration );
    }

    private void overwriteValue( PageCursor cursor, StructurePropagation<KEY> structurePropagation, KEY key, VALUE value,
            ValueMerger<KEY,VALUE> valueMerger, int pos, int keyCount, long stableGeneration, long unstableGeneration ) throws IOException
    {
        // this key already exists, what shall we do? ask the valueMerger
        bTreeNode.valueAt( cursor, readValue, pos );
        VALUE mergedValue = valueMerger.merge( readKey, key, readValue, value );
        if ( mergedValue != null )
        {
            createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD, stableGeneration, unstableGeneration );
            // simple, just write the merged value right in there
            boolean couldOverwrite = bTreeNode.setValueAt( cursor, mergedValue, pos );
            //noinspection StatementWithEmptyBody
            if ( !couldOverwrite )
            {
                // Value could not be overwritten in a simple way because they differ in size.
                // Delete old value
                bTreeNode.removeKeyValueAt( cursor, pos, keyCount );
                TreeNode.setKeyCount( cursor, keyCount - 1 );
                boolean didSplit =
                        doInsertInLeaf( cursor, structurePropagation, key, mergedValue, pos, keyCount - 1, stableGeneration, unstableGeneration );
                if ( !didSplit && bTreeNode.leafUnderflow( cursor, keyCount ) )
                {
                    underflowInLeaf( cursor, structurePropagation, keyCount, stableGeneration, unstableGeneration );
                }
            }
        }
    }

    private boolean doInsertInLeaf( PageCursor cursor, StructurePropagation<KEY> structurePropagation, KEY key, VALUE value, int pos,
            int keyCount, long stableGeneration, long unstableGeneration ) throws IOException
    {
        Overflow overflow = bTreeNode.leafOverflow( cursor, keyCount, key, value );
        if ( overflow == YES )
        {
            // Overflow, split leaf
            splitLeaf( cursor, structurePropagation, key, value, keyCount, stableGeneration, unstableGeneration );
            return true;
        }

        if ( overflow == NO_NEED_DEFRAG )
        {
            bTreeNode.defragmentLeaf( cursor );
        }

        // No overflow, insert key and value
        bTreeNode.insertKeyValueAt( cursor, key, value, pos, keyCount );
        TreeNode.setKeyCount( cursor, keyCount + 1 );
        return false;
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
        long oldRight = TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
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
        int pos = positionOf( search( cursor, LEAF, newKey, readKey, keyCount ) );

        structurePropagation.hasRightKeyInsert = true;
        structurePropagation.midChild = current;
        structurePropagation.rightChild = newRight;

        try ( PageCursor rightCursor = cursor.openLinkedCursor( newRight ) )
        {
            // Initialize new right
            TreeNode.goTo( rightCursor, "new right sibling in split", newRight );
            bTreeNode.initializeLeaf( rightCursor, stableGeneration, unstableGeneration );
            TreeNode.setRightSibling( rightCursor, oldRight, stableGeneration, unstableGeneration );
            TreeNode.setLeftSibling( rightCursor, current, stableGeneration, unstableGeneration );

            // Do split
            bTreeNode.doSplitLeaf( cursor, keyCount, rightCursor, pos, newKey, newValue, structurePropagation.rightKey );
        }

        // Update old right with new left sibling (newRight)
        if ( TreeNode.isNode( oldRight ) )
        {
            try ( PageCursor oldRightCursor = cursor.openLinkedCursor( oldRight ) )
            {
                TreeNode.goTo( oldRightCursor, "old right sibling", oldRight );
                TreeNode.setLeftSibling( oldRightCursor, newRight, stableGeneration, unstableGeneration );
            }
        }

        // Update left child
        TreeNode.setRightSibling( cursor, newRight, stableGeneration, unstableGeneration );
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

        handleStructureChanges( cursor, structurePropagation, stableGeneration, unstableGeneration );

        if ( currentLevel <= 0 )
        {
            tryShrinkTree( cursor, structurePropagation, stableGeneration, unstableGeneration );
        }

        return into;
    }

    private void handleStructureChanges( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        while ( structurePropagation.hasLeftChildUpdate  ||
                structurePropagation.hasMidChildUpdate ||
                structurePropagation.hasRightChildUpdate ||
                structurePropagation.hasLeftKeyReplace ||
                structurePropagation.hasRightKeyReplace ||
                structurePropagation.hasRightKeyInsert )
        {
            int pos = levels[currentLevel].childPos;
            if ( !popLevel( cursor ) )
            {
                // Root split, let that be handled outside
                break;
            }

            if ( structurePropagation.hasLeftChildUpdate )
            {
                structurePropagation.hasLeftChildUpdate = false;
                if ( pos == 0 )
                {
                    updateRightmostChildInLeftSibling( cursor, structurePropagation.leftChild,
                            stableGeneration, unstableGeneration );
                }
                else
                {
                    bTreeNode.setChildAt( cursor, structurePropagation.leftChild, pos - 1,
                            stableGeneration, unstableGeneration );
                }
            }

            if ( structurePropagation.hasMidChildUpdate )
            {
                updateMidChild( cursor, structurePropagation, pos, stableGeneration, unstableGeneration );
            }

            if ( structurePropagation.hasRightChildUpdate )
            {
                structurePropagation.hasRightChildUpdate = false;
                int keyCount = TreeNode.keyCount( cursor );
                if ( pos == keyCount )
                {
                    updateLeftmostChildInRightSibling( cursor, structurePropagation.rightChild,
                            stableGeneration, unstableGeneration );
                }
                else
                {
                    bTreeNode.setChildAt( cursor, structurePropagation.rightChild, pos + 1,
                            stableGeneration, unstableGeneration );
                }
            }

            // Insert before replace because replace can lead to split and another insert in next level.
            // Replace can only come from rebalance on lower levels and because we do no rebalance among
            // internal nodes we will only ever have one replace on our way up.
            if ( structurePropagation.hasRightKeyInsert )
            {
                structurePropagation.hasRightKeyInsert = false;
                insertInInternal( cursor, structurePropagation, TreeNode.keyCount( cursor ),
                        structurePropagation.rightKey, structurePropagation.rightChild,
                        stableGeneration, unstableGeneration );
            }

            if ( structurePropagation.hasLeftKeyReplace &&
                    levels[currentLevel].covers( structurePropagation.leftKey ) )
            {
                structurePropagation.hasLeftKeyReplace = false;
                switch ( structurePropagation.keyReplaceStrategy )
                {
                case REPLACE:
                    overwriteKeyInternal( cursor, structurePropagation, structurePropagation.leftKey, pos - 1,
                            stableGeneration, unstableGeneration );
                    break;
                case BUBBLE:
                    replaceKeyByBubbleRightmostFromSubtree( cursor, structurePropagation, pos - 1,
                            stableGeneration, unstableGeneration );
                    break;
                default:
                    throw new IllegalArgumentException( "Unknown KeyReplaceStrategy " +
                            structurePropagation.keyReplaceStrategy );
                }
            }

            if ( structurePropagation.hasRightKeyReplace &&
                    levels[currentLevel].covers( structurePropagation.rightKey ) )
            {
                structurePropagation.hasRightKeyReplace = false;
                switch ( structurePropagation.keyReplaceStrategy )
                {
                case REPLACE:
                    overwriteKeyInternal( cursor, structurePropagation, structurePropagation.rightKey, pos,
                            stableGeneration, unstableGeneration );
                    break;
                case BUBBLE:
                    replaceKeyByBubbleRightmostFromSubtree( cursor, structurePropagation, pos,
                            stableGeneration, unstableGeneration );
                    break;
                default:
                    throw new IllegalArgumentException( "Unknown KeyReplaceStrategy " +
                            structurePropagation.keyReplaceStrategy );
                }
            }
        }
    }

    private void overwriteKeyInternal( PageCursor cursor, StructurePropagation<KEY> structurePropagation, KEY newKey, int pos,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                stableGeneration, unstableGeneration );
        int keyCount = TreeNode.keyCount( cursor );
        boolean couldOverwrite = bTreeNode.setKeyAtInternal( cursor, newKey, pos );
        if ( !couldOverwrite )
        {
            // Remove key and right child
            long rightChild = bTreeNode.childAt( cursor, pos + 1, stableGeneration, unstableGeneration );
            bTreeNode.removeKeyAndRightChildAt( cursor, pos, keyCount );
            TreeNode.setKeyCount( cursor, keyCount - 1 );

            doInsertInInternal( cursor, structurePropagation, keyCount - 1, newKey, rightChild, stableGeneration, unstableGeneration );
        }
    }

    private void tryShrinkTree( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        // New root will be propagated out. If rootKeyCount is 0 we can shrink the tree.
        int rootKeyCount = TreeNode.keyCount( cursor );

        while ( rootKeyCount == 0 && TreeNode.isInternal( cursor ) )
        {
            long oldRoot = cursor.getCurrentPageId();
            long onlyChildOfRoot = bTreeNode.childAt( cursor, 0, stableGeneration, unstableGeneration );
            PointerChecking.checkPointer( onlyChildOfRoot, false );

            structurePropagation.hasMidChildUpdate = true;
            structurePropagation.midChild = onlyChildOfRoot;

            idProvider.releaseId( stableGeneration, unstableGeneration, oldRoot );
            TreeNode.goTo( cursor, "child", onlyChildOfRoot );

            rootKeyCount = TreeNode.keyCount( cursor );
        }
    }

    private void updateMidChild( PageCursor cursor, StructurePropagation<KEY> structurePropagation, int childPos,
            long stableGeneration, long unstableGeneration )
    {
        structurePropagation.hasMidChildUpdate = false;
        bTreeNode.setChildAt( cursor, structurePropagation.midChild, childPos,
                stableGeneration, unstableGeneration );
    }

    private void replaceKeyByBubbleRightmostFromSubtree( PageCursor cursor,
            StructurePropagation<KEY> structurePropagation, int subtreePosition,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        long currentPageId = cursor.getCurrentPageId();
        long subtree = bTreeNode.childAt( cursor, subtreePosition, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( subtree, false );

        TreeNode.goTo( cursor, "child", subtree );
        boolean foundKeyBelow = bubbleRightmostKeyRecursive( cursor, structurePropagation, currentPageId,
                stableGeneration, unstableGeneration );

        // Propagate structurePropagation from below
        if ( structurePropagation.hasMidChildUpdate )
        {
            updateMidChild( cursor, structurePropagation, subtreePosition, stableGeneration, unstableGeneration );
        }

        if ( foundKeyBelow )
        {
            // A key has been bubble up to us.
            // It's in structurePropagation.bubbleKey and should be inserted in subtreePosition.
            overwriteKeyInternal( cursor, structurePropagation, structurePropagation.bubbleKey, subtreePosition,
                    stableGeneration, unstableGeneration );
        }
        else
        {
            // No key could be found in subtree, it's completely empty and can be removed.
            // We shift keys and children in this internal node to the left (potentially creating new version of this
            // node).
            createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                    stableGeneration, unstableGeneration);
            int keyCount = TreeNode.keyCount( cursor );
            simplyRemoveFromInternal( cursor, keyCount, subtreePosition, true );
        }
    }

    private boolean bubbleRightmostKeyRecursive( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            long previousNode, long stableGeneration, long unstableGeneration ) throws IOException
    {
        try
        {
            if ( TreeNode.isLeaf( cursor ) )
            {
                // Base case
                return false;
            }
            // Recursive case
            long currentPageId = cursor.getCurrentPageId();
            int keyCount = TreeNode.keyCount( cursor );
            long rightmostSubtree = bTreeNode.childAt( cursor, keyCount, stableGeneration, unstableGeneration );
            PointerChecking.checkPointer( rightmostSubtree, false );

            TreeNode.goTo( cursor, "child", rightmostSubtree );

            boolean foundKeyBelow = bubbleRightmostKeyRecursive( cursor, structurePropagation, currentPageId,
                    stableGeneration, unstableGeneration );

            // Propagate structurePropagation from below
            if ( structurePropagation.hasMidChildUpdate )
            {
                updateMidChild( cursor, structurePropagation, keyCount, stableGeneration, unstableGeneration );
            }

            if ( foundKeyBelow )
            {
                return true;
            }

            if ( keyCount == 0 )
            {
                // This subtree does not contain anything any more
                // Repoint sibling and add to freelist and return false
                connectLeftAndRightSibling( cursor, stableGeneration, unstableGeneration );
                idProvider.releaseId( stableGeneration, unstableGeneration, currentPageId );
                return false;
            }

            // Create new version of node, save rightmost key in structurePropagation, remove rightmost key and child
            createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD, stableGeneration, unstableGeneration );
            bTreeNode.keyAt( cursor, structurePropagation.bubbleKey, keyCount - 1, INTERNAL );
            simplyRemoveFromInternal( cursor, keyCount, keyCount - 1, false );

            return true;
        }
        finally
        {
            TreeNode.goTo( cursor, "back to previous node", previousNode );
        }
    }

    private int simplyRemoveFromInternal( PageCursor cursor, int keyCount, int keyPos, boolean leftChild )
    {
        // Remove key and child
        if ( leftChild )
        {
            bTreeNode.removeKeyAndLeftChildAt(  cursor, keyPos, keyCount );
        }
        else
        {
            bTreeNode.removeKeyAndRightChildAt( cursor, keyPos, keyCount );
        }

        // Decrease key count
        int newKeyCount = keyCount - 1;
        TreeNode.setKeyCount( cursor, newKeyCount );
        return newKeyCount;
    }

    private void updateRightmostChildInLeftSibling( PageCursor cursor, long childPointer, long stableGeneration,
            long unstableGeneration ) throws IOException
    {
        long leftSibling = TreeNode.leftSibling( cursor, stableGeneration, unstableGeneration );
        // Left sibling is not allowed to be NO_NODE here because that means there is a child node with no parent
        PointerChecking.checkPointer( leftSibling, false );

        try ( PageCursor leftSiblingCursor = cursor.openLinkedCursor( leftSibling ) )
        {
            TreeNode.goTo( leftSiblingCursor, "left sibling", leftSibling );
            int keyCount = TreeNode.keyCount( leftSiblingCursor );
            bTreeNode.setChildAt( leftSiblingCursor, childPointer, keyCount, stableGeneration, unstableGeneration );
        }
    }

    private void updateLeftmostChildInRightSibling( PageCursor cursor, long childPointer, long stableGeneration,
            long unstableGeneration ) throws IOException
    {
        long rightSibling = TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
        // Left sibling is not allowed to be NO_NODE here because that means there is a child node with no parent
        PointerChecking.checkPointer( rightSibling, false );

        try ( PageCursor rightSiblingCursor = cursor.openLinkedCursor( rightSibling ) )
        {
            TreeNode.goTo( rightSiblingCursor, "right sibling", rightSibling );
            bTreeNode.setChildAt( rightSiblingCursor, childPointer, 0, stableGeneration, unstableGeneration );
        }
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
        int keyCount = TreeNode.keyCount( cursor );

        int search = search( cursor, LEAF, key, readKey, keyCount );
        int pos = positionOf( search );
        boolean hit = isHit( search );
        if ( !hit )
        {
            return false;
        }

        createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                stableGeneration, unstableGeneration );
        keyCount = simplyRemoveFromLeaf( cursor, into, keyCount, pos );

        if ( bTreeNode.leafUnderflow( cursor, keyCount ) )
        {
            // Underflow
            underflowInLeaf( cursor, structurePropagation, keyCount, stableGeneration, unstableGeneration );
        }

        return true;
    }

    private void underflowInLeaf( PageCursor cursor, StructurePropagation<KEY> structurePropagation, int keyCount,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        long leftSibling = TreeNode.leftSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( leftSibling, true );
        long rightSibling = TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( rightSibling, true );

        if ( TreeNode.isNode( leftSibling ) )
        {
            // Go to left sibling and read stuff
            try ( PageCursor leftSiblingCursor = cursor.openLinkedCursor( GenerationSafePointerPair.pointer( leftSibling ) ) )
            {
                leftSiblingCursor.next();
                int leftSiblingKeyCount = TreeNode.keyCount( leftSiblingCursor );

                int keysToRebalance = bTreeNode.canRebalanceLeaves( leftSiblingCursor, leftSiblingKeyCount, cursor, keyCount );
                if ( keysToRebalance > 0 )
                {
                    createSuccessorIfNeeded( leftSiblingCursor, structurePropagation, UPDATE_LEFT_CHILD,
                            stableGeneration, unstableGeneration );
                    rebalanceLeaf( leftSiblingCursor, leftSiblingKeyCount, cursor, keyCount, keysToRebalance , structurePropagation );
                }
                else if ( keysToRebalance == -1 )
                {
                    // No need to create new unstable version of left sibling.
                    // Parent pointer will be updated later.
                    mergeFromLeftSiblingLeaf( cursor, leftSiblingCursor, structurePropagation, keyCount,
                            leftSiblingKeyCount, stableGeneration, unstableGeneration );
                }
            }
        }
        else if ( TreeNode.isNode( rightSibling ) )
        {
            try ( PageCursor rightSiblingCursor = cursor.openLinkedCursor(
                    GenerationSafePointerPair.pointer( rightSibling ) ) )
            {
                rightSiblingCursor.next();
                int rightSiblingKeyCount = TreeNode.keyCount( rightSiblingCursor );

                if ( bTreeNode.canMergeLeaves( cursor, keyCount, rightSiblingCursor, rightSiblingKeyCount ) )
                {
                    createSuccessorIfNeeded( rightSiblingCursor, structurePropagation, UPDATE_RIGHT_CHILD,
                            stableGeneration, unstableGeneration );
                    mergeToRightSiblingLeaf( cursor, rightSiblingCursor, structurePropagation, keyCount,
                            rightSiblingKeyCount, stableGeneration, unstableGeneration);
                }
            }
        }
    }

    private void connectLeftAndRightSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
            throws IOException
    {
        long currentId = cursor.getCurrentPageId();
        long leftSibling = TreeNode.leftSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( leftSibling, true );
        long rightSibling = TreeNode. rightSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( rightSibling, true );
        if ( TreeNode.isNode( leftSibling ) )
        {
            TreeNode.goTo( cursor, "left sibling", leftSibling );
            TreeNode.setRightSibling( cursor, rightSibling, stableGeneration, unstableGeneration );
        }
        if ( TreeNode.isNode( rightSibling ) )
        {
            TreeNode.goTo( cursor, "right sibling", rightSibling );
            TreeNode.setLeftSibling( cursor, leftSibling, stableGeneration, unstableGeneration );
        }

        TreeNode.goTo( cursor, "back to origin after repointing siblings", currentId );
    }

    private void mergeToRightSiblingLeaf( PageCursor cursor, PageCursor rightSiblingCursor,
            StructurePropagation<KEY> structurePropagation, int keyCount, int rightSiblingKeyCount,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        // Read the right-most key from the right sibling to use when comparing whether or not
        // a common parent covers the keys in right sibling too
        bTreeNode.keyAt( rightSiblingCursor, structurePropagation.rightKey, rightSiblingKeyCount - 1, LEAF );
        merge( cursor, keyCount, rightSiblingCursor, rightSiblingKeyCount, stableGeneration, unstableGeneration );

        // Propagate change
        // mid child has been merged into right child
        // right key was separator key
        structurePropagation.hasMidChildUpdate = true;
        structurePropagation.midChild = rightSiblingCursor.getCurrentPageId();
        structurePropagation.hasRightKeyReplace = true;
        structurePropagation.keyReplaceStrategy = BUBBLE;
    }

    private void mergeFromLeftSiblingLeaf( PageCursor cursor, PageCursor leftSiblingCursor,
            StructurePropagation<KEY> structurePropagation, int keyCount, int leftSiblingKeyCount,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        // Read the left-most key from the left sibling to use when comparing whether or not
        // a common parent covers the keys in left sibling too
        bTreeNode.keyAt( leftSiblingCursor, structurePropagation.leftKey, 0, LEAF );
        merge( leftSiblingCursor, leftSiblingKeyCount, cursor, keyCount, stableGeneration, unstableGeneration );

        // Propagate change
        // left child has been merged into mid child
        // left key was separator key
        structurePropagation.hasLeftChildUpdate = true;
        structurePropagation.leftChild = cursor.getCurrentPageId();
        structurePropagation.hasLeftKeyReplace = true;
        structurePropagation.keyReplaceStrategy = BUBBLE;
    }

    private void merge( PageCursor leftSiblingCursor, int leftSiblingKeyCount, PageCursor rightSiblingCursor,
            int rightSiblingKeyCount, long stableGeneration, long unstableGeneration ) throws IOException
    {
        bTreeNode.copyKeyValuesFromLeftToRight( leftSiblingCursor, leftSiblingKeyCount, rightSiblingCursor, rightSiblingKeyCount );

        // Update successor of left sibling to be right sibling
        TreeNode.setSuccessor( leftSiblingCursor, rightSiblingCursor.getCurrentPageId(),
                stableGeneration, unstableGeneration );

        // Add left sibling to free list
        connectLeftAndRightSibling( leftSiblingCursor, stableGeneration, unstableGeneration );
        idProvider.releaseId( stableGeneration, unstableGeneration, leftSiblingCursor.getCurrentPageId() );
    }

    private void rebalanceLeaf( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount,
            int numberOfKeysToMove, StructurePropagation<KEY> structurePropagation )
    {
        bTreeNode.moveKeyValuesFromLeftToRight( leftCursor, leftKeyCount, rightCursor, rightKeyCount, leftKeyCount - numberOfKeysToMove );

        // Propagate change
        structurePropagation.hasLeftKeyReplace = true;
        structurePropagation.keyReplaceStrategy = REPLACE;
        bTreeNode.keyAt( rightCursor, structurePropagation.leftKey, 0, LEAF );
    }

    /**
     * Remove key and value on given position and decrement key count. Deleted value is stored in {@code into}.
     * Key count after remove is returned.
     *
     * @param cursor Cursor pinned to node in which to remove from,
     * @param into VALUE in which to store removed value
     * @param keyCount Key count of node before remove
     * @param pos Position to remove from
     * @return keyCount after remove
     */
    private int simplyRemoveFromLeaf( PageCursor cursor, VALUE into, int keyCount, int pos )
    {
        // Save value to remove
        bTreeNode.valueAt( cursor, into, pos );
        // Remove key/value
        bTreeNode.removeKeyValueAt( cursor, pos, keyCount );

        // Decrease key count
        int newKeyCount = keyCount - 1;
        TreeNode.setKeyCount( cursor, newKeyCount );
        return newKeyCount;
    }

    /**
     * Create a new node and copy content from current node (where {@code cursor} sits) if current node is not already
     * of {@code unstableGeneration}.
     * <p>
     * Neighboring nodes' sibling pointers will be updated to point to new node.
     * <p>
     * Current node will be updated with successor pointer to new node.
     * <p>
     * {@code structurePropagation} will be updated with information about this new node so that it can report to
     * level above.
     *
     * @param cursor {@link PageCursor} pinned to page containing node to potentially create a new version of
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param structureUpdate {@link StructurePropagation.StructureUpdate} define how to update structurePropagation
     * if new unstable version is created
     * @param stableGeneration stable generation, i.e. generations <= this generation are considered stable.
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @throws IOException on cursor failure
     */
    private void createSuccessorIfNeeded( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            StructurePropagation.StructureUpdate structureUpdate, long stableGeneration, long unstableGeneration )
            throws IOException
    {
        long oldId = cursor.getCurrentPageId();
        long nodeGeneration = TreeNode.generation( cursor );
        if ( nodeGeneration == unstableGeneration )
        {
            // Don't copy
            return;
        }

        // Do copy
        long successorId = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        try ( PageCursor successorCursor = cursor.openLinkedCursor( successorId ) )
        {
            TreeNode.goTo( successorCursor, "successor", successorId );
            cursor.copyTo( 0, successorCursor, 0, cursor.getCurrentPageSize() );
            TreeNode.setGeneration( successorCursor, unstableGeneration );
            TreeNode.setSuccessor( successorCursor, TreeNode.NO_NODE_FLAG, stableGeneration, unstableGeneration );
        }

        // Insert successor pointer in old stable version
        //   (stableNode)
        //        |
        //     [successor]
        //        |
        //        v
        // (newUnstableNode)
        TreeNode.setSuccessor( cursor, successorId, stableGeneration, unstableGeneration );

        // Redirect sibling pointers
        //               ---------[leftSibling]---------(stableNode)----------[rightSibling]---------
        //              |                                     |                                      |
        //              |                                  [successor]                                    |
        //              |                                     |                                      |
        //              v                                     v                                      v
        // (leftSiblingOfStableNode) -[rightSibling]-> (newUnstableNode) <-[leftSibling]- (rightSiblingOfStableNode)
        long leftSibling = TreeNode.leftSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( leftSibling, true );
        long rightSibling = TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( rightSibling, true );
        if ( TreeNode.isNode( leftSibling ) )
        {
            TreeNode.goTo( cursor, "left sibling in split", leftSibling );
            TreeNode.setRightSibling( cursor, successorId, stableGeneration, unstableGeneration );
        }
        if ( TreeNode.isNode( rightSibling ) )
        {
            TreeNode.goTo( cursor, "right sibling in split", rightSibling );
            TreeNode.setLeftSibling( cursor, successorId, stableGeneration, unstableGeneration );
        }

        // Leave cursor at new tree node
        TreeNode.goTo( cursor, "successor", successorId );

        // Propagate structure change
        structureUpdate.update( structurePropagation, successorId );

        idProvider.releaseId( stableGeneration, unstableGeneration, oldId );
    }
}
