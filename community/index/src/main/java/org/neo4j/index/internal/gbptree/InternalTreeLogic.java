/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.index.internal.gbptree.TreeNode.Section;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.KeySearch.isHit;
import static org.neo4j.index.internal.gbptree.KeySearch.positionOf;
import static org.neo4j.index.internal.gbptree.PointerChecking.assertNoSuccessor;
import static org.neo4j.index.internal.gbptree.StructurePropagation.UPDATE_MID_CHILD;
import static org.neo4j.index.internal.gbptree.StructurePropagation.UPDATE_RIGHT_CHILD;
import static org.neo4j.index.internal.gbptree.StructurePropagation.KeyReplaceStrategy.BUBBLE;
import static org.neo4j.index.internal.gbptree.StructurePropagation.KeyReplaceStrategy.REPLACE;

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
    private final Section<KEY,VALUE> mainSection;
    private final Section<KEY,VALUE> deltaSection;
    private final Layout<KEY,VALUE> layout;
    private final KEY newKeyPlaceHolder;
    private final KEY readKey;
    private final VALUE readValue;
    // TODO: javadoc
    private final int leafMaxKeyCount;
    private final int leafMaxDeltaKeyCount;
    private final KEY[] deltaKeys;
    private final VALUE[] deltaValues;

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

    @SuppressWarnings( "unchecked" )
    InternalTreeLogic( IdProvider idProvider, TreeNode<KEY,VALUE> bTreeNode, Layout<KEY,VALUE> layout )
    {
        this.idProvider = idProvider;
        this.bTreeNode = bTreeNode;
        this.mainSection = bTreeNode.main();
        this.deltaSection = bTreeNode.delta();
        this.layout = layout;
        this.newKeyPlaceHolder = layout.newKey();
        this.readKey = layout.newKey();
        this.readValue = layout.newValue();
        this.leafMaxKeyCount = mainSection.leafMaxKeyCount();
        this.leafMaxDeltaKeyCount = deltaSection.leafMaxKeyCount();
        this.deltaKeys = (KEY[]) new Object[leafMaxDeltaKeyCount];
        this.deltaValues = (VALUE[]) new Object[leafMaxDeltaKeyCount];
        for ( int i = 0; i < leafMaxDeltaKeyCount; i++ )
        {
            deltaKeys[i] = layout.newKey();
            deltaValues[i] = layout.newValue();
        }

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
            bTreeNode.goTo( cursor, "parent", levels[currentLevel].treeNodeId );
        }

        while ( bTreeNode.isInternal( cursor ) )
        {
            // We still need to go down further, but we're on the right path
            int keyCount = mainSection.keyCount( cursor );
            int searchResult = search( cursor, key, readKey, mainSection, keyCount );
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
                    mainSection.keyAt( cursor, level.lower, childPos - 1 );
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
                    mainSection.keyAt( cursor, level.upper, childPos );
                }
            }

            long childId = mainSection.childAt( cursor, childPos, stableGeneration, unstableGeneration );
            PointerChecking.checkPointer( childId, false );

            bTreeNode.goTo( cursor, "child", childId );
            level.treeNodeId = cursor.getCurrentPageId();

            assert assertNoSuccessor( bTreeNode, cursor, stableGeneration, unstableGeneration );
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
        moveToCorrectLeaf( cursor, key, stableGeneration, unstableGeneration );

        insertInLeaf( cursor, structurePropagation, key, value, valueMerger, stableGeneration, unstableGeneration );

        while ( structurePropagation.hasMidChildUpdate || structurePropagation.hasRightKeyInsert )
        {
            int pos = levels[currentLevel].childPos;
            if ( !popLevel( cursor ) )
            {
                // Root split, let that be handled outside
                break;
            }

            if ( structurePropagation.hasMidChildUpdate )
            {
                updateMidChild( cursor, structurePropagation, pos, stableGeneration, unstableGeneration );
            }
            if ( structurePropagation.hasRightKeyInsert )
            {
                structurePropagation.hasRightKeyInsert = false;
                insertInInternal( cursor, structurePropagation, mainSection.keyCount( cursor ),
                        structurePropagation.rightKey, structurePropagation.rightChild,
                        stableGeneration, unstableGeneration );
            }
        }
    }

    private int search( PageCursor cursor, KEY key, KEY readKey, Section<KEY,VALUE> section, int keyCount )
    {
        int searchResult = KeySearch.search( cursor, section, key, readKey, keyCount );
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
        if ( keyCount < mainSection.internalMaxKeyCount() )
        {
            // No overflow
            int pos = positionOf( search( cursor, primKey, readKey, mainSection, keyCount ) );

            mainSection.insertKeyAt( cursor, primKey, pos, keyCount );
            // NOTE pos+1 since we never insert a new child before child(0) because its key is really
            // the one from the parent.
            mainSection.insertChildAt( cursor, rightChild, pos + 1, keyCount, stableGeneration, unstableGeneration );

            // Increase key count
            mainSection.setKeyCount( cursor, keyCount + 1 );

            return;
        }

        // Overflow
        // We will overwrite rightKey in structurePropagation, so copy it over to a place holder
        layout.copyKey( structurePropagation.rightKey, newKeyPlaceHolder );
        splitInternal( cursor, structurePropagation, newKeyPlaceHolder, rightChild, keyCount,
                stableGeneration, unstableGeneration );
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
        long oldRight = bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( oldRight, true );
        long newRight = idProvider.acquireNewId( stableGeneration, unstableGeneration );

        // Find position to insert new key
        int pos = positionOf( search( cursor, newKey, readKey, mainSection, keyCount ) );

        int keyCountAfterInsert = keyCount + 1;
        int middlePos = middle( keyCountAfterInsert );

        // Update structurePropagation
        structurePropagation.hasRightKeyInsert = true;
        structurePropagation.midChild = current;
        structurePropagation.rightChild = newRight;
        if ( middlePos == pos )
        {
            layout.copyKey( newKey, structurePropagation.rightKey );
        }
        else
        {
            mainSection.keyAt( cursor, structurePropagation.rightKey, pos < middlePos ? middlePos - 1 : middlePos );
        }

        // Update new right
        try ( PageCursor rightCursor = cursor.openLinkedCursor( newRight ) )
        {
            bTreeNode.goTo( rightCursor, "new right sibling in split", newRight );
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
                    mainSection.insertKeyAt( rightCursor, newKey, countBeforePos, countBeforePos );
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
                mainSection.insertChildAt( rightCursor, newRightChild, countBeforePos, countBeforePos,
                        stableGeneration, unstableGeneration );
                // ... second copy
                if ( countAfterPos > 0 )
                {
                    cursor.copyTo( bTreeNode.childOffset( pos + 1 ), rightCursor,
                            bTreeNode.childOffset( countBeforePos + 1 ), countAfterPos * bTreeNode.childSize() );
                }
            }
            mainSection.setKeyCount( rightCursor, rightKeyCount );
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
        mainSection.setKeyCount( cursor, middlePos );
        if ( pos < middlePos )
        {
            mainSection.insertKeyAt( cursor, newKey, pos, middlePos - 1 );
            mainSection.insertChildAt( cursor, newRightChild, pos + 1, middlePos - 1,
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
            KEY key, VALUE value, ValueMerger<KEY,VALUE> valueMerger,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        int deltaKeyCount = deltaSection.keyCount( cursor );
        int deltaSearch = search( cursor, key, readKey, deltaSection, deltaKeyCount );
        int deltaPos = positionOf( deltaSearch );
        if ( isHit( deltaSearch ) )
        {
            // this key already exists in the delta section so overwrite its value there
            overwriteKeyValue( cursor, structurePropagation, key, value, valueMerger,
                    stableGeneration, unstableGeneration, deltaPos, deltaSection );
            assert mainHighest( cursor );
            return; // No split has occurred
        }

        int keyCount = mainSection.keyCount( cursor );
        int search = search( cursor, key, readKey, mainSection, keyCount );
        int pos = positionOf( search );
        if ( isHit( search ) )
        {
            // this key already exists in the main section so overwrite its value there
            overwriteKeyValue( cursor, structurePropagation, key, value, valueMerger, stableGeneration,
                    unstableGeneration, pos, mainSection );
            assert mainHighest( cursor );
            return; // No split has occurred
        }

        createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                stableGeneration, unstableGeneration );

        Section<KEY,VALUE> section = selectSection( keyCount, pos, deltaKeyCount );
        if ( section != null )
        {
            int sectionPos;
            int sectionKeyCount;
            if ( section == deltaSection )
            {
                assert deltaKeyCount <= leafMaxDeltaKeyCount :
                    "deltaKeyCount:" + deltaKeyCount + " max:" + leafMaxDeltaKeyCount;
                if ( deltaKeyCount == leafMaxDeltaKeyCount )
                {
                    keyCount = consolidateDeltas( cursor, keyCount, deltaKeyCount );
                    deltaKeyCount = 0;
                    deltaPos = 0;
                }
                sectionPos = deltaPos;
                sectionKeyCount = deltaKeyCount;
            }
            else
            {
                sectionPos = pos;
                sectionKeyCount = keyCount;
            }

            // No overflow, insert key and value
            section.insertKeyAt( cursor, key, sectionPos, sectionKeyCount );
            section.insertValueAt( cursor, value, sectionPos, sectionKeyCount );
            section.setKeyCount( cursor, sectionKeyCount + 1 );

            assert mainHighest( cursor );
            return; // No split has occurred
        }

        // Overflow, split leaf
        int totalKeyCount = consolidateDeltas( cursor, keyCount, deltaKeyCount );
        splitLeaf( cursor, structurePropagation, key, value, totalKeyCount, stableGeneration, unstableGeneration );
        assert mainHighest( cursor );
    }

    private boolean mainHighest( PageCursor cursor )
    {
        int keyCount = mainSection.keyCount( cursor );
        int deltaKeyCount = deltaSection.keyCount( cursor );
        if ( keyCount > 0 && deltaKeyCount > 0 )
        {
            // compare
            mainSection.keyAt( cursor, readKey, keyCount - 1 );
            deltaSection.keyAt( cursor, deltaKeys[0], deltaKeyCount - 1 );
            if ( layout.compare( deltaKeys[0], readKey ) > 0 )
            {
                return false;
            }
        }
        else if ( keyCount > 0 )
        {
            // only stuff in main
        }
        else if ( deltaKeyCount > 0 )
        {
            // only stuff in delta
            return false;
        }
        return true;
    }

    private Section<KEY,VALUE> selectSection( int keyCount, int pos, int deltaKeyCount )
    {
        int totalKeyCount = keyCount + deltaKeyCount;
        if ( totalKeyCount < leafMaxKeyCount )
        {
            // There's room in this leaf
            if ( leafMaxDeltaKeyCount > 0 && keyCount - pos > deltaKeyCount )
            {
                // It seems to be quite a bit to the left, therefore it's better to put it in the delta section
                return deltaSection;
            }
            // It's to the far right in this leaf, just insert it right in to the main section
            return mainSection;
        }
        return null;
    }

    private int consolidateDeltas( PageCursor cursor, int keyCount, int deltaKeyCount )
    {
        if ( deltaKeyCount == 0 )
        {
            return keyCount;
        }

        // read in delta section into memory
        for ( int i = 0; i < deltaKeyCount; i++ )
        {
            deltaSection.keyAt( cursor, deltaKeys[i], i );
        }
        for ( int i = 0; i < deltaKeyCount; i++ )
        {
            deltaSection.valueAt( cursor, deltaValues[i], i );
        }

        // merge delta section into main
        int totalKeyCount = keyCount + deltaKeyCount;
        for ( int main = keyCount - 1, delta = deltaKeyCount - 1, target = totalKeyCount - 1;
                target >= 0 && delta >= 0; target-- )
        {
            int compare;
            if ( main < 0 )
            {
                compare = 1;
            }
            else
            {
                mainSection.keyAt( cursor, readKey, main );
                compare = layout.compare( deltaKeys[delta], readKey );
            }
            if ( compare > 0 )
            {
                // pick from delta
                mainSection.setKeyAt( cursor, deltaKeys[delta], target );
                mainSection.setValueAt( cursor, deltaValues[delta], target );
                delta--;
            }
            else
            {
                // pick from main
                mainSection.setKeyAt( cursor, readKey, target );
                cursor.copyTo( bTreeNode.valueOffset( main ), cursor, bTreeNode.valueOffset( target ), bTreeNode.valueSize() );
                main--;
            }
        }

        // set key counts
        mainSection.setKeyCount( cursor, totalKeyCount );
        deltaSection.setKeyCount( cursor, 0 );
        return totalKeyCount;
    }

    private void overwriteKeyValue( PageCursor cursor, StructurePropagation<KEY> structurePropagation, KEY key,
            VALUE value, ValueMerger<KEY,VALUE> valueMerger, long stableGeneration, long unstableGeneration, int pos,
            Section<KEY,VALUE> section )
            throws IOException
    {
        section.valueAt( cursor, readValue, pos );
        VALUE mergedValue = valueMerger.merge( readKey, key, readValue, value );
        if ( mergedValue != null )
        {
            createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                    stableGeneration, unstableGeneration );
            // simple, just write the merged value right in there
            section.setValueAt( cursor, mergedValue, pos );
        }
    }

    /**
     * Leaves cursor at same page as when called. No guarantees on offset.
     * Cursor is expected to be pointing to full leaf.
     *
     * NOTE: Leaf must have been already consolidated before coming in here
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
        int pos = positionOf( search( cursor, newKey, readKey, mainSection, keyCount ) );
        int keyCountAfterInsert = keyCount + 1;
        int middlePos = middle( keyCountAfterInsert );

        // allKeysIncludingNewKey should now contain all keys in sorted order and
        // allValuesIncludingNewValue should now contain all values in same order as corresponding keys
        // and are ready to be split between left and newRight.

        // We now have everything we need to start working on newRight
        // and everything that needs to be updated in left has been so.

        structurePropagation.hasRightKeyInsert = true;
        structurePropagation.midChild = current;
        structurePropagation.rightChild = newRight;

        if ( middlePos == pos )
        {
            layout.copyKey( newKey, structurePropagation.rightKey );
        }
        else
        {
            mainSection.keyAt( cursor, structurePropagation.rightKey, pos < middlePos ? middlePos - 1 : middlePos );
        }

        // Update new right
        try ( PageCursor rightCursor = cursor.openLinkedCursor( newRight ) )
        {
            bTreeNode.goTo( rightCursor, "new right sibling in split", newRight );
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
                mainSection.insertKeyAt( rightCursor, newKey, countBeforePos, countBeforePos );
                mainSection.insertValueAt( rightCursor, newValue, countBeforePos, countBeforePos );
                int countAfterPos = keyCount - pos;
                if ( countAfterPos > 0 )
                {
                    // second copy
                    copyKeysAndValues( cursor, pos, rightCursor, countBeforePos + 1, countAfterPos );
                }
            }
            mainSection.setKeyCount( rightCursor, rightKeyCount );
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
            mainSection.insertKeyAt( cursor, newKey, pos, middlePos - 1 );
            mainSection.insertValueAt( cursor, newValue, pos, middlePos - 1 );
        }
        mainSection.setKeyCount( cursor, middlePos );
        bTreeNode.setRightSibling( cursor, newRight, stableGeneration, unstableGeneration );
    }

    private void copyKeysAndValues( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count )
    {
        fromCursor.copyTo( bTreeNode.keyOffset( fromPos ), toCursor, bTreeNode.keyOffset( toPos ),
                count * bTreeNode.keySize() );
        fromCursor.copyTo( bTreeNode.valueOffset( fromPos ), toCursor, bTreeNode.valueOffset( toPos ),
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

        while ( structurePropagation.hasLeftChildUpdate  ||
                structurePropagation.hasMidChildUpdate ||
                structurePropagation.hasRightChildUpdate ||
                structurePropagation.hasLeftKeyReplace ||
                structurePropagation.hasRightKeyReplace )
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
                    mainSection.setChildAt( cursor, structurePropagation.leftChild, pos - 1,
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
                int keyCount = mainSection.keyCount( cursor );
                if ( pos == keyCount )
                {
                    updateLeftmostChildInRightSibling( cursor, structurePropagation.rightChild,
                            stableGeneration, unstableGeneration );
                }
                else
                {
                    mainSection.setChildAt( cursor, structurePropagation.rightChild, pos + 1,
                            stableGeneration, unstableGeneration );
                }
            }

            if ( structurePropagation.hasLeftKeyReplace &&
                    levels[currentLevel].covers( structurePropagation.leftKey ) )
            {
                structurePropagation.hasLeftKeyReplace = false;
                switch ( structurePropagation.keyReplaceStrategy )
                {
                case REPLACE:
                    createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                            stableGeneration, unstableGeneration );
                    mainSection.setKeyAt( cursor, structurePropagation.leftKey, pos - 1 );
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
                    createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                            stableGeneration, unstableGeneration );
                    mainSection.setKeyAt( cursor, structurePropagation.rightKey, pos );
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

        if ( currentLevel <= 0 )
        {
            tryShrinkTree( cursor, structurePropagation, stableGeneration, unstableGeneration );
        }

        return into;
    }

    private void tryShrinkTree( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        // New root will be propagated out. If rootKeyCount is 0 we can shrink the tree.
        int rootKeyCount = mainSection.keyCount( cursor );

        while ( rootKeyCount == 0 && bTreeNode.isInternal( cursor ) )
        {
            long oldRoot = cursor.getCurrentPageId();
            long onlyChildOfRoot = mainSection.childAt( cursor, 0, stableGeneration, unstableGeneration );
            PointerChecking.checkPointer( onlyChildOfRoot, false );

            structurePropagation.hasMidChildUpdate = true;
            structurePropagation.midChild = onlyChildOfRoot;

            idProvider.releaseId( stableGeneration, unstableGeneration, oldRoot );
            bTreeNode.goTo( cursor, "child", onlyChildOfRoot );

            rootKeyCount = mainSection.keyCount( cursor );
        }
    }

    private void updateMidChild( PageCursor cursor, StructurePropagation<KEY> structurePropagation, int childPos,
            long stableGeneration, long unstableGeneration )
    {
        structurePropagation.hasMidChildUpdate = false;
        mainSection.setChildAt( cursor, structurePropagation.midChild, childPos,
                stableGeneration, unstableGeneration );
    }

    private void replaceKeyByBubbleRightmostFromSubtree( PageCursor cursor,
            StructurePropagation<KEY> structurePropagation, int subtreePosition,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        long currentPageId = cursor.getCurrentPageId();
        long subtree = mainSection.childAt( cursor, subtreePosition, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( subtree, false );

        bTreeNode.goTo( cursor, "child", subtree );
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
            // It's in structurePropagation.leftKey and should be inserted in subtreePosition.
            createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                    stableGeneration, unstableGeneration );
            mainSection.setKeyAt( cursor, structurePropagation.bubbleKey, subtreePosition );
        }
        else
        {
            // No key could be found in subtree, it's completely empty and can be removed.
            // We shift keys and children in this internal node to the left (potentially creating new version of this
            // node).
            createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                    stableGeneration, unstableGeneration);
            int keyCount = mainSection.keyCount( cursor );
            simplyRemoveFromInternal( cursor, keyCount, subtreePosition, subtreePosition );
        }
    }

    private boolean bubbleRightmostKeyRecursive( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            long previousNode, long stableGeneration, long unstableGeneration ) throws IOException
    {
        try
        {
            if ( bTreeNode.isLeaf( cursor ) )
            {
                // Base case
                return false;
            }
            // Recursive case
            long currentPageId = cursor.getCurrentPageId();
            int keyCount = mainSection.keyCount( cursor );
            long rightmostSubtree = mainSection.childAt( cursor, keyCount, stableGeneration, unstableGeneration );
            PointerChecking.checkPointer( rightmostSubtree, false );

            bTreeNode.goTo( cursor, "child", rightmostSubtree );

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
            createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                    stableGeneration, unstableGeneration );
            mainSection.keyAt( cursor, structurePropagation.bubbleKey, keyCount - 1 );
            simplyRemoveFromInternal( cursor, keyCount, keyCount - 1, keyCount );

            return true;
        }
        finally
        {
            bTreeNode.goTo( cursor, "back to previous node", previousNode );
        }
    }

    private int simplyRemoveFromInternal( PageCursor cursor, int keyCount, int keyPos, int childPos )
    {
        // Remove key and child
        mainSection.removeKeyAt( cursor, keyPos, keyCount );
        mainSection.removeChildAt( cursor, childPos, keyCount );

        // Decrease key count
        int newKeyCount = keyCount - 1;
        mainSection.setKeyCount( cursor, newKeyCount );
        return newKeyCount;
    }

    private void updateRightmostChildInLeftSibling( PageCursor cursor, long childPointer, long stableGeneration,
            long unstableGeneration ) throws IOException
    {
        long leftSibling = bTreeNode.leftSibling( cursor, stableGeneration, unstableGeneration );
        // Left sibling is not allowed to be NO_NODE here because that means there is a child node with no parent
        PointerChecking.checkPointer( leftSibling, false );

        try ( PageCursor leftSiblingCursor = cursor.openLinkedCursor( leftSibling ) )
        {
            bTreeNode.goTo( leftSiblingCursor, "left sibling", leftSibling );
            int keyCount = mainSection.keyCount( leftSiblingCursor );
            mainSection.setChildAt( leftSiblingCursor, childPointer, keyCount, stableGeneration, unstableGeneration );
        }
    }

    private void updateLeftmostChildInRightSibling( PageCursor cursor, long childPointer, long stableGeneration,
            long unstableGeneration ) throws IOException
    {
        long rightSibling = bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
        // Left sibling is not allowed to be NO_NODE here because that means there is a child node with no parent
        PointerChecking.checkPointer( rightSibling, false );

        try ( PageCursor rightSiblingCursor = cursor.openLinkedCursor( rightSibling ) )
        {
            bTreeNode.goTo( rightSiblingCursor, "right sibling", rightSibling );
            mainSection.setChildAt( rightSiblingCursor, childPointer, 0, stableGeneration, unstableGeneration );
        }
    }

    /**
     * Remove given {@code key} and associated value from tree if it exists. The removed value will be stored in
     * provided {@code into} which will be returned for convenience.
     * <p>
     * If the given {@code key} does not exist in tree, return {@code null}.
     * <p>
     * Leaves cursor at same page as when called. No guarantees on offset.
     * Also this method upholds this constraint: highest key in this leaf will be in the main section.
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
        int keyCount = mainSection.keyCount( cursor );
        int deltaKeyCount = deltaSection.keyCount( cursor );
        int search = search( cursor, key, readKey, mainSection, keyCount );
        int pos = positionOf( search );
        boolean hit = isHit( search );
        if ( hit )
        {
            // we found it in main section, remove it
            createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                    stableGeneration, unstableGeneration );
            boolean lastOne = pos == keyCount - 1;
            keyCount = simplyRemoveFromLeaf( cursor, into, keyCount, pos, mainSection );

            if ( lastOne && deltaKeyCount > 0 )
            {
                // we removed the highest key from main section,
                // check if delta key is the highest now and if so move it to this pos right now
                KEY deltaKey = deltaKeys[0];
                deltaSection.keyAt( cursor, deltaKey, deltaKeyCount - 1 );
                boolean moveIt = keyCount > 0
                        // there's at least one key in main section, we have to compare
                        ? layout.compare( deltaKey, mainSection.keyAt( cursor, readKey, pos - 1 ) ) > 0
                        // the key in the delta section is definitely the highest now
                        : true;
                if ( moveIt )
                {
                    // write highest from delta into main
                    mainSection.setKeyAt( cursor, deltaKey, pos );
                    mainSection.setValueAt( cursor, deltaSection.valueAt( cursor, readValue, deltaKeyCount - 1 ), pos );
                    keyCount++;
                    mainSection.setKeyCount( cursor, keyCount );

                    // remove highest in delta
                    deltaSection.removeKeyAt( cursor, deltaKeyCount - 1, deltaKeyCount );
                    deltaSection.removeValueAt( cursor, deltaKeyCount - 1, deltaKeyCount );
                    deltaKeyCount--;
                    deltaSection.setKeyCount( cursor, deltaKeyCount );
                }
            }
        }
        else if ( deltaKeyCount > 0 )
        {
            // check delta section
            int deltaSearch = search( cursor, key, readKey, deltaSection, deltaKeyCount );
            int deltaPos = positionOf( deltaSearch );
            hit = isHit( deltaSearch );
            if ( hit )
            {
                createSuccessorIfNeeded( cursor, structurePropagation, UPDATE_MID_CHILD,
                        stableGeneration, unstableGeneration );
                simplyRemoveFromLeaf( cursor, into, deltaKeyCount, deltaPos, deltaSection );
                assert mainHighest( cursor );
                return true;
            }
        }

        assert mainHighest( cursor );
        if ( hit )
        {
            if ( keyCount + deltaKeyCount < (leafMaxKeyCount + 1) / 2 )
            {
                // Underflow
                int totalKeyCount = consolidateDeltas( cursor, keyCount, deltaKeyCount );
                underflowInLeaf( cursor, structurePropagation, totalKeyCount, stableGeneration, unstableGeneration );
            }
        }
        assert mainHighest( cursor );
        return hit;
    }

    // TODO: javadoc
    private void underflowInLeaf( PageCursor cursor, StructurePropagation<KEY> structurePropagation, int keyCount,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        long leftSibling = bTreeNode.leftSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( leftSibling, true );
        long rightSibling = bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( rightSibling, true );

        if ( TreeNode.isNode( leftSibling ) )
        {
            // Go to left sibling and read stuff
            try ( PageCursor leftSiblingCursor = cursor.openLinkedCursor( GenerationSafePointerPair.pointer( leftSibling ) ) )
            {
                leftSiblingCursor.next();
                int leftSiblingKeyCount = mainSection.keyCount( leftSiblingCursor );
                leftSiblingKeyCount = consolidateDeltas( leftSiblingCursor, leftSiblingKeyCount,
                        deltaSection.keyCount( leftSiblingCursor ) );

                if ( keyCount + leftSiblingKeyCount >= leafMaxKeyCount )
                {
                    createSuccessorIfNeeded( leftSiblingCursor, structurePropagation,
                            StructurePropagation.UPDATE_LEFT_CHILD, stableGeneration, unstableGeneration );
                    rebalanceLeaf( cursor, leftSiblingCursor, structurePropagation, keyCount, leftSiblingKeyCount );
                }
                else
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
                int rightSiblingKeyCount = mainSection.keyCount( rightSiblingCursor );
                rightSiblingKeyCount = consolidateDeltas( rightSiblingCursor, rightSiblingKeyCount,
                        deltaSection.keyCount( rightSiblingCursor ) );

                if ( keyCount + rightSiblingKeyCount <= leafMaxKeyCount )
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
        long leftSibling = bTreeNode.leftSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( leftSibling, true );
        long rightSibling = bTreeNode. rightSibling( cursor, stableGeneration, unstableGeneration );
        PointerChecking.checkPointer( rightSibling, true );
        if ( TreeNode.isNode( leftSibling ) )
        {
            bTreeNode.goTo( cursor, "left sibling", leftSibling );
            bTreeNode.setRightSibling( cursor, rightSibling, stableGeneration, unstableGeneration );
        }
        if ( TreeNode.isNode( rightSibling ) )
        {
            bTreeNode.goTo( cursor, "right sibling", rightSibling );
            bTreeNode.setLeftSibling( cursor, leftSibling, stableGeneration, unstableGeneration );
        }

        bTreeNode.goTo( cursor, "back to origin after repointing siblings", currentId );
    }

    // TODO: javadoc
    private void mergeToRightSiblingLeaf( PageCursor cursor, PageCursor rightSiblingCursor,
            StructurePropagation<KEY> structurePropagation, int keyCount, int rightSiblingKeyCount,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        merge( cursor, keyCount, rightSiblingCursor, rightSiblingKeyCount, stableGeneration, unstableGeneration );

        // Propagate change
        // mid child has been merged into right child
        // right key was separator key
        structurePropagation.hasMidChildUpdate = true;
        structurePropagation.midChild = rightSiblingCursor.getCurrentPageId();
        structurePropagation.hasRightKeyReplace = true;
        structurePropagation.keyReplaceStrategy = BUBBLE;
        mainSection.keyAt( rightSiblingCursor, structurePropagation.rightKey, rightSiblingKeyCount - 1 );
    }

    // TODO: javadoc
    private void mergeFromLeftSiblingLeaf( PageCursor cursor, PageCursor leftSiblingCursor,
            StructurePropagation<KEY> structurePropagation, int keyCount, int leftSiblingKeyCount,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        // Move stuff and update key count
        merge( leftSiblingCursor, leftSiblingKeyCount, cursor, keyCount, stableGeneration, unstableGeneration );

        // Propagate change
        // left child has been merged into mid child
        // left key was separator key
        structurePropagation.hasLeftChildUpdate = true;
        structurePropagation.leftChild = cursor.getCurrentPageId();
        structurePropagation.hasLeftKeyReplace = true;
        structurePropagation.keyReplaceStrategy = BUBBLE;
        mainSection.keyAt( cursor, structurePropagation.leftKey, 0 );
    }

    // TODO: javadoc
    private void merge( PageCursor leftSiblingCursor, int leftSiblingKeyCount, PageCursor rightSiblingCursor,
            int rightSiblingKeyCount, long stableGeneration, long unstableGeneration ) throws IOException
    {
        // Push keys in right sibling to the right
        bTreeNode.insertKeySlotsAt( rightSiblingCursor, 0, leftSiblingKeyCount, rightSiblingKeyCount );
        bTreeNode.insertValueSlotsAt( rightSiblingCursor, 0, leftSiblingKeyCount, rightSiblingKeyCount );

        // Move keys and values from left sibling to right sibling
        copyKeysAndValues( leftSiblingCursor, 0, rightSiblingCursor, 0, leftSiblingKeyCount );
        mainSection.setKeyCount( rightSiblingCursor, rightSiblingKeyCount + leftSiblingKeyCount );

        // Update successor of left sibling to be right sibling
        bTreeNode.setSuccessor( leftSiblingCursor, rightSiblingCursor.getCurrentPageId(),
                stableGeneration, unstableGeneration );

        // Add left sibling to free list
        connectLeftAndRightSibling( leftSiblingCursor, stableGeneration, unstableGeneration );
        idProvider.releaseId( stableGeneration, unstableGeneration, leftSiblingCursor.getCurrentPageId() );
    }

    // TODO: javadoc
    private void rebalanceLeaf( PageCursor cursor, PageCursor leftSiblingCursor,
            StructurePropagation<KEY> structurePropagation, int keyCount, int leftSiblingKeyCount )
    {
        int totalKeyCount = keyCount + leftSiblingKeyCount;
        int keyCountInLeftSiblingAfterRebalance = totalKeyCount / 2;
        int numberOfKeysToMove = leftSiblingKeyCount - keyCountInLeftSiblingAfterRebalance;

        // Push keys in right sibling to the right
        bTreeNode.insertKeySlotsAt( cursor, 0, numberOfKeysToMove, keyCount );
        bTreeNode.insertValueSlotsAt( cursor, 0, numberOfKeysToMove, keyCount );

        // Move keys and values from left sibling to right sibling
        copyKeysAndValues( leftSiblingCursor, keyCountInLeftSiblingAfterRebalance, cursor, 0, numberOfKeysToMove );
        mainSection.setKeyCount( cursor, keyCount + numberOfKeysToMove );
        mainSection.setKeyCount( leftSiblingCursor, leftSiblingKeyCount - numberOfKeysToMove );

        // Propagate change
        structurePropagation.hasLeftKeyReplace = true;
        structurePropagation.keyReplaceStrategy = REPLACE;
        mainSection.keyAt( cursor, structurePropagation.leftKey, 0 );
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
    private int simplyRemoveFromLeaf( PageCursor cursor, VALUE into, int keyCount, int pos, Section<KEY,VALUE> section )
    {
        // Remove key/value
        section.removeKeyAt( cursor, pos, keyCount );
        section.valueAt( cursor, into, pos );
        section.removeValueAt( cursor, pos, keyCount );

        // Decrease key count
        int newKeyCount = keyCount - 1;
        section.setKeyCount( cursor, newKeyCount );
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
        long nodeGeneration = bTreeNode.generation( cursor );
        if ( nodeGeneration == unstableGeneration )
        {
            // Don't copy
            return;
        }

        // Do copy
        long successorId = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        try ( PageCursor successorCursor = cursor.openLinkedCursor( successorId ) )
        {
            bTreeNode.goTo( successorCursor, "successor", successorId );
            cursor.copyTo( 0, successorCursor, 0, cursor.getCurrentPageSize() );
            bTreeNode.setGeneration( successorCursor, unstableGeneration );
            bTreeNode.setSuccessor( successorCursor, TreeNode.NO_NODE_FLAG, stableGeneration, unstableGeneration );
        }

        // Insert successor pointer in old stable version
        //   (stableNode)
        //        |
        //     [successor]
        //        |
        //        v
        // (newUnstableNode)
        bTreeNode.setSuccessor( cursor, successorId, stableGeneration, unstableGeneration );

        // Redirect sibling pointers
        //               ---------[leftSibling]---------(stableNode)----------[rightSibling]---------
        //              |                                     |                                      |
        //              |                                  [successor]                                    |
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
            bTreeNode.setRightSibling( cursor, successorId, stableGeneration, unstableGeneration );
        }
        if ( TreeNode.isNode( rightSibling ) )
        {
            bTreeNode.goTo( cursor, "right sibling in split", rightSibling );
            bTreeNode.setLeftSibling( cursor, successorId, stableGeneration, unstableGeneration );
        }

        // Leave cursor at new tree node
        bTreeNode.goTo( cursor, "successor", successorId );

        // Propagate structure change
        structureUpdate.update( structurePropagation, successorId );

        idProvider.releaseId( stableGeneration, unstableGeneration, oldId );
    }

    long initializeNewRootAfterSplit( PageCursor cursor, StructurePropagation<KEY> structurePropagation,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        long newRootId = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        bTreeNode.goTo( cursor, "new root", newRootId );
        bTreeNode.initializeInternal( cursor, stableGeneration, unstableGeneration );
        mainSection.insertKeyAt( cursor, structurePropagation.rightKey, 0, 0 );
        mainSection.setKeyCount( cursor, 1 );
        mainSection.setChildAt( cursor, structurePropagation.midChild, 0, stableGeneration, unstableGeneration );
        mainSection.setChildAt( cursor, structurePropagation.rightChild, 1, stableGeneration, unstableGeneration );
        return newRootId;
    }
}
