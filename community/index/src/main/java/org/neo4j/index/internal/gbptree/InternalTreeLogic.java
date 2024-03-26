/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import static org.neo4j.index.internal.gbptree.CursorCreator.bind;
import static org.neo4j.index.internal.gbptree.GBPPointerType.LEFT_SIBLING;
import static org.neo4j.index.internal.gbptree.GBPPointerType.RIGHT_SIBLING;
import static org.neo4j.index.internal.gbptree.KeySearch.childPositionOf;
import static org.neo4j.index.internal.gbptree.KeySearch.isHit;
import static org.neo4j.index.internal.gbptree.KeySearch.positionOf;
import static org.neo4j.index.internal.gbptree.Overflow.NO_NEED_DEFRAG;
import static org.neo4j.index.internal.gbptree.Overflow.YES;
import static org.neo4j.index.internal.gbptree.PointerChecking.assertNoSuccessor;
import static org.neo4j.index.internal.gbptree.StructurePropagation.KeyReplaceStrategy.BUBBLE;
import static org.neo4j.index.internal.gbptree.StructurePropagation.KeyReplaceStrategy.REPLACE;
import static org.neo4j.index.internal.gbptree.StructurePropagation.UPDATE_LEFT_CHILD;
import static org.neo4j.index.internal.gbptree.StructurePropagation.UPDATE_MID_CHILD;
import static org.neo4j.index.internal.gbptree.StructurePropagation.UPDATE_RIGHT_CHILD;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.generation;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.isInternal;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.keyCount;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.neo4j.index.internal.gbptree.MultiRootGBPTree.Monitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

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
class InternalTreeLogic<KEY, VALUE> implements InternalAccess<KEY, VALUE> {
    static final double DEFAULT_SPLIT_RATIO = 0.5;

    private final IdProvider idProvider;
    private final LeafNodeBehaviour<KEY, VALUE> leafNode;
    private final InternalNodeBehaviour<KEY> internalNode;
    private final Layout<KEY, VALUE> layout;
    private final KEY newKeyPlaceHolder;
    private final KEY readKey;
    private final ValueHolder<VALUE> readValue;
    private final Monitor monitor;
    private final TreeWriterCoordination coordination;
    final byte layerType;
    private StructureWriteLog.Session structureWriteLog;

    /**
     * Current path down the tree
     * - level:-1 is uninitialized (so that a call to {@link #initialize(PageCursor, double, StructureWriteLog.Session)} is required)
     * - level: 0 is at root
     * - level: 1 is at first level below root
     * ... a.s.o
     * <p>
     * Calling {@link #insert(PageCursor, StructurePropagation, Object, Object, ValueMerger, boolean, long, long, CursorContext)}
     * or {@link #remove(PageCursor, StructurePropagation, Object, ValueHolder, long, long, CursorContext)} leaves the cursor
     * at the last updated page (tree node id) and remembers the path down the tree to where it is.
     * Further, inserts/removals will move the cursor from its current position to where the next change will
     * take place using as few page pins as possible.
     */
    private Level<KEY>[] levels;

    private int currentLevel = -1;
    private double ratioToKeepInLeftOnSplit;

    /**
     * Keeps information about one level in a path down the tree where the {@link PageCursor} is currently at.
     *
     * @param <KEY> type of keys in the tree.
     */
    private static class Level<KEY> {
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

        Level(Layout<KEY, ?> layout) {
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
        boolean covers(KEY key) {
            boolean insideLower = lowerIsOpenEnded || layout.compare(key, lower) >= 0;
            boolean insideHigher = upperIsOpenEnded || layout.compare(key, upper) < 0;
            return insideLower && insideHigher;
        }
    }

    InternalTreeLogic(
            IdProvider idProvider,
            LeafNodeBehaviour<KEY, VALUE> leafNode,
            InternalNodeBehaviour<KEY> internalNode,
            Layout<KEY, VALUE> layout,
            Monitor monitor,
            TreeWriterCoordination coordination,
            byte layerType) {
        this.idProvider = idProvider;
        this.leafNode = leafNode;
        this.internalNode = internalNode;
        this.layout = layout;
        this.newKeyPlaceHolder = layout.newKey();
        this.readKey = layout.newKey();
        this.readValue = new ValueHolder<>(layout.newValue());
        this.monitor = monitor;
        this.coordination = coordination;
        this.layerType = layerType;

        // an arbitrary depth slightly bigger than an unimaginably big tree
        levels = new Level[10];
        levels[0] = new Level<>(layout);
    }

    @Override
    public TreeWriterCoordination coordination() {
        return coordination;
    }

    @Override
    public LeafNodeBehaviour<KEY, VALUE> leafNode() {
        return leafNode;
    }

    @Override
    public InternalNodeBehaviour<KEY> internalNode() {
        return internalNode;
    }

    /**
     * Prepare for starting over with new updates.
     * @param cursorAtRoot {@link PageCursor} pointing at root of tree.
     * @param ratioToKeepInLeftOnSplit Decide how much to keep in left node on split, 0=keep nothing, 0.5=split 50-50, 1=keep everything.
     */
    protected void initialize(
            PageCursor cursorAtRoot, double ratioToKeepInLeftOnSplit, StructureWriteLog.Session structureWriteLog) {
        currentLevel = 0;
        Level<KEY> level = levels[currentLevel];
        level.treeNodeId = cursorAtRoot.getCurrentPageId();
        level.lowerIsOpenEnded = true;
        level.upperIsOpenEnded = true;
        this.ratioToKeepInLeftOnSplit = ratioToKeepInLeftOnSplit;
        this.structureWriteLog = structureWriteLog;
    }

    void reset() {
        currentLevel = -1;
    }

    int depth() {
        return currentLevel;
    }

    private Level<KEY> descendToLevel(int currentLevel) {
        if (currentLevel >= levels.length) {
            levels = Arrays.copyOf(levels, currentLevel + 1);
        }
        var level = levels[currentLevel];
        if (level == null) {
            level = levels[currentLevel] = new Level<>(layout);
        }
        return level;
    }

    private boolean popLevel(PageCursor cursor) throws IOException {
        currentLevel--;
        if (currentLevel >= 0) {
            coordination.up();
            Level<KEY> level = levels[currentLevel];
            TreeNodeUtil.goTo(cursor, "parent", level.treeNodeId);
            return true;
        }
        // Note: else if currentLevel == -1 (i.e. we're doing structure changes and we're doing a root split/merge
        // then we cannot quite call coordination.up() since it would release the latch on the root,
        // which will probably cause concurrency issues. This means now that there's a mismatch between
        // the coordination depth and this currentLevel. This must be overcome by forcing a reset/initialize
        // from the writer on the next operation alt. right after the root split/merge.
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
     * @param cursorContext underlying page cursor context.
     * @return {@code true} if the {@link TreeWriterCoordination} permitted the move to the leaf (via its parents), otherwise {@code false}.
     * @throws IOException on {@link PageCursor} error.
     * @throws TreeInconsistencyException on seeing tree nodes of unexpected type
     */
    @Override
    public boolean moveToCorrectLeaf(
            PageCursor cursor, KEY key, long stableGeneration, long unstableGeneration, CursorContext cursorContext)
            throws IOException {
        int previousLevel = currentLevel;
        while (!levels[currentLevel].covers(key)) {
            currentLevel--;
            coordination.up();
        }
        if (currentLevel != previousLevel) {
            TreeNodeUtil.goTo(cursor, "parent", levels[currentLevel].treeNodeId);
        }

        boolean isInternal = isInternal(cursor);
        while (isInternal) {
            ensureNodeIsTreeNode(cursor, key);

            // We still need to go down further, but we're on the right path
            int keyCount = keyCount(cursor);
            int searchResult = KeySearch.search(cursor, internalNode, key, readKey, keyCount, cursorContext);
            int childPos = childPositionOf(searchResult);

            Level<KEY> parentLevel = levels[currentLevel];
            currentLevel++;
            Level<KEY> level = descendToLevel(currentLevel);

            // Restrict the key range as the cursor moves down to the next level
            level.childPos = childPos;
            level.lowerIsOpenEnded = childPos == 0
                    && !TreeNodeUtil.isNode(TreeNodeUtil.leftSibling(cursor, stableGeneration, unstableGeneration));
            if (!level.lowerIsOpenEnded) {
                if (childPos == 0) {
                    layout.copyKey(parentLevel.lower, level.lower);
                    level.lowerIsOpenEnded = parentLevel.lowerIsOpenEnded;
                } else {
                    internalNode.keyAt(cursor, level.lower, childPos - 1, cursorContext);
                }
            }
            level.upperIsOpenEnded = childPos >= keyCount
                    && !TreeNodeUtil.isNode(TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration));
            if (!level.upperIsOpenEnded) {
                if (childPos == keyCount) {
                    layout.copyKey(parentLevel.upper, level.upper);
                    level.upperIsOpenEnded = parentLevel.upperIsOpenEnded;
                } else {
                    internalNode.keyAt(cursor, level.upper, childPos, cursorContext);
                }
            }

            long childId = internalNode.childAt(cursor, childPos, stableGeneration, unstableGeneration);
            checkChildPointer(childId, cursor, childPos, internalNode, stableGeneration, unstableGeneration);

            coordination.beforeTraversingToChild(GenerationSafePointerPair.pointer(childId), childPos);
            TreeNodeUtil.goTo(cursor, "child", childId);
            level.treeNodeId = cursor.getCurrentPageId();
            int childKeyCount = keyCount(cursor);
            isInternal = isInternal(cursor);
            if (!coordination.arrivedAtChild(
                    isInternal,
                    (isInternal ? internalNode : leafNode).availableSpace(cursor, childKeyCount),
                    generation(cursor) != unstableGeneration,
                    childKeyCount)) {
                return false;
            }

            assert assertNoSuccessor(cursor, stableGeneration, unstableGeneration);
        }

        ensureNodeIsTreeNode(cursor, key);
        ensureTreeNodeIsLeaf(cursor, key);
        return true;
    }

    private void ensureNodeIsTreeNode(PageCursor cursor, KEY key) {
        if (TreeNodeUtil.nodeType(cursor) != TreeNodeUtil.NODE_TYPE_TREE_NODE) {
            throw new TreeInconsistencyException(
                    "Index update aborted due to finding tree node that doesn't have correct type (pageId: %d, type: %d), when moving cursor towards "
                            + key + ". This is most likely caused by an inconsistency in the index. ",
                    cursor.getCurrentPageId(),
                    TreeNodeUtil.nodeType(cursor));
        }
    }

    private void ensureTreeNodeIsLeaf(PageCursor cursor, KEY key) {
        if (!TreeNodeUtil.isLeaf(cursor)) {
            throw new TreeInconsistencyException(
                    "Index update aborted due to ending up on a tree node which isn't a leaf after moving cursor towards "
                            + key + ", cursor is at pageId " + cursor.getCurrentPageId()
                            + ". This is most likely caused by an inconsistency in the index.");
        }
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
     * {@link #initialize(PageCursor, double, StructureWriteLog.Session)}) or at where last insert/remove left it.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param key key to be inserted
     * @param value value to be associated with key
     * @param valueMerger {@link ValueMerger} for deciding what to do with existing keys
     * @param createIfNotExists create this key if it doesn't exist
     * @param stableGeneration stable generation, i.e. generations <= this generation are considered stable.
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @param cursorContext underlying page cursor context
     * @return {@code true} if the insertion was permitted by the {@link TreeWriterCoordination}, otherwise {@code false}.
     * @throws IOException on cursor failure
     */
    boolean insert(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            KEY key,
            VALUE value,
            ValueMerger<KEY, VALUE> valueMerger,
            boolean createIfNotExists,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        assert cursorIsAtExpectedLocation(cursor);
        leafNode.validateKeyValueSize(key, value);
        if (!moveToCorrectLeaf(cursor, key, stableGeneration, unstableGeneration, cursorContext)) {
            return false;
        }

        boolean insertSuccess = insertInLeaf(
                cursor,
                structurePropagation,
                key,
                value,
                valueMerger,
                createIfNotExists,
                stableGeneration,
                unstableGeneration,
                cursorContext);
        // insertInLeaf may have created successor so even if it fails we need to handle structure changes (successor
        // creation in parent)
        handleStructureChanges(cursor, structurePropagation, stableGeneration, unstableGeneration, cursorContext);
        return insertSuccess;
    }

    /**
     * Asserts that cursor is where it's expected to be at, compared to current level.
     *
     * @param cursor {@link PageCursor} to check.
     * @return {@code true} so that it can be called in an {@code assert} statement.
     */
    @Override
    public boolean cursorIsAtExpectedLocation(PageCursor cursor) {
        assert currentLevel >= 0 : "Uninitialized tree logic, currentLevel:" + currentLevel;
        long currentPageId = cursor.getCurrentPageId();
        long expectedPageId = levels[currentLevel].treeNodeId;
        assert currentPageId == expectedPageId
                : "Expected cursor to be at page:" + expectedPageId + " at level:" + currentLevel + ", but was at page:"
                        + currentPageId;
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
    private void insertInInternal(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            int keyCount,
            KEY primKey,
            long rightChild,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        createSuccessorIfNeeded(cursor, structurePropagation, UPDATE_MID_CHILD, stableGeneration, unstableGeneration);

        doInsertInInternal(
                cursor,
                structurePropagation,
                keyCount,
                primKey,
                rightChild,
                stableGeneration,
                unstableGeneration,
                cursorContext);
    }

    private void doInsertInInternal(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            int keyCount,
            KEY primKey,
            long rightChild,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        Overflow overflow = internalNode.overflow(cursor, keyCount, primKey);
        if (overflow == YES) {
            // Overflow
            // We will overwrite rightKey in structurePropagation, so copy it over to a place holder
            layout.copyKey(primKey, newKeyPlaceHolder);
            splitInternal(
                    cursor,
                    structurePropagation,
                    newKeyPlaceHolder,
                    rightChild,
                    keyCount,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext);
            return;
        }

        if (overflow == NO_NEED_DEFRAG) {
            internalNode.defragment(cursor, keyCount);
        }

        // No overflow
        int pos = positionOf(KeySearch.search(cursor, internalNode, primKey, readKey, keyCount, cursorContext));
        internalNode.insertKeyAndRightChildAt(
                cursor, primKey, rightChild, pos, keyCount, stableGeneration, unstableGeneration, cursorContext);
        // Increase key count
        TreeNodeUtil.setKeyCount(cursor, keyCount + 1);
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
     * @param cursorContext underlying page cursor context.
     * @throws IOException on cursor failure
     */
    private void splitInternal(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            KEY newKey,
            long newRightChild,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        long current = cursor.getCurrentPageId();
        coordination.beforeSplitInternal(current);
        long oldRight = TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration);
        checkRightSiblingPointer(oldRight, true, cursor, stableGeneration, unstableGeneration);
        long newRight = idProvider.acquireNewId(stableGeneration, unstableGeneration, bind(cursor));

        // Find position to insert new key
        int pos = positionOf(KeySearch.search(cursor, internalNode, newKey, readKey, keyCount, cursorContext));

        // Update structurePropagation
        structurePropagation.hasRightKeyInsert = true;
        structurePropagation.midChild = current;
        structurePropagation.rightChild = newRight;

        structureWriteLog.split(
                unstableGeneration,
                currentLevel > 0 ? levels[currentLevel - 1].treeNodeId : -1,
                cursor.getCurrentPageId(),
                newRight);

        try (PageCursor rightCursor = cursor.openLinkedCursor(newRight)) {
            // Initialize new right
            TreeNodeUtil.goTo(rightCursor, "new right sibling in split", newRight);
            internalNode.initialize(rightCursor, layerType, stableGeneration, unstableGeneration);
            TreeNodeUtil.setRightSibling(rightCursor, oldRight, stableGeneration, unstableGeneration);
            TreeNodeUtil.setLeftSibling(rightCursor, current, stableGeneration, unstableGeneration);

            // Do split
            internalNode.doSplit(
                    cursor,
                    keyCount,
                    rightCursor,
                    pos,
                    newKey,
                    newRightChild,
                    stableGeneration,
                    unstableGeneration,
                    structurePropagation.rightKey,
                    ratioToKeepInLeftOnSplit,
                    cursorContext);
        }

        // Update old right with new left sibling (newRight)
        if (TreeNodeUtil.isNode(oldRight)) {
            try (PageCursor oldRightCursor = cursor.openLinkedCursor(oldRight)) {
                TreeNodeUtil.goTo(oldRightCursor, "old right sibling", oldRight);
                TreeNodeUtil.setLeftSibling(oldRightCursor, newRight, stableGeneration, unstableGeneration);
            }
        }

        // Update left node with new right sibling
        TreeNodeUtil.setRightSibling(cursor, newRight, stableGeneration, unstableGeneration);
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
     * @param createIfNotExists create this key if it doesn't exist
     * @param cursorContext underlying page cursor context.
     * @return {@code true} if the insertion was permitted by the {@link TreeWriterCoordination}, otherwise {@code false}.
     * @throws IOException on cursor failure
     */
    private boolean insertInLeaf(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            KEY key,
            VALUE value,
            ValueMerger<KEY, VALUE> valueMerger,
            boolean createIfNotExists,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        int keyCount = keyCount(cursor);
        int search = KeySearch.search(cursor, leafNode, key, readKey, keyCount, cursorContext);
        int pos = positionOf(search);
        if (isHit(search)) {
            return mergeValue(
                    cursor,
                    structurePropagation,
                    key,
                    value,
                    valueMerger,
                    pos,
                    keyCount,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext,
                    createIfNotExists);
        }

        if (createIfNotExists) {
            createSuccessorIfNeeded(
                    cursor, structurePropagation, UPDATE_MID_CHILD, stableGeneration, unstableGeneration);
            return doInsertInLeaf(
                            cursor,
                            structurePropagation,
                            key,
                            value,
                            pos,
                            keyCount,
                            stableGeneration,
                            unstableGeneration,
                            cursorContext)
                    != InsertResult.SPLIT_FAIL;
        }
        return true;
    }

    /**
     * Merges a new value with a value for an existing key.
     *
     * @param cursor {@link PageCursor} pinned to page containing leaf node targeted for merge operation.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param key key to have its value merged
     * @param value value to merge with the existing value for the associated key
     * @param valueMerger {@link ValueMerger} for merging the existing value with {@code value}
     * @param pos position index the key to merge is at
     * @param keyCount number of keys in the leaf
     * @param stableGeneration stable generation, i.e. generations <= this generation are considered stable.
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @param cursorContext underlying page cursor context
     * @param createIfNotExists create this key if it doesn't exist
     * @return {@code true} if the merge operation was permitted by the {@link TreeWriterCoordination}, otherwise {@code false}.
     * @throws IOException on cursor failure
     */
    private boolean mergeValue(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            KEY key,
            VALUE value,
            ValueMerger<KEY, VALUE> valueMerger,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext,
            boolean createIfNotExists)
            throws IOException {
        // This key already exists, what shall we do? ask the valueMerger
        leafNode.valueAt(cursor, readValue, pos, cursorContext);
        int totalSpaceBefore = leafNode.totalSpaceOfKeyValue(key, readValue.value);
        var mergeResult = ValueMerger.MergeResult.REPLACED;
        if (readValue.defined) {
            mergeResult = valueMerger.merge(readKey, key, readValue.value, value);
            if (mergeResult == ValueMerger.MergeResult.UNCHANGED) {
                return true;
            }
        } else if (!createIfNotExists) {
            // multiversion tree could observe key with undefined value, it shouldn't create new value if it's not
            // requested in this case
            return true;
        }

        // Check the value size diff with coordination because the size could be reduced and may cause underflow
        int totalSpaceAfter =
                switch (mergeResult) {
                    case MERGED -> leafNode.totalSpaceOfKeyValue(key, readValue.value);
                    case REPLACED -> leafNode.totalSpaceOfKeyValue(key, value);
                    default -> 0;
                };

        int valueShrinkSize = totalSpaceBefore - totalSpaceAfter;
        if (!coordination.beforeRemovalFromLeaf(valueShrinkSize)) {
            return false;
        }

        createSuccessorIfNeeded(cursor, structurePropagation, UPDATE_MID_CHILD, stableGeneration, unstableGeneration);
        if (mergeResult == ValueMerger.MergeResult.REPLACED || mergeResult == ValueMerger.MergeResult.MERGED) {
            // First try to write the merged value right in there
            var mergedValue = mergeResult == ValueMerger.MergeResult.REPLACED ? value : readValue.value;
            return setValueAtWithFallback(
                    cursor,
                    pos,
                    key,
                    mergedValue,
                    keyCount,
                    structurePropagation,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext);
        } else if (mergeResult == ValueMerger.MergeResult.REMOVED) {
            // Remove this entry from the tree and possibly underflow while doing so
            int newKeyCount = leafNode.removeKeyValueAt(
                    cursor, pos, keyCount, stableGeneration, unstableGeneration, cursorContext);
            TreeNodeUtil.setKeyCount(cursor, newKeyCount);
            coordination.updateChildInformation(leafNode.availableSpace(cursor, newKeyCount), newKeyCount);
            if (leafNode.underflow(cursor, newKeyCount)) {
                underflowInLeaf(
                        cursor, structurePropagation, newKeyCount, stableGeneration, unstableGeneration, cursorContext);
            }
        } else {
            throw new UnsupportedOperationException("Unexpected merge result " + mergeResult);
        }
        return true;
    }

    private boolean setValueAtWithFallback(
            PageCursor cursor,
            int pos,
            KEY key,
            VALUE value,
            int keyCount,
            StructurePropagation<KEY> structurePropagation,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        if (leafNode.setValueAt(cursor, value, pos, cursorContext, stableGeneration, unstableGeneration)) {
            return true;
        }
        // Value could not be overwritten in a simple way because they differ in size.
        // Delete old value and insert w/ overflow/underflow checks.
        var newKeyCount =
                leafNode.removeKeyValueAt(cursor, pos, keyCount, stableGeneration, unstableGeneration, cursorContext);
        TreeNodeUtil.setKeyCount(cursor, newKeyCount);
        // The doInsertInLeaf below will update the child information, so no explicit update here
        var result = doInsertInLeaf(
                cursor,
                structurePropagation,
                key,
                value,
                pos,
                newKeyCount,
                stableGeneration,
                unstableGeneration,
                cursorContext);
        if (result == InsertResult.SPLIT_FAIL) {
            return false;
        }
        if (result == InsertResult.NO_SPLIT && leafNode.underflow(cursor, newKeyCount + 1)) {
            underflowInLeaf(
                    cursor, structurePropagation, newKeyCount + 1, stableGeneration, unstableGeneration, cursorContext);
        }
        return true;
    }

    /**
     * Inserts into leaf assuming already positioned cursors. Insert may involve triggering a defragmentation to free up more space,
     * and it may also entail splitting the leaf.
     *
     * @param cursor {@link PageCursor} pinned to page containing leaf node targeted for insert operation.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param key key to insert
     * @param value value to insert, associated with the {@code key}
     * @param pos position index where to insert the key/value
     * @param keyCount number of keys in the leaf
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @param cursorContext underlying page cursor context
     * @return
     *     <ul>
     *         <li>{@link InsertResult#NO_SPLIT} if insertion was made, granted by {@link TreeWriterCoordination}</li>
     *         <li>{@link InsertResult#SPLIT} if insertion was made, including a split of the leaf, both granted by {@link TreeWriterCoordination}</li>
     *         <li>{@link InsertResult#SPLIT_FAIL} if insertion required leaf split, but {@link TreeWriterCoordination} disallowed that operation.
     *         In this case no insertion or change was made.</li>
     *     <ul/>
     * @throws IOException on cursor failure
     */
    private InsertResult doInsertInLeaf(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            KEY key,
            VALUE value,
            int pos,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        Overflow overflow = leafNode.overflow(cursor, keyCount, key, value, cursorContext);
        if (overflow == YES) {
            // Overflow, split leaf
            if (!splitLeaf(
                    cursor,
                    structurePropagation,
                    key,
                    value,
                    keyCount,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext)) {
                return InsertResult.SPLIT_FAIL;
            }
            return InsertResult.SPLIT;
        }

        InsertResult result = InsertResult.NO_SPLIT;
        if (overflow == NO_NEED_DEFRAG) {
            int keyCountAfterDeframent = leafNode.defragment(cursor, keyCount, cursorContext);
            if (keyCountAfterDeframent != keyCount) {
                keyCount = keyCountAfterDeframent;
                // need to find new insert position
                pos = positionOf(KeySearch.search(cursor, leafNode, key, readKey, keyCount, cursorContext));
                // tell caller to skip underflow after this defragment.
                // we can't check with coordination before defragment if it will trigger underflow to switch to
                // pessimistic mode, because we don't know how much keys will be removed by it
                result = InsertResult.SPLIT_SKIP_UNDERFLOW;
            }
        }

        // No overflow, insert key and value
        leafNode.insertKeyValueAt(
                cursor, key, value, pos, keyCount, stableGeneration, unstableGeneration, cursorContext);
        int newKeyCount = keyCount + 1;
        TreeNodeUtil.setKeyCount(cursor, newKeyCount);
        // This update will cover both the defrag and insert
        coordination.updateChildInformation(leafNode.availableSpace(cursor, newKeyCount), newKeyCount);
        return result;
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
     * @return {@code true} if {@link TreeWriterCoordination} permitted the split operation, otherwise {@code false}.
     * @throws IOException on cursor failure
     */
    private boolean splitLeaf(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            KEY newKey,
            VALUE newValue,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
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

        // Position where newKey / newValue is to be inserted
        int pos = positionOf(KeySearch.search(cursor, leafNode, newKey, readKey, keyCount, cursorContext));
        // Position where to split
        int middlePos = leafNode.findSplitter(
                cursor,
                keyCount,
                newKey,
                newValue,
                pos,
                structurePropagation.rightKey,
                ratioToKeepInLeftOnSplit,
                cursorContext);
        if (!coordination.beforeSplittingLeaf(internalNode.totalSpaceOfKeyChild(structurePropagation.rightKey))) {
            return false;
        }

        long current = cursor.getCurrentPageId();
        long oldRight = TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration);
        checkRightSiblingPointer(oldRight, true, cursor, stableGeneration, unstableGeneration);
        long newRight = idProvider.acquireNewId(stableGeneration, unstableGeneration, bind(cursor));

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

        structurePropagation.hasRightKeyInsert = true;
        structurePropagation.midChild = current;
        structurePropagation.rightChild = newRight;

        structureWriteLog.split(
                unstableGeneration,
                currentLevel > 0 ? levels[currentLevel - 1].treeNodeId : -1,
                cursor.getCurrentPageId(),
                newRight);

        try (PageCursor rightCursor = cursor.openLinkedCursor(newRight)) {
            // Initialize new right
            TreeNodeUtil.goTo(rightCursor, "new right sibling in split", newRight);
            leafNode.initialize(rightCursor, layerType, stableGeneration, unstableGeneration);
            TreeNodeUtil.setRightSibling(rightCursor, oldRight, stableGeneration, unstableGeneration);
            TreeNodeUtil.setLeftSibling(rightCursor, current, stableGeneration, unstableGeneration);

            // Do split
            leafNode.doSplit(
                    cursor,
                    keyCount,
                    rightCursor,
                    pos,
                    newKey,
                    newValue,
                    structurePropagation.rightKey,
                    middlePos,
                    ratioToKeepInLeftOnSplit,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext);
        }

        // Update old right with new left sibling (newRight)
        if (TreeNodeUtil.isNode(oldRight)) {
            try (PageCursor oldRightCursor = cursor.openLinkedCursor(oldRight)) {
                TreeNodeUtil.goTo(oldRightCursor, "old right sibling", oldRight);
                TreeNodeUtil.setLeftSibling(oldRightCursor, newRight, stableGeneration, unstableGeneration);
            }
        }

        // Update left child
        TreeNodeUtil.setRightSibling(cursor, newRight, stableGeneration, unstableGeneration);
        return true;
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
     * {@link #initialize(PageCursor, double, StructureWriteLog.Session)}) or at where last insert/remove left it.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param key key to be removed
     * @param into {@code VALUE} instance to write removed value to
     * @param stableGeneration stable generation, i.e. generations <= this generation are considered stable.
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @return
     * <ul>
     *     <li>{@link RemoveResult#REMOVED} if the key was found, where the removed value is now written in the {@code into} object</li>
     *     <li>{@link RemoveResult#NOT_FOUND} if the key was not found</li>
     *     <li>{@link RemoveResult#FAIL} if the remove operation was not permitted by the {@link TreeWriterCoordination}</li>
     * </ul>
     * @throws IOException on cursor failure
     */
    RemoveResult remove(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            KEY key,
            ValueHolder<VALUE> into,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        assert cursorIsAtExpectedLocation(cursor);
        if (!moveToCorrectLeaf(cursor, key, stableGeneration, unstableGeneration, cursorContext)) {
            return RemoveResult.FAIL;
        }

        RemoveResult result = removeFromLeaf(
                cursor, structurePropagation, key, into, stableGeneration, unstableGeneration, cursorContext);
        if (result == RemoveResult.REMOVED) {
            handleStructureChanges(cursor, structurePropagation, stableGeneration, unstableGeneration, cursorContext);
            tryShrinkTree(cursor, structurePropagation, stableGeneration, unstableGeneration);
        }
        return result;
    }

    @Override
    public void handleStructureChanges(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        while (structurePropagation.hasLeftChildUpdate
                || structurePropagation.hasMidChildUpdate
                || structurePropagation.hasRightChildUpdate
                || structurePropagation.hasLeftKeyReplace
                || structurePropagation.hasRightKeyReplace
                || structurePropagation.hasRightKeyInsert) {
            int pos = levels[currentLevel].childPos;
            if (!popLevel(cursor)) {
                // Root split, let that be handled outside
                break;
            }

            if (structurePropagation.hasLeftChildUpdate) {
                structurePropagation.hasLeftChildUpdate = false;
                if (pos == 0) {
                    updateRightmostChildInLeftSibling(
                            cursor, structurePropagation.leftChild, stableGeneration, unstableGeneration);
                } else {
                    internalNode.setChildAt(
                            cursor, structurePropagation.leftChild, pos - 1, stableGeneration, unstableGeneration);
                }
            }

            if (structurePropagation.hasMidChildUpdate) {
                updateMidChild(cursor, structurePropagation, pos, stableGeneration, unstableGeneration);
            }

            if (structurePropagation.hasRightChildUpdate) {
                structurePropagation.hasRightChildUpdate = false;
                int keyCount = keyCount(cursor);
                if (pos == keyCount) {
                    updateLeftmostChildInRightSibling(
                            cursor, structurePropagation.rightChild, stableGeneration, unstableGeneration);
                } else {
                    internalNode.setChildAt(
                            cursor, structurePropagation.rightChild, pos + 1, stableGeneration, unstableGeneration);
                }
            }

            // Insert before replace because replace can lead to split and another insert in next level.
            // Replace can only come from rebalance on lower levels and because we do no rebalance among
            // internal nodes we will only ever have one replace on our way up.
            if (structurePropagation.hasRightKeyInsert) {
                structurePropagation.hasRightKeyInsert = false;
                insertInInternal(
                        cursor,
                        structurePropagation,
                        keyCount(cursor),
                        structurePropagation.rightKey,
                        structurePropagation.rightChild,
                        stableGeneration,
                        unstableGeneration,
                        cursorContext);
            }

            if (structurePropagation.hasLeftKeyReplace && levels[currentLevel].covers(structurePropagation.leftKey)) {
                assert pos > 0 : "attempt to replace key left to the leftmost key";
                structurePropagation.hasLeftKeyReplace = false;
                switch (structurePropagation.keyReplaceStrategy) {
                    case REPLACE -> overwriteKeyInternal(
                            cursor,
                            structurePropagation,
                            structurePropagation.leftKey,
                            pos - 1,
                            stableGeneration,
                            unstableGeneration,
                            cursorContext);
                    case BUBBLE -> replaceKeyByBubbleRightmostFromSubtree(
                            cursor, structurePropagation, pos - 1, stableGeneration, unstableGeneration, cursorContext);
                }
            }

            if (structurePropagation.hasRightKeyReplace && levels[currentLevel].covers(structurePropagation.rightKey)) {
                structurePropagation.hasRightKeyReplace = false;
                switch (structurePropagation.keyReplaceStrategy) {
                    case REPLACE -> {
                        int keyCount = keyCount(cursor);
                        if (keyCount != pos) {
                            overwriteKeyInternal(
                                    cursor,
                                    structurePropagation,
                                    structurePropagation.rightKey,
                                    pos,
                                    stableGeneration,
                                    unstableGeneration,
                                    cursorContext);
                        } else {
                            // we are at the right boundary of the internal node, actual key that should be replaced is
                            // above us
                            // PS: replacing leftmost key in the right sibling isn't correct here because that key is
                            // the left boundary for the right sibling of our right sibling
                            structurePropagation.hasRightKeyReplace = true;
                        }
                    }
                    case BUBBLE -> replaceKeyByBubbleRightmostFromSubtree(
                            cursor, structurePropagation, pos, stableGeneration, unstableGeneration, cursorContext);
                }
            }

            var keyCountAfterUpdate = keyCount(cursor);
            coordination.updateChildInformation(
                    internalNode.availableSpace(cursor, keyCountAfterUpdate), keyCountAfterUpdate);
        }
    }

    private void overwriteKeyInternal(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            KEY newKey,
            int pos,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        createSuccessorIfNeeded(cursor, structurePropagation, UPDATE_MID_CHILD, stableGeneration, unstableGeneration);
        boolean couldOverwrite = internalNode.setKeyAt(cursor, newKey, pos);
        if (!couldOverwrite) {
            int keyCount = keyCount(cursor);
            // Remove key and right child
            long rightChild = internalNode.childAt(cursor, pos + 1, stableGeneration, unstableGeneration);
            internalNode.removeKeyAndRightChildAt(
                    cursor, pos, keyCount, stableGeneration, unstableGeneration, cursorContext);
            TreeNodeUtil.setKeyCount(cursor, keyCount - 1);

            doInsertInInternal(
                    cursor,
                    structurePropagation,
                    keyCount - 1,
                    newKey,
                    rightChild,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext);
        }
    }

    @Override
    public void tryShrinkTree(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            long stableGeneration,
            long unstableGeneration)
            throws IOException {
        if (currentLevel > 0) {
            return;
        }
        // New root will be propagated out. If rootKeyCount is 0 we can shrink the tree.
        int rootKeyCount = keyCount(cursor);

        while (rootKeyCount == 0 && isInternal(cursor)) {
            long oldRoot = cursor.getCurrentPageId();
            long onlyChildOfRoot = internalNode.childAt(cursor, 0, stableGeneration, unstableGeneration);
            checkChildPointer(onlyChildOfRoot, cursor, 0, internalNode, stableGeneration, unstableGeneration);

            structurePropagation.hasMidChildUpdate = true;
            structurePropagation.midChild = onlyChildOfRoot;

            structureWriteLog.shrinkTree(unstableGeneration, oldRoot);
            structureWriteLog.addToFreelist(unstableGeneration, oldRoot);
            idProvider.releaseId(stableGeneration, unstableGeneration, oldRoot, bind(cursor));
            TreeNodeUtil.goTo(cursor, "child", onlyChildOfRoot);

            rootKeyCount = keyCount(cursor);
            monitor.treeShrink();
        }
    }

    private void updateMidChild(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            int childPos,
            long stableGeneration,
            long unstableGeneration) {
        structurePropagation.hasMidChildUpdate = false;
        internalNode.setChildAt(cursor, structurePropagation.midChild, childPos, stableGeneration, unstableGeneration);
    }

    private void replaceKeyByBubbleRightmostFromSubtree(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            int subtreePosition,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        long currentPageId = cursor.getCurrentPageId();
        long subtree = internalNode.childAt(cursor, subtreePosition, stableGeneration, unstableGeneration);
        checkChildPointer(subtree, cursor, subtreePosition, internalNode, stableGeneration, unstableGeneration);

        TreeNodeUtil.goTo(cursor, "child", subtree);
        boolean foundKeyBelow = bubbleRightmostKeyRecursive(
                cursor, structurePropagation, currentPageId, stableGeneration, unstableGeneration, cursorContext);

        // Propagate structurePropagation from below
        if (structurePropagation.hasMidChildUpdate) {
            updateMidChild(cursor, structurePropagation, subtreePosition, stableGeneration, unstableGeneration);
        }

        if (foundKeyBelow) {
            // A key has been bubble up to us.
            // It's in structurePropagation.bubbleKey and should be inserted in subtreePosition.
            overwriteKeyInternal(
                    cursor,
                    structurePropagation,
                    structurePropagation.bubbleKey,
                    subtreePosition,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext);
        } else {
            // No key could be found in subtree, it's completely empty and can be removed.
            // We shift keys and children in this internal node to the left (potentially creating new version of this
            // node).
            createSuccessorIfNeeded(
                    cursor, structurePropagation, UPDATE_MID_CHILD, stableGeneration, unstableGeneration);
            int keyCount = keyCount(cursor);
            simplyRemoveFromInternal(
                    cursor, keyCount, subtreePosition, true, stableGeneration, unstableGeneration, cursorContext);
            tryShrinkTree(cursor, structurePropagation, stableGeneration, unstableGeneration);
        }
    }

    private boolean bubbleRightmostKeyRecursive(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            long previousNode,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        try {
            if (TreeNodeUtil.isLeaf(cursor)) {
                // Base case
                return false;
            }
            // Recursive case
            long currentPageId = cursor.getCurrentPageId();
            int keyCount = keyCount(cursor);
            long rightmostSubtree = internalNode.childAt(cursor, keyCount, stableGeneration, unstableGeneration);
            checkChildPointer(rightmostSubtree, cursor, keyCount, internalNode, stableGeneration, unstableGeneration);

            TreeNodeUtil.goTo(cursor, "child", rightmostSubtree);

            boolean foundKeyBelow = bubbleRightmostKeyRecursive(
                    cursor, structurePropagation, currentPageId, stableGeneration, unstableGeneration, cursorContext);

            // Propagate structurePropagation from below
            if (structurePropagation.hasMidChildUpdate) {
                updateMidChild(cursor, structurePropagation, keyCount, stableGeneration, unstableGeneration);
            }

            if (foundKeyBelow) {
                return true;
            }

            if (keyCount == 0) {
                // This subtree does not contain anything any more
                // Repoint sibling and add to freelist and return false
                connectLeftAndRightSibling(cursor, stableGeneration, unstableGeneration);
                structureWriteLog.addToFreelist(unstableGeneration, currentPageId);
                idProvider.releaseId(stableGeneration, unstableGeneration, currentPageId, bind(cursor));
                return false;
            }

            // Create new version of node, save rightmost key in structurePropagation, remove rightmost key and child
            createSuccessorIfNeeded(
                    cursor, structurePropagation, UPDATE_MID_CHILD, stableGeneration, unstableGeneration);
            internalNode.keyAt(cursor, structurePropagation.bubbleKey, keyCount - 1, cursorContext);
            simplyRemoveFromInternal(
                    cursor, keyCount, keyCount - 1, false, stableGeneration, unstableGeneration, cursorContext);

            return true;
        } finally {
            TreeNodeUtil.goTo(cursor, "back to previous node", previousNode);
        }
    }

    private void simplyRemoveFromInternal(
            PageCursor cursor,
            int keyCount,
            int keyPos,
            boolean leftChild,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        // Remove key and child
        if (leftChild) {
            internalNode.removeKeyAndLeftChildAt(
                    cursor, keyPos, keyCount, stableGeneration, unstableGeneration, cursorContext);
        } else {
            internalNode.removeKeyAndRightChildAt(
                    cursor, keyPos, keyCount, stableGeneration, unstableGeneration, cursorContext);
        }

        // Decrease key count
        TreeNodeUtil.setKeyCount(cursor, keyCount - 1);
    }

    private void updateRightmostChildInLeftSibling(
            PageCursor cursor, long childPointer, long stableGeneration, long unstableGeneration) throws IOException {
        long leftSibling = TreeNodeUtil.leftSibling(cursor, stableGeneration, unstableGeneration);
        // Left sibling is not allowed to be NO_NODE here because that means there is a child node with no parent
        checkLeftSiblingPointer(leftSibling, false, cursor, stableGeneration, unstableGeneration);

        try (PageCursor leftSiblingCursor = cursor.openLinkedCursor(leftSibling)) {
            TreeNodeUtil.goTo(leftSiblingCursor, "left sibling", leftSibling);
            int keyCount = keyCount(leftSiblingCursor);
            internalNode.setChildAt(leftSiblingCursor, childPointer, keyCount, stableGeneration, unstableGeneration);
        }
    }

    private void updateLeftmostChildInRightSibling(
            PageCursor cursor, long childPointer, long stableGeneration, long unstableGeneration) throws IOException {
        long rightSibling = TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration);
        // Right sibling is not allowed to be NO_NODE here because that means there is a child node with no parent
        checkRightSiblingPointer(rightSibling, false, cursor, stableGeneration, unstableGeneration);

        try (PageCursor rightSiblingCursor = cursor.openLinkedCursor(rightSibling)) {
            TreeNodeUtil.goTo(rightSiblingCursor, "right sibling", rightSibling);
            internalNode.setChildAt(rightSiblingCursor, childPointer, 0, stableGeneration, unstableGeneration);
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
    private RemoveResult removeFromLeaf(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            KEY key,
            ValueHolder<VALUE> into,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        int keyCount = keyCount(cursor);
        int search = KeySearch.search(cursor, leafNode, key, readKey, keyCount, cursorContext);
        int pos = positionOf(search);
        boolean hit = isHit(search);
        if (!hit) {
            // true in that the operation was successful (idempotent remove)
            return RemoveResult.NOT_FOUND;
        }

        leafNode.valueAt(cursor, into, pos, cursorContext);
        if (!coordination.beforeRemovalFromLeaf(leafNode.totalSpaceRemovedOfKeyValue(key, into.value))) {
            return RemoveResult.FAIL;
        }
        createSuccessorIfNeeded(cursor, structurePropagation, UPDATE_MID_CHILD, stableGeneration, unstableGeneration);
        keyCount = simplyRemoveFromLeaf(cursor, keyCount, pos, stableGeneration, unstableGeneration, cursorContext);

        if (leafNode.underflow(cursor, keyCount)) {
            // Underflow
            underflowInLeaf(
                    cursor, structurePropagation, keyCount, stableGeneration, unstableGeneration, cursorContext);
        } else {
            coordination.updateChildInformation(leafNode.availableSpace(cursor, keyCount), keyCount);
        }

        return RemoveResult.REMOVED;
    }

    /**
     * Called when a leaf has more than half its space available. First looks at the left sibling for either moving some of its entries
     * to this leaf, or if all entries from this leaf plus all entries from the left sibling leaf can fit into this leaf and if so merge
     * the left sibling into this leaf. If this leaf doesn't have a left sibling, the right sibling is checked whether or not this leaf
     * can be merged into the right sibling.
     *
     * @param cursor pinned to the leaf that underflowed.
     * @param structurePropagation {@link StructurePropagation} used to report structure changes between tree levels.
     * @param keyCount number of keys in the leaf that underflowed.
     * @param stableGeneration stable generation, i.e. generations <= this generation are considered stable.
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @param cursorContext underlying page cursor context.
     * @throws IOException on page access I/O error.
     */
    @Override
    public void underflowInLeaf(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            int keyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        coordination.beforeUnderflowInLeaf(cursor.getCurrentPageId());
        long leftSibling = TreeNodeUtil.leftSibling(cursor, stableGeneration, unstableGeneration);
        checkLeftSiblingPointer(leftSibling, true, cursor, stableGeneration, unstableGeneration);
        long rightSibling = TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration);
        checkRightSiblingPointer(rightSibling, true, cursor, stableGeneration, unstableGeneration);

        if (TreeNodeUtil.isNode(leftSibling)) {
            // Go to left sibling and read stuff
            try (PageCursor leftSiblingCursor =
                    cursor.openLinkedCursor(GenerationSafePointerPair.pointer(leftSibling))) {
                leftSiblingCursor.next();
                int leftSiblingKeyCount = keyCount(leftSiblingCursor);

                int keysToRebalance = leafNode.canRebalance(leftSiblingCursor, leftSiblingKeyCount, cursor, keyCount);
                if (keysToRebalance > 0) {
                    createSuccessorIfNeeded(
                            leftSiblingCursor,
                            structurePropagation,
                            UPDATE_LEFT_CHILD,
                            stableGeneration,
                            unstableGeneration);
                    rebalanceLeaf(
                            leftSiblingCursor,
                            leftSiblingKeyCount,
                            cursor,
                            keyCount,
                            keysToRebalance,
                            structurePropagation,
                            cursorContext);
                } else if (keysToRebalance == -1) {
                    // No need to create new unstable version of left sibling.
                    // Parent pointer will be updated later.
                    mergeFromLeftSiblingLeaf(
                            cursor,
                            leftSiblingCursor,
                            structurePropagation,
                            keyCount,
                            leftSiblingKeyCount,
                            stableGeneration,
                            unstableGeneration,
                            cursorContext,
                            bind(leftSiblingCursor));
                }
            }
        } else if (TreeNodeUtil.isNode(rightSibling)) {
            try (PageCursor rightSiblingCursor =
                    cursor.openLinkedCursor(GenerationSafePointerPair.pointer(rightSibling))) {
                rightSiblingCursor.next();
                int rightSiblingKeyCount = keyCount(rightSiblingCursor);

                if (leafNode.canMerge(cursor, keyCount, rightSiblingCursor, rightSiblingKeyCount)) {
                    createSuccessorIfNeeded(
                            rightSiblingCursor,
                            structurePropagation,
                            UPDATE_RIGHT_CHILD,
                            stableGeneration,
                            unstableGeneration);
                    mergeToRightSiblingLeaf(
                            cursor,
                            rightSiblingCursor,
                            structurePropagation,
                            keyCount,
                            rightSiblingKeyCount,
                            stableGeneration,
                            unstableGeneration,
                            cursorContext,
                            bind(rightSiblingCursor));
                }
            }
        }
    }

    private static void connectLeftAndRightSibling(PageCursor cursor, long stableGeneration, long unstableGeneration)
            throws IOException {
        long currentId = cursor.getCurrentPageId();
        long leftSibling = TreeNodeUtil.leftSibling(cursor, stableGeneration, unstableGeneration);
        checkLeftSiblingPointer(leftSibling, true, cursor, stableGeneration, unstableGeneration);
        long rightSibling = TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration);
        checkRightSiblingPointer(rightSibling, true, cursor, stableGeneration, unstableGeneration);
        if (TreeNodeUtil.isNode(leftSibling)) {
            TreeNodeUtil.goTo(cursor, "left sibling", leftSibling);
            TreeNodeUtil.setRightSibling(cursor, rightSibling, stableGeneration, unstableGeneration);
        }
        if (TreeNodeUtil.isNode(rightSibling)) {
            TreeNodeUtil.goTo(cursor, "right sibling", rightSibling);
            TreeNodeUtil.setLeftSibling(cursor, leftSibling, stableGeneration, unstableGeneration);
        }

        TreeNodeUtil.goTo(cursor, "back to origin after repointing siblings", currentId);
    }

    private void mergeToRightSiblingLeaf(
            PageCursor cursor,
            PageCursor rightSiblingCursor,
            StructurePropagation<KEY> structurePropagation,
            int keyCount,
            int rightSiblingKeyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext,
            CursorCreator linkedCursorCreator)
            throws IOException {
        // Read the right-most key from the right sibling to use when comparing whether or not
        // a common parent covers the keys in right sibling too
        assert rightSiblingKeyCount > 0 : "trying to read the last key from the empty leaf";
        leafNode.keyAt(rightSiblingCursor, structurePropagation.rightKey, rightSiblingKeyCount - 1, cursorContext);

        structureWriteLog.merge(
                unstableGeneration,
                levels[currentLevel - 1].treeNodeId,
                rightSiblingCursor.getCurrentPageId(),
                cursor.getCurrentPageId());

        merge(
                cursor,
                keyCount,
                rightSiblingCursor,
                rightSiblingKeyCount,
                stableGeneration,
                unstableGeneration,
                linkedCursorCreator,
                cursorContext);

        // Propagate change
        // mid child has been merged into right child
        // right key was separator key
        structurePropagation.hasMidChildUpdate = true;
        structurePropagation.midChild = rightSiblingCursor.getCurrentPageId();
        structurePropagation.hasRightKeyReplace = true;
        structurePropagation.keyReplaceStrategy = BUBBLE;
    }

    private void mergeFromLeftSiblingLeaf(
            PageCursor cursor,
            PageCursor leftSiblingCursor,
            StructurePropagation<KEY> structurePropagation,
            int keyCount,
            int leftSiblingKeyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext,
            CursorCreator linkedCursorCreator)
            throws IOException {
        // Read the left-most key from the left sibling to use when comparing whether or not
        // a common parent covers the keys in left sibling too
        assert leftSiblingKeyCount > 0 : "trying to read the first key from the empty leaf";
        leafNode.keyAt(leftSiblingCursor, structurePropagation.leftKey, 0, cursorContext);

        structureWriteLog.merge(
                unstableGeneration,
                levels[currentLevel - 1].treeNodeId,
                cursor.getCurrentPageId(),
                leftSiblingCursor.getCurrentPageId());

        merge(
                leftSiblingCursor,
                leftSiblingKeyCount,
                cursor,
                keyCount,
                stableGeneration,
                unstableGeneration,
                linkedCursorCreator,
                cursorContext);

        // Propagate change
        // left child has been merged into mid child
        // left key was separator key
        structurePropagation.hasLeftChildUpdate = true;
        structurePropagation.leftChild = cursor.getCurrentPageId();
        structurePropagation.hasLeftKeyReplace = true;
        structurePropagation.keyReplaceStrategy = BUBBLE;
    }

    private void merge(
            PageCursor leftSiblingCursor,
            int leftSiblingKeyCount,
            PageCursor rightSiblingCursor,
            int rightSiblingKeyCount,
            long stableGeneration,
            long unstableGeneration,
            CursorCreator linkedCursorCreator,
            CursorContext cursorContext)
            throws IOException {
        leafNode.copyKeyValuesFromLeftToRight(
                leftSiblingCursor, leftSiblingKeyCount, rightSiblingCursor, rightSiblingKeyCount, cursorContext);

        // Update successor of left sibling to be right sibling
        TreeNodeUtil.setSuccessor(
                leftSiblingCursor, rightSiblingCursor.getCurrentPageId(), stableGeneration, unstableGeneration);

        // Add left sibling to free list
        connectLeftAndRightSibling(leftSiblingCursor, stableGeneration, unstableGeneration);
        structureWriteLog.addToFreelist(unstableGeneration, leftSiblingCursor.getCurrentPageId());
        idProvider.releaseId(
                stableGeneration, unstableGeneration, leftSiblingCursor.getCurrentPageId(), linkedCursorCreator);
    }

    private void rebalanceLeaf(
            PageCursor leftCursor,
            int leftKeyCount,
            PageCursor rightCursor,
            int rightKeyCount,
            int numberOfKeysToMove,
            StructurePropagation<KEY> structurePropagation,
            CursorContext cursorContext)
            throws IOException {
        leafNode.moveKeyValuesFromLeftToRight(
                leftCursor, leftKeyCount, rightCursor, rightKeyCount, leftKeyCount - numberOfKeysToMove, cursorContext);

        // Propagate change
        structurePropagation.hasLeftKeyReplace = true;
        structurePropagation.keyReplaceStrategy = REPLACE;
        leafNode.keyAt(rightCursor, structurePropagation.leftKey, 0, cursorContext);
    }

    /**
     * Remove key and value on given position and decrement key count. Deleted value is stored in {@code into}.
     * Key count after remove is returned.
     *
     * @param cursor Cursor pinned to node in which to remove from,
     * @param keyCount Key count of node before remove
     * @param pos Position to remove from
     * @param stableGeneration stable generation, i.e. generations <= this generation are considered stable.
     * @param unstableGeneration unstable generation, i.e. generation which is under development right now.
     * @param cursorContext underlying page cursor context.
     * @return keyCount after remove
     */
    private int simplyRemoveFromLeaf(
            PageCursor cursor,
            int keyCount,
            int pos,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext)
            throws IOException {
        // Remove key/value
        int newKeyCount =
                leafNode.removeKeyValueAt(cursor, pos, keyCount, stableGeneration, unstableGeneration, cursorContext);
        TreeNodeUtil.setKeyCount(cursor, newKeyCount);
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
    @Override
    public void createSuccessorIfNeeded(
            PageCursor cursor,
            StructurePropagation<KEY> structurePropagation,
            StructurePropagation.StructureUpdate structureUpdate,
            long stableGeneration,
            long unstableGeneration)
            throws IOException {
        long oldId = cursor.getCurrentPageId();
        long nodeGeneration = generation(cursor);
        if (nodeGeneration == unstableGeneration) {
            // Don't copy
            return;
        }

        // Do copy
        long successorId = idProvider.acquireNewId(stableGeneration, unstableGeneration, bind(cursor));
        structureWriteLog.createSuccessor(
                unstableGeneration,
                currentLevel > 0 ? levels[currentLevel - 1].treeNodeId : -1,
                cursor.getCurrentPageId(),
                successorId);
        try (PageCursor successorCursor = cursor.openLinkedCursor(successorId)) {
            TreeNodeUtil.goTo(successorCursor, "successor", successorId);
            cursor.copyTo(0, successorCursor, 0, cursor.getPagedFile().payloadSize());
            TreeNodeUtil.setGeneration(successorCursor, unstableGeneration);
            TreeNodeUtil.setSuccessor(successorCursor, TreeNodeUtil.NO_NODE_FLAG, stableGeneration, unstableGeneration);
        }

        // Insert successor pointer in old stable version
        //   (stableNode)
        //        |
        //     [successor]
        //        |
        //        v
        // (newUnstableNode)
        TreeNodeUtil.setSuccessor(cursor, successorId, stableGeneration, unstableGeneration);

        // Redirect sibling pointers
        //               ---------[leftSibling]---------(stableNode)----------[rightSibling]---------
        //              |                                     |                                      |
        //              |                                  [successor]                                    |
        //              |                                     |                                      |
        //              v                                     v                                      v
        // (leftSiblingOfStableNode) -[rightSibling]-> (newUnstableNode) <-[leftSibling]- (rightSiblingOfStableNode)
        long leftSibling = TreeNodeUtil.leftSibling(cursor, stableGeneration, unstableGeneration);
        checkLeftSiblingPointer(leftSibling, true, cursor, stableGeneration, unstableGeneration);
        long rightSibling = TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration);
        checkRightSiblingPointer(rightSibling, true, cursor, stableGeneration, unstableGeneration);
        if (TreeNodeUtil.isNode(leftSibling)) {
            TreeNodeUtil.goTo(cursor, "left sibling in split", leftSibling);
            TreeNodeUtil.setRightSibling(cursor, successorId, stableGeneration, unstableGeneration);
        }
        if (TreeNodeUtil.isNode(rightSibling)) {
            TreeNodeUtil.goTo(cursor, "right sibling in split", rightSibling);
            TreeNodeUtil.setLeftSibling(cursor, successorId, stableGeneration, unstableGeneration);
        }

        // Leave cursor at new tree node
        TreeNodeUtil.goTo(cursor, "successor", successorId);

        // Propagate structure change
        structureUpdate.update(structurePropagation, successorId);

        structureWriteLog.addToFreelist(unstableGeneration, oldId);
        idProvider.releaseId(stableGeneration, unstableGeneration, oldId, bind(cursor));
    }

    private static <KEY> void checkChildPointer(
            long childPointer,
            PageCursor cursor,
            int childPos,
            InternalNodeBehaviour<KEY> bTreeNode,
            long stableGeneration,
            long unstableGeneration) {
        PointerChecking.checkPointer(
                childPointer,
                false,
                cursor.getCurrentPageId(),
                GBPPointerType.CHILD,
                stableGeneration,
                unstableGeneration,
                cursor,
                bTreeNode.childOffset(childPos));
    }

    private static void checkRightSiblingPointer(
            long siblingPointer,
            boolean allowNoNode,
            PageCursor cursor,
            long stableGeneration,
            long unstableGeneration) {
        PointerChecking.checkPointer(
                siblingPointer,
                allowNoNode,
                cursor.getCurrentPageId(),
                RIGHT_SIBLING,
                stableGeneration,
                unstableGeneration,
                cursor,
                TreeNodeUtil.BYTE_POS_RIGHTSIBLING);
    }

    private static void checkLeftSiblingPointer(
            long siblingPointer,
            boolean allowNoNode,
            PageCursor cursor,
            long stableGeneration,
            long unstableGeneration) {
        PointerChecking.checkPointer(
                siblingPointer,
                allowNoNode,
                cursor.getCurrentPageId(),
                LEFT_SIBLING,
                stableGeneration,
                unstableGeneration,
                cursor,
                TreeNodeUtil.BYTE_POS_LEFTSIBLING);
    }

    private enum InsertResult {
        NO_SPLIT,
        SPLIT,
        SPLIT_SKIP_UNDERFLOW,
        SPLIT_FAIL
    }

    enum RemoveResult {
        NOT_FOUND,
        REMOVED,
        FAIL
    }
}
