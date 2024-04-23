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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.GBPTreeConsistencyChecker.assertNoCrashOrBrokenPointerInGSPP;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.Overflow.NO;
import static org.neo4j.index.internal.gbptree.Overflow.YES;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.DATA_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.ValueMergers.overwrite;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyChecker.ConsistencyCheckState;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@SuppressWarnings("unused")
@ExtendWith(RandomExtension.class)
@ResourceLock(InternalTreeLogicTestBase.INDEX_RESOURCE)
abstract class InternalTreeLogicTestBase<KEY, VALUE> {
    static final String INDEX_RESOURCE = "index";
    private static final int PAGE_SIZE = 256;
    private static long stableGeneration = GenerationSafePointer.MIN_GENERATION;
    private static long unstableGeneration = stableGeneration + 1;

    @Inject
    private RandomSupport random;

    private PageAwareByteArrayCursor cursor;
    private PageAwareByteArrayCursor readCursor;
    private SimpleIdProvider id;

    private ValueMerger<KEY, VALUE> adder;
    private InternalTreeLogic<KEY, VALUE> treeLogic;
    private VALUE dontCare;
    private StructurePropagation<KEY> structurePropagation;

    private double ratioToKeepInLeftOnSplit = InternalTreeLogic.DEFAULT_SPLIT_RATIO;

    protected TestLayout<KEY, VALUE> layout;
    protected LeafNodeBehaviour<KEY, VALUE> leaf;
    protected InternalNodeBehaviour<KEY> internal;

    static Stream<Arguments> generators() {
        return Stream.of(
                arguments("NoCheckpoint", GenerationManager.NO_OP_GENERATION, false),
                arguments("Checkpoint", GenerationManager.DEFAULT, true));
    }

    Root root;
    int numberOfRootSplits;
    private int numberOfRootSuccessors;

    @BeforeEach
    void setUp() throws IOException {
        cursor = new PageAwareByteArrayCursor(PAGE_SIZE);
        readCursor = cursor.duplicate();
        id = new SimpleIdProvider(cursor::duplicate);

        id.reset();
        long newId = id.acquireNewId(stableGeneration, unstableGeneration, CursorCreator.bind(cursor));
        goTo(cursor, newId);
        readCursor.next(newId);

        layout = getLayout();
        OffloadPageCursorFactory pcFactory = (id, flags, cursorContext) -> cursor.duplicate(id);
        OffloadIdValidator idValidator = OffloadIdValidator.ALWAYS_TRUE;
        OffloadStoreImpl<KEY, VALUE> offloadStore =
                new OffloadStoreImpl<>(layout, id, pcFactory, idValidator, PAGE_SIZE);
        leaf = getLeaf(PAGE_SIZE, layout, offloadStore);
        internal = getInternal(PAGE_SIZE, layout, offloadStore);
        adder = getAdder();
        treeLogic = new InternalTreeLogic<>(
                id, leaf, internal, layout, NO_MONITOR, TreeWriterCoordination.NO_COORDINATION, DATA_LAYER_FLAG);
        dontCare = layout.newValue();
        structurePropagation = new StructurePropagation<>(layout.newKey(), layout.newKey(), layout.newKey());
    }

    protected abstract ValueMerger<KEY, VALUE> getAdder();

    protected abstract LeafNodeBehaviour<KEY, VALUE> getLeaf(
            int pageSize, Layout<KEY, VALUE> layout, OffloadStore<KEY, VALUE> offloadStore);

    protected abstract InternalNodeBehaviour<KEY> getInternal(
            int pageSize, Layout<KEY, VALUE> layout, OffloadStore<KEY, VALUE> offloadStore);

    protected abstract TestLayout<KEY, VALUE> getLayout();

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustInsertAtFirstPositionInEmptyLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        KEY key = key(1L);
        VALUE value = value(1L);
        root.goTo(readCursor);
        assertThat(keyCount()).isEqualTo(0);

        // when
        generationManager.checkpoint();
        insert(key, value);

        // then
        root.goTo(readCursor);
        assertThat(keyCount()).isEqualTo(1);
        assertEqualsKey(keyAt(0, false), key);
        assertEqualsValue(valueAt(0), value);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustSortCorrectlyOnInsertFirstInLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        generationManager.checkpoint();

        long someHighSeed = 1000L;
        int keyCount = 0;
        KEY newKey = key(someHighSeed);
        VALUE newValue = value(someHighSeed);
        while (leaf.overflow(cursor, keyCount, newKey, newValue, NULL_CONTEXT) == NO) {
            insert(newKey, newValue);

            // then
            root.goTo(readCursor);
            assertEqualsKey(keyAt(0, false), newKey);
            assertEqualsValue(valueAt(0), newValue);

            keyCount++;
            newKey = key(someHighSeed - keyCount);
            newValue = value(someHighSeed - keyCount);
        }
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustSortCorrectlyOnInsertLastInLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        generationManager.checkpoint();
        int keyCount = 0;
        KEY key = key(keyCount);
        VALUE value = value(keyCount);
        while (leaf.overflow(cursor, keyCount, key, value, NULL_CONTEXT) == NO) {
            // when
            insert(key, value);

            // then
            root.goTo(readCursor);
            assertEqualsKey(keyAt(keyCount, false), key);
            assertEqualsValue(valueAt(keyCount), value);

            keyCount++;
            key = key(keyCount);
            value = value(keyCount);
        }
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustSortCorrectlyOnInsertInMiddleOfLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        generationManager.checkpoint();
        int keyCount = 0;
        int someHighSeed = 1000;
        long middleValue = keyCount % 2 == 0 ? keyCount / 2 : someHighSeed - keyCount / 2;
        KEY key = key(middleValue);
        VALUE value = value(middleValue);
        while (leaf.overflow(cursor, keyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);

            // then
            root.goTo(readCursor);
            assertEqualsKey(keyAt((keyCount + 1) / 2, false), key);

            keyCount++;
            middleValue = keyCount % 2 == 0 ? keyCount / 2 : someHighSeed - keyCount / 2;
            key = key(middleValue);
            value = value(middleValue);
        }
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustSplitWhenInsertingMiddleOfFullLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        int someMiddleSeed = 1000;
        int keyCount = 0;
        int middle = keyCount % 2 == 0 ? keyCount : someMiddleSeed - keyCount;
        KEY key = key(middle);
        VALUE value = value(middle);
        while (leaf.overflow(cursor, keyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);

            keyCount++;
            middle = keyCount % 2 == 0 ? keyCount : someMiddleSeed - keyCount;
            key = key(middle);
            value = value(middle);
        }

        // when
        generationManager.checkpoint();
        insert(key, value);

        // then
        assertEquals(1, numberOfRootSplits);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustSplitWhenInsertingLastInFullLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        int keyCount = 0;
        KEY key = key(keyCount);
        VALUE value = value(keyCount);
        while (leaf.overflow(cursor, keyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);
            assertThat(structurePropagation.hasRightKeyInsert).isFalse();

            keyCount++;
            key = key(keyCount);
            value = value(keyCount);
        }

        // when
        generationManager.checkpoint();
        insert(key, value);

        // then
        assertEquals(1, numberOfRootSplits); // Should cause a split
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustSplitWhenInsertingFirstInFullLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        int keyCount = 0;
        int someHighSeed = 1000;
        KEY key = key(someHighSeed - keyCount);
        VALUE value = value(someHighSeed - keyCount);
        while (leaf.overflow(cursor, keyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);
            assertThat(structurePropagation.hasRightKeyInsert).isFalse();

            keyCount++;
            key = key(someHighSeed - keyCount);
            value = value(someHighSeed - keyCount);
        }

        // when
        generationManager.checkpoint();
        insert(key, value);

        // then
        assertEquals(1, numberOfRootSplits);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustUpdatePointersInSiblingsToSplit(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        long someLargeSeed = 10000;
        int keyCount = 0;
        KEY key = key(someLargeSeed - keyCount);
        VALUE value = value(someLargeSeed - keyCount);
        while (leaf.overflow(cursor, keyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);

            keyCount++;
            key = key(someLargeSeed - keyCount);
            value = value(someLargeSeed - keyCount);
        }

        // First split
        generationManager.checkpoint();
        insert(key, value);
        keyCount++;
        key = key(someLargeSeed - keyCount);
        value = value(keyCount);

        // Assert child pointers and sibling pointers are intact after split in root
        root.goTo(readCursor);
        long child0 = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long child1 = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        assertSiblingOrderAndPointers(child0, child1);

        // Insert until we have another split in leftmost leaf
        while (keyCount(root.id()) == 1) {
            insert(key, value);
            keyCount++;
            key = key(someLargeSeed - keyCount);
            value = value(keyCount);
        }

        // Just to be sure
        assertThat(TreeNodeUtil.isInternal(readCursor)).isTrue();
        assertThat(TreeNodeUtil.keyCount(readCursor)).isEqualTo(2);

        // Assert child pointers and sibling pointers are intact
        // AND that node not involved in split also has its left sibling pointer updated
        child0 = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        child1 = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        long child2 = childAt(
                readCursor, 2, stableGeneration, unstableGeneration); // <- right sibling to split-node before split

        assertSiblingOrderAndPointers(child0, child1, child2);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void splitWithSplitRatio0(String name, GenerationManager generationManager, boolean isCheckpointing)
            throws IOException {
        // given
        ratioToKeepInLeftOnSplit = 0;
        initialize();
        int keyCount = 0;
        KEY key = key(random.nextLong());
        VALUE value = value(random.nextLong());
        while (leaf.overflow(cursor, keyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);
            assertThat(structurePropagation.hasRightKeyInsert).isFalse();

            keyCount++;
            key = key(random.nextLong());
            value = value(random.nextLong());
        }

        // when
        insert(key, value);

        // then
        root.goTo(readCursor);
        long child0 = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long child1 = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        int leftKeyCount = keyCount(child0);
        int rightKeyCount = keyCount(child1);
        assertEquals(1, numberOfRootSplits);

        // Left node should hold as few keys as possible, such that nothing more can be moved to right.
        KEY rightmostKeyInLeftChild = keyAt(child0, leftKeyCount - 1, false);
        VALUE rightmostValueInLeftChild = valueAt(child0, leftKeyCount - 1);
        goTo(readCursor, child1);
        assertThat(leaf.overflow(
                        readCursor, rightKeyCount, rightmostKeyInLeftChild, rightmostValueInLeftChild, NULL_CONTEXT))
                .isEqualTo(YES);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void splitWithSplitRatio1(String name, GenerationManager generationManager, boolean isCheckpointing)
            throws IOException {
        // given
        ratioToKeepInLeftOnSplit = 1;
        initialize();
        int keyCount = 0;
        KEY key = key(random.nextLong());
        VALUE value = value(random.nextLong());
        while (leaf.overflow(cursor, keyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);
            assertThat(structurePropagation.hasRightKeyInsert).isFalse();

            keyCount++;
            key = key(random.nextLong());
            value = value(random.nextLong());
        }

        // when
        insert(key, value);

        // then
        root.goTo(readCursor);
        long child0 = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long child1 = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        int leftKeyCount = keyCount(child0);
        assertEquals(1, numberOfRootSplits);

        // Right node should hold as few keys as possible, such that nothing more can be moved to left.
        KEY leftmostKeyInRightChild = keyAt(child1, 0, false);
        VALUE leftmostValueInRightChild = valueAt(child1, 0);
        goTo(readCursor, child0);
        assertThat(leaf.overflow(
                        readCursor, leftKeyCount, leftmostKeyInRightChild, leftmostValueInRightChild, NULL_CONTEXT))
                .isEqualTo(YES);
    }

    /* REMOVE */
    @ParameterizedTest
    @MethodSource("generators")
    void writerMustRemoveFirstInEmptyLeaf(String name, GenerationManager generationManager, boolean isCheckpointing)
            throws Exception {
        // given
        initialize();
        long keyValue = 1L;
        long valueValue = 1L;
        KEY key = key(keyValue);
        VALUE value = value(valueValue);
        insert(key, value);

        // when
        generationManager.checkpoint();
        VALUE readValue = layout.newValue();
        remove(key, readValue);

        // then
        root.goTo(readCursor);
        assertThat(TreeNodeUtil.keyCount(cursor)).isEqualTo(0);
        assertEqualsValue(value, readValue);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustRemoveFirstInFullLeaf(String name, GenerationManager generationManager, boolean isCheckpointing)
            throws Exception {
        // given
        initialize();
        int maxKeyCount = 0;
        KEY key = key(maxKeyCount);
        VALUE value = value(maxKeyCount);
        while (leaf.overflow(cursor, maxKeyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);

            maxKeyCount++;
            key = key(maxKeyCount);
            value = value(maxKeyCount);
        }

        // when
        generationManager.checkpoint();
        VALUE readValue = layout.newValue();
        remove(key(0), readValue);

        // then
        assertEqualsValue(value(0), readValue);
        root.goTo(readCursor);
        assertThat(TreeNodeUtil.keyCount(readCursor)).isEqualTo(maxKeyCount - 1);
        for (int i = 0; i < maxKeyCount - 1; i++) {
            assertEqualsKey(keyAt(i, false), key(i + 1L));
        }
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustRemoveInMiddleInFullLeaf(String name, GenerationManager generationManager, boolean isCheckpointing)
            throws Exception {
        // given
        initialize();
        int maxKeyCount = 0;
        KEY key = key(maxKeyCount);
        VALUE value = value(maxKeyCount);
        while (leaf.overflow(cursor, maxKeyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);

            maxKeyCount++;
            key = key(maxKeyCount);
            value = value(maxKeyCount);
        }
        int middle = maxKeyCount / 2;

        // when
        generationManager.checkpoint();
        VALUE readValue = layout.newValue();
        remove(key(middle), readValue);

        // then
        assertEqualsValue(value(middle), readValue);
        root.goTo(readCursor);
        assertThat(keyCount()).isEqualTo(maxKeyCount - 1);
        assertEqualsKey(keyAt(middle, false), key(middle + 1L));
        for (int i = 0; i < maxKeyCount - 1; i++) {
            long expected = i < middle ? i : i + 1L;
            assertEqualsKey(keyAt(i, false), key(expected));
        }
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustRemoveLastInFullLeaf(String name, GenerationManager generationManager, boolean isCheckpointing)
            throws Exception {
        initialize();
        int maxKeyCount = 0;
        KEY key = key(maxKeyCount);
        VALUE value = value(maxKeyCount);
        while (leaf.overflow(cursor, maxKeyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);

            maxKeyCount++;
            key = key(maxKeyCount);
            value = value(maxKeyCount);
        }

        // when
        generationManager.checkpoint();
        VALUE readValue = layout.newValue();
        remove(key(maxKeyCount - 1), readValue);

        // then
        assertEqualsValue(value(maxKeyCount - 1), readValue);
        root.goTo(readCursor);
        assertThat(keyCount()).isEqualTo(maxKeyCount - 1);
        for (int i = 0; i < maxKeyCount - 1; i++) {
            assertEqualsKey(keyAt(i, false), key(i));
        }
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustRemoveFromLeftChild(String name, GenerationManager generationManager, boolean isCheckpointing)
            throws Exception {
        initialize();
        for (int i = 0; numberOfRootSplits == 0; i++) {
            insert(key(i), value(i));
        }

        // when
        generationManager.checkpoint();
        goTo(readCursor, structurePropagation.midChild);
        assertEqualsKey(keyAt(0, false), key(0L));
        VALUE readValue = layout.newValue();
        remove(key(0), readValue);

        // then
        assertEqualsValue(value(0), readValue);
        goTo(readCursor, structurePropagation.midChild);
        assertEqualsKey(keyAt(0, false), key(1L));
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustRemoveFromRightChildButNotFromInternalWithHitOnInternalSearch(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        initialize();
        int i;
        for (i = 0; numberOfRootSplits == 0; i++) {
            insert(key(i), value(i));
        }
        insert(key(i), value(i)); // And one more to avoid rebalance

        // when key to remove exists in internal
        KEY internalKey = structurePropagation.rightKey;
        root.goTo(readCursor);
        assertEqualsKey(keyAt(0, true), internalKey);

        // and as first key in right child
        long rightChild = structurePropagation.rightChild;
        goTo(readCursor, rightChild);
        int keyCountInRightChild = keyCount();
        KEY keyToRemove = keyAt(0, false);
        assertEquals(getSeed(keyToRemove), getSeed(internalKey), "expected same seed");

        // and we remove it
        generationManager.checkpoint();
        remove(keyToRemove, dontCare);

        // then we should still find it in internal
        root.goTo(readCursor);
        assertThat(keyCount()).isEqualTo(1);
        assertEquals(getSeed(keyAt(0, true)), getSeed(keyToRemove), "expected same seed");

        // but not in right leaf
        rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        goTo(readCursor, rightChild);
        assertThat(keyCount()).isEqualTo(keyCountInRightChild - 1);
        assertEqualsKey(keyAt(0, false), key(getSeed(keyToRemove) + 1));
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustNotRemoveWhenKeyDoesNotExist(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        int maxKeyCount = 0;
        KEY key = key(maxKeyCount);
        VALUE value = value(maxKeyCount);
        while (leaf.overflow(cursor, maxKeyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);

            maxKeyCount++;
            key = key(maxKeyCount);
            value = value(maxKeyCount);
        }

        // when
        generationManager.checkpoint();
        remove(key(maxKeyCount), dontCare);

        // then
        root.goTo(readCursor);
        assertThat(keyCount()).isEqualTo(maxKeyCount);
        for (int i = 0; i < maxKeyCount; i++) {
            assertEqualsKey(keyAt(i, false), key(i));
        }
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustNotRemoveWhenKeyOnlyExistInInternal(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        int i;
        for (i = 0; numberOfRootSplits == 0; i++) {
            insert(key(i), value(i));
        }
        insert(key(i), value(i)); // And an extra to not cause rebalance

        // when key to remove exists in internal
        long currentRightChild = structurePropagation.rightChild;
        KEY keyToRemove = keyAt(currentRightChild, 0, false);
        assertThat(getSeed(keyToRemove)).isEqualTo(getSeed(keyAt(root.id(), 0, true)));

        // and as first key in right child
        goTo(readCursor, currentRightChild);
        int keyCountInRightChild = keyCount();
        assertEquals(getSeed(keyToRemove), getSeed(keyAt(0, false)), "same seed");

        // and we remove it
        generationManager.checkpoint();
        remove(keyToRemove, dontCare); // Possibly create successor of right child
        root.goTo(readCursor);
        currentRightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);

        // then we should still find it in internal
        assertThat(keyCount()).isEqualTo(1);
        assertEquals(getSeed(keyAt(0, true)), getSeed(keyToRemove), "same seed");

        // but not in right leaf
        goTo(readCursor, currentRightChild);
        assertThat(keyCount()).isEqualTo(keyCountInRightChild - 1);
        assertEquals(getSeed(keyAt(0, false)), getSeed(key(getSeed(keyToRemove) + 1)), "same seed");

        // and when we remove same key again, nothing should change
        int keyCount = keyCount();
        remove(keyToRemove, dontCare);
        assertEquals(keyCount, keyCount());
    }

    /* REBALANCE */

    @ParameterizedTest
    @MethodSource("generators")
    void mustNotRebalanceFromRightToLeft(String name, GenerationManager generationManager, boolean isCheckpointing)
            throws Exception {
        // given
        initialize();
        long key = 0;
        while (numberOfRootSplits == 0) {
            insert(key(key), value(key));
            key++;
        }

        // ... enough keys in right child to share with left child if rebalance is needed
        insert(key(key), value(key));

        // ... and the prim key diving key range for left child and right child
        root.goTo(readCursor);
        KEY primKey = keyAt(0, true);

        // ... and knowing key count of right child
        long rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        goTo(readCursor, rightChild);
        int expectedKeyCount = TreeNodeUtil.keyCount(readCursor);

        // when
        // ... removing all keys from left child
        for (long i = 0; ; i++) {
            KEY removeKey = key(i);
            if (layout.compare(removeKey, primKey) >= 0) {
                break;
            }
            remove(removeKey, dontCare);
        }

        // then
        // ... looking a right child
        goTo(readCursor, rightChild);

        // ... no keys should have moved from right sibling
        int actualKeyCount = TreeNodeUtil.keyCount(readCursor);
        assertEquals(
                expectedKeyCount,
                actualKeyCount,
                "actualKeyCount=" + actualKeyCount + ", expectedKeyCount=" + expectedKeyCount);
        assertEquals(getSeed(primKey), getSeed(keyAt(0, false)), "same seed");
    }

    @ParameterizedTest
    @MethodSource("generators")
    void mustPropagateAllStructureChanges(String name, GenerationManager generationManager, boolean isCheckpointing)
            throws Exception {
        assumeTrue(isCheckpointing, "No checkpointing, no successor");

        // given
        initialize();
        long key = 10;
        while (numberOfRootSplits == 0) {
            insert(key(key), value(key));
            key++;
        }
        // ... enough keys in left child to share with right child if rebalance is needed
        for (long smallKey = 0; smallKey < 2; smallKey++) {
            insert(key(smallKey), value(smallKey));
        }

        // ... and the prim key dividing key range for left and right child
        root.goTo(readCursor);
        KEY oldPrimKey = keyAt(0, true);

        // ... and left and right child
        long originalLeftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long originalRightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        goTo(readCursor, originalRightChild);
        List<KEY> keysInRightChild = allLeafKeys(readCursor);

        // when
        // ... after checkpoint
        generationManager.checkpoint();

        // ... removing keys from right child until rebalance is triggered
        int index = 0;
        long rightChild;
        KEY originalLeftmost = keysInRightChild.get(0);
        KEY leftmostInRightChild;
        do {
            remove(keysInRightChild.get(index), dontCare);
            index++;
            root.goTo(readCursor);
            rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
            goTo(readCursor, rightChild);
            leftmostInRightChild = keyAt(0, false);
        } while (layout.compare(leftmostInRightChild, originalLeftmost) >= 0);

        // then
        // ... primKey in root is updated
        root.goTo(readCursor);
        KEY primKey = keyAt(0, true);
        assertEqualsKey(primKey, leftmostInRightChild);
        assertNotEqualsKey(primKey, oldPrimKey);

        // ... new versions of left and right child
        long newLeftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long newRightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        assertThat(newLeftChild).isNotEqualTo(originalLeftChild);
        assertThat(newRightChild).isNotEqualTo(originalRightChild);
    }

    /* MERGE */

    @ParameterizedTest
    @MethodSource("generators")
    void mustPropagateStructureOnMergeFromLeft(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        assumeTrue(isCheckpointing, "No checkpointing, no successor");

        // GIVEN:
        //       ------root-------
        //      /        |         \
        //     v         v          v
        //   left <--> middle <--> right
        List<KEY> allKeys = new ArrayList<>();
        initialize();
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for (; id.lastId() < targetLastId; i++) {
            KEY key = key(i);
            insert(key, value(i));
            allKeys.add(key);
        }
        root.goTo(readCursor);
        assertEquals(2, keyCount());
        long oldRootId = readCursor.getCurrentPageId();
        long oldLeftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long oldMiddleChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        long oldRightChild = childAt(readCursor, 2, stableGeneration, unstableGeneration);
        assertSiblings(oldLeftChild, oldMiddleChild, oldRightChild);

        // WHEN
        generationManager.checkpoint();
        KEY middleKey = keyAt(oldMiddleChild, 0, false); // Should be located in middle leaf
        remove(middleKey, dontCare);
        allKeys.remove(middleKey);

        // THEN
        // old root should still have 2 keys
        goTo(readCursor, oldRootId);
        assertEquals(2, keyCount());

        // new root should have only 1 key
        root.goTo(readCursor);
        assertEquals(1, keyCount());

        // left child should be a new node
        long newLeftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        assertThat(oldLeftChild).isNotEqualTo(newLeftChild);
        assertThat(oldMiddleChild).isNotEqualTo(newLeftChild);

        // right child should be same old node
        long newRightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        assertThat(oldRightChild).isEqualTo(newRightChild);

        // old left and old middle has new left as successor
        goTo(readCursor, oldLeftChild);
        assertThat(successor(readCursor, stableGeneration, unstableGeneration)).isEqualTo(newLeftChild);
        goTo(readCursor, oldMiddleChild);
        assertThat(successor(readCursor, stableGeneration, unstableGeneration)).isEqualTo(newLeftChild);

        // new left child contain keys from old left and old middle
        goTo(readCursor, oldRightChild);
        KEY firstKeyOfOldRightChild = keyAt(0, false);
        int index = indexOf(firstKeyOfOldRightChild, allKeys, layout);
        List<KEY> expectedKeysInNewLeftChild = allKeys.subList(0, index);
        goTo(readCursor, newLeftChild);
        assertLeafNodeContainsExpectedKeys(expectedKeysInNewLeftChild);

        // new children are siblings
        assertSiblings(newLeftChild, oldRightChild, TreeNodeUtil.NO_NODE_FLAG);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void mustPropagateStructureOnMergeToRight(String name, GenerationManager generationManager, boolean isCheckpointing)
            throws Exception {
        assumeTrue(isCheckpointing, "No checkpointing, no successor");

        // GIVEN:
        //        ---------root---------
        //       /           |          \
        //      v            v           v
        //   oldleft <-> oldmiddle <-> oldright
        List<KEY> allKeys = new ArrayList<>();
        initialize();
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for (; id.lastId() < targetLastId; i++) {
            KEY key = key(i);
            insert(key, value(i));
            allKeys.add(key);
        }
        root.goTo(readCursor);
        assertEquals(2, keyCount());
        long oldLeftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long oldMiddleChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        long oldRightChild = childAt(readCursor, 2, stableGeneration, unstableGeneration);
        assertSiblings(oldLeftChild, oldMiddleChild, oldRightChild);
        goTo(readCursor, oldLeftChild);
        KEY keyInLeftChild = keyAt(0, false);

        // WHEN
        generationManager.checkpoint();
        // removing key in left child
        root.goTo(readCursor);
        remove(keyInLeftChild, dontCare);
        allKeys.remove(keyInLeftChild);
        // New structure
        // NOTE: oldleft gets a successor (intermediate) before removing key and then another one once it is merged,
        //       effectively creating a chain of successor pointers to our newleft that in the end contain keys from
        //       oldleft and oldmiddle
        //                                                         ----root----
        //                                                        /            |
        //                                                       v             v
        // oldleft -[successor]-> intermediate -[successor]-> newleft <-> oldright
        //                                                      ^
        //                                                       \-[successor]- oldmiddle

        // THEN
        // old root should still have 2 keys
        assertEquals(2, keyCount());

        // new root should have only 1 key
        root.goTo(readCursor);
        assertEquals(1, keyCount());

        // left child should be a new node
        long newLeftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        assertThat(oldLeftChild).isNotEqualTo(newLeftChild);
        assertThat(oldMiddleChild).isNotEqualTo(newLeftChild);

        // right child should be same old node
        long newRightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        assertThat(oldRightChild).isEqualTo(newRightChild);

        // old left and old middle has new left as successor
        goTo(readCursor, oldLeftChild);
        assertThat(newestGeneration(readCursor, stableGeneration, unstableGeneration))
                .isEqualTo(newLeftChild);
        goTo(readCursor, oldMiddleChild);
        assertThat(successor(readCursor, stableGeneration, unstableGeneration)).isEqualTo(newLeftChild);

        // new left child contain keys from old left and old middle
        goTo(readCursor, oldRightChild);
        KEY firstKeyInOldRightChild = keyAt(0, false);
        int index = indexOf(firstKeyInOldRightChild, allKeys, layout);
        List<KEY> expectedKeysInNewLeftChild = allKeys.subList(0, index);
        goTo(readCursor, newLeftChild);
        assertLeafNodeContainsExpectedKeys(expectedKeysInNewLeftChild);

        // new children are siblings
        assertSiblings(newLeftChild, oldRightChild, TreeNodeUtil.NO_NODE_FLAG);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void mustPropagateStructureWhenMergingBetweenDifferentSubtrees(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // GIVEN
        // We will merge oldLeft into oldRight
        //                               -----root----
        //                             /               \
        //                            v                 v
        //                 _____leftParent    <->      rightParent_____
        //                / / /         \              /           \ \ \
        //               v v v           v            v             v v v
        // [some more children]       oldLeft <-> oldRight         [some more children]
        initialize();
        long i = 0;
        while (numberOfRootSplits < 2) {
            insert(key(i), value(i));
            i++;
        }

        root.goTo(readCursor);
        long oldLeft = rightmostLeafInSubtree(root.id(), 0);
        long oldRight = leftmostLeafInSubtree(root.id(), 1);
        KEY oldSplitter = keyAt(0, true);
        KEY rightmostKeyInLeftSubtree = rightmostInternalKeyInSubtree(root.id(), 0);

        List<KEY> allKeysInOldLeftAndOldRight = new ArrayList<>();
        goTo(readCursor, oldLeft);
        allLeafKeys(readCursor, allKeysInOldLeftAndOldRight);
        goTo(readCursor, oldRight);
        allLeafKeys(readCursor, allKeysInOldLeftAndOldRight);

        KEY keyInOldRight = keyAt(0, false);

        // WHEN
        generationManager.checkpoint();
        remove(keyInOldRight, dontCare);
        remove(keyInOldRight, allKeysInOldLeftAndOldRight, layout);

        // THEN
        // oldSplitter in root should have been replaced by rightmostKeyInLeftSubtree
        root.goTo(readCursor);
        KEY newSplitter = keyAt(0, true);
        assertNotEqualsKey(newSplitter, oldSplitter);
        assertEqualsKey(newSplitter, rightmostKeyInLeftSubtree);

        // rightmostKeyInLeftSubtree should have been removed from successor version of leftParent
        KEY newRightmostInternalKeyInLeftSubtree = rightmostInternalKeyInSubtree(root.id(), 0);
        assertNotEqualsKey(newRightmostInternalKeyInLeftSubtree, rightmostKeyInLeftSubtree);

        // newRight contain all
        goToSuccessor(readCursor, oldRight);
        List<KEY> allKeysInNewRight = allLeafKeys(readCursor);
        assertThat(allKeysInNewRight.size()).isEqualTo(allKeysInOldLeftAndOldRight.size());
        for (int index = 0; index < allKeysInOldLeftAndOldRight.size(); index++) {
            assertEqualsKey(allKeysInOldLeftAndOldRight.get(index), allKeysInNewRight.get(index));
        }
    }

    @ParameterizedTest
    @MethodSource("generators")
    void mustLeaveSingleLeafAsRootWhenEverythingIsRemoved(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // GIVEN
        // a tree with some keys
        List<KEY> allKeys = new ArrayList<>();
        initialize();
        long i = 0;
        while (numberOfRootSplits < 3) {
            KEY key = key(i);
            insert(key, value(i));
            allKeys.add(key);
            i++;
        }

        // WHEN
        // removing all keys but one
        generationManager.checkpoint();
        for (int j = 0; j < allKeys.size() - 1; j++) {
            remove(allKeys.get(j), dontCare);
        }

        // THEN
        root.goTo(readCursor);
        assertThat(TreeNodeUtil.isLeaf(readCursor)).isTrue();
    }

    /* OVERALL CONSISTENCY */

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustProduceConsistentTreeWithRandomInserts(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        int numberOfEntries = 100_000;
        for (int i = 0; i < numberOfEntries; i++) {
            // when
            long keySeed = random.nextLong();
            insert(key(keySeed), value(random.nextLong()));
            if (i == numberOfEntries / 2) {
                generationManager.checkpoint();
            }
        }

        // then
        root.goTo(readCursor);
        consistencyCheck();
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustProduceConsistentTreeWithRandomInsertsWithConflictingKeys(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        int numberOfEntries = 100_000;
        for (int i = 0; i < numberOfEntries; i++) {
            // when
            long keySeed = random.nextLong(1000);
            insert(key(keySeed), value(random.nextLong()));
            if (i == numberOfEntries / 2) {
                generationManager.checkpoint();
            }
        }

        // then
        consistencyCheck();
    }

    /* TEST VALUE MERGER */

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustOverwriteWithOverwriteMerger(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        KEY key = key(random.nextLong());
        VALUE firstValue = value(random.nextLong());
        insert(key, firstValue);

        // when
        generationManager.checkpoint();
        VALUE secondValue = value(random.nextLong());
        insert(key, secondValue, ValueMergers.overwrite());

        // then
        root.goTo(readCursor);
        assertThat(keyCount()).isEqualTo(1);
        assertEqualsValue(valueAt(0), secondValue);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void writerMustKeepExistingWithKeepExistingMerger(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // given
        initialize();
        KEY key = key(random.nextLong());
        VALUE firstValue = value(random.nextLong());
        insert(key, firstValue, ValueMergers.keepExisting());
        root.goTo(readCursor);
        assertThat(keyCount()).isEqualTo(1);
        VALUE actual = valueAt(0);
        assertEqualsValue(actual, firstValue);

        // when
        generationManager.checkpoint();
        VALUE secondValue = value(random.nextLong());
        insert(key, secondValue, ValueMergers.keepExisting());

        // then
        root.goTo(readCursor);
        assertThat(keyCount()).isEqualTo(1);
        actual = valueAt(0);
        assertEqualsValue(actual, firstValue);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void shouldMergeValue(String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // GIVEN
        initialize();
        KEY key = key(10);
        long baseValue = 100;
        insert(key, value(baseValue));

        // WHEN
        generationManager.checkpoint();
        long toAdd = 5;
        insert(key, value(toAdd), adder);

        // THEN
        root.goTo(readCursor);
        KEY readKey = layout.newKey();
        int keyCount = keyCount();
        int searchResult = KeySearch.search(readCursor, leaf, key, readKey, keyCount, NULL_CONTEXT);
        assertThat(KeySearch.isHit(searchResult)).isTrue();
        int pos = KeySearch.positionOf(searchResult);
        assertEquals(0, pos);
        assertEqualsKey(key, keyAt(pos, false));
        assertEqualsValue(value(baseValue + toAdd), valueAt(pos));
    }

    @Test
    void shouldRemoveEntryThatMergerWantsToRemove() throws IOException {
        // given
        initialize();
        long baseKey = random.nextLong();
        KEY key1 = key(baseKey + 1);
        KEY key2 = key(baseKey + 2);
        KEY key3 = key(baseKey + 3);
        long baseValue = random.nextLong();
        VALUE value1 = value(baseValue + 1);
        VALUE value2 = value(baseValue + 2);
        VALUE value3 = value(baseValue + 3);
        insert(key1, value1);
        insert(key2, value2);
        insert(key3, value3);

        // when
        ValueMerger<KEY, VALUE> remover =
                (existingKey, newKey, existingValue, newValue) -> ValueMerger.MergeResult.REMOVED;
        insert(key2, value(baseValue + 4), remover);

        // then
        goTo(readCursor, root.id());
        // key1 should exist
        KEY readKey2 = layout.newKey();
        int keyCount2 = keyCount();
        int searchResult = KeySearch.search(readCursor, leaf, key1, readKey2, keyCount2, NULL_CONTEXT);
        assertThat(KeySearch.isHit(searchResult)).isTrue();
        // key2 should not exist
        KEY readKey1 = layout.newKey();
        int keyCount1 = keyCount();
        searchResult = KeySearch.search(readCursor, leaf, key2, readKey1, keyCount1, NULL_CONTEXT);
        assertThat(KeySearch.isHit(searchResult)).isFalse();
        // key3 should exist
        KEY readKey = layout.newKey();
        int keyCount = keyCount();
        searchResult = KeySearch.search(readCursor, leaf, key3, readKey, keyCount, NULL_CONTEXT);
        assertThat(KeySearch.isHit(searchResult)).isTrue();
    }

    @Test
    void shouldHandleUnderflowOnMergeRemove() throws IOException {
        // given
        initialize();
        KEY firstKey = key(0);
        insert(firstKey, value(0));
        long firstRootId = root.id();
        int highestInsertedKey = 0;
        Set<KEY> expectedKeys = new TreeSet<>((k1, k2) -> layout.compare(k1, k2));
        expectedKeys.add(firstKey);
        // insert until there's a root split
        while (root.id() == firstRootId) {
            highestInsertedKey++;
            KEY key = key(highestInsertedKey);
            insert(key, value(highestInsertedKey));
            expectedKeys.add(key);
        }
        // and continue inserting until there are two keys in root
        while (keyCount(root.id()) < 2) {
            highestInsertedKey++;
            KEY key = key(highestInsertedKey);
            insert(key, value(highestInsertedKey));
            expectedKeys.add(key);
        }

        // when
        int lowestInsertedKey = 0;
        ValueMerger<KEY, VALUE> remover =
                (existingKey, newKey, existingValue, newValue) -> ValueMerger.MergeResult.REMOVED;
        while (keyCount(root.id()) > 1) {
            KEY key = key(lowestInsertedKey);
            insert(key, value(lowestInsertedKey), remover);
            assertThat(expectedKeys.remove(key)).isTrue();
            lowestInsertedKey++;
        }

        // then yes, merge w/ ValueMerger that returns REMOVED can handle underflow and merge leaves
        // verify the actual existing keys too
        goTo(readCursor, root.id());
        int rootKeyCount = keyCount();
        long[] children = new long[rootKeyCount + 1];
        for (int i = 0; i < children.length; i++) {
            children[i] = internal.childAt(readCursor, i, stableGeneration, unstableGeneration);
        }
        for (long childId : children) {
            goTo(readCursor, childId);
            int keyCount = keyCount();
            for (int i = 0; i < keyCount; i++) {
                KEY key = layout.newKey();
                leaf.keyAt(readCursor, key, i, NULL_CONTEXT);
                assertThat(expectedKeys.remove(key)).isTrue();
            }
        }
        assertThat(expectedKeys.isEmpty()).isTrue();
    }

    /* CREATE NEW VERSION ON UPDATE */

    @ParameterizedTest
    @MethodSource("generators")
    void shouldCreateNewVersionWhenInsertInStableRootAsLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        assumeTrue(isCheckpointing, "No checkpointing, no successor");

        // GIVEN root
        initialize();
        long oldGenerationId = cursor.getCurrentPageId();

        // WHEN root -[successor]-> successor of root
        generationManager.checkpoint();
        insert(key(1L), value(1L));
        long successor = cursor.getCurrentPageId();

        // THEN
        root.goTo(readCursor);
        assertEquals(1, numberOfRootSuccessors);
        assertThat(structurePropagation.midChild).isEqualTo(successor);
        assertThat(successor).isNotEqualTo(oldGenerationId);
        assertEquals(1, keyCount());

        goTo(readCursor, oldGenerationId);
        assertThat(successor(readCursor, stableGeneration, unstableGeneration)).isEqualTo(successor);
        assertEquals(0, keyCount());
    }

    @ParameterizedTest
    @MethodSource("generators")
    void shouldCreateNewVersionWhenRemoveInStableRootAsLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        assumeTrue(isCheckpointing, "No checkpointing, no successor");

        // GIVEN root
        initialize();
        KEY key = key(1L);
        VALUE value = value(10L);
        insert(key, value);
        long oldGenerationId = cursor.getCurrentPageId();

        // WHEN root -[successor]-> successor of root
        generationManager.checkpoint();
        remove(key, dontCare);
        long successor = cursor.getCurrentPageId();

        // THEN
        root.goTo(readCursor);
        assertEquals(1, numberOfRootSuccessors);
        assertThat(structurePropagation.midChild).isEqualTo(successor);
        assertThat(successor).isNotEqualTo(oldGenerationId);
        assertEquals(0, keyCount());

        goTo(readCursor, oldGenerationId);
        assertThat(successor(readCursor, stableGeneration, unstableGeneration)).isEqualTo(successor);
        assertEquals(1, keyCount());
    }

    @ParameterizedTest
    @MethodSource("generators")
    void shouldCreateNewVersionWhenInsertInStableLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        assumeTrue(isCheckpointing, "No checkpointing, no successor");

        // GIVEN:
        //       ------root-------
        //      /        |         \
        //     v         v          v
        //   left <--> middle <--> right
        initialize();
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for (; id.lastId() < targetLastId; i++) {
            insert(key(i), value(i));
        }
        root.goTo(readCursor);
        assertEquals(2, keyCount());
        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long middleChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        long rightChild = childAt(readCursor, 2, stableGeneration, unstableGeneration);
        assertSiblings(leftChild, middleChild, rightChild);

        // WHEN
        generationManager.checkpoint();
        long middle = i / 2;
        KEY middleKey = key(middle); // Should be located in middle leaf
        VALUE oldValue = value(middle);
        VALUE newValue = value(middle * 11);
        insert(middleKey, newValue);

        // THEN
        // root have new middle child
        long expectedNewMiddleChild = targetLastId + 1;
        assertThat(id.lastId()).isEqualTo(expectedNewMiddleChild);
        long newMiddleChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        assertThat(newMiddleChild).isEqualTo(expectedNewMiddleChild);

        // old middle child has successor
        goTo(readCursor, middleChild);
        assertThat(successor(readCursor, stableGeneration, unstableGeneration)).isEqualTo(newMiddleChild);

        // old middle child has seen no change
        assertKeyAssociatedWithValue(middleKey, oldValue);

        // new middle child has seen change
        goTo(readCursor, newMiddleChild);
        assertKeyAssociatedWithValue(middleKey, newValue);

        // sibling pointers updated
        assertSiblings(leftChild, newMiddleChild, rightChild);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void shouldCreateNewVersionWhenRemoveInStableLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        assumeTrue(isCheckpointing, "No checkpointing, no successor");

        // GIVEN:
        //       ------root-------
        //      /        |         \
        //     v         v          v
        //   left <--> middle <--> right
        initialize();
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for (; id.lastId() < targetLastId; i += 2) {
            insert(key(i), value(i));
        }
        root.goTo(readCursor);
        assertEquals(2, keyCount());

        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long middleChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        long rightChild = childAt(readCursor, 2, stableGeneration, unstableGeneration);

        // add some more keys to middleChild to not have remove trigger a merge
        goTo(readCursor, middleChild);
        KEY firstKeyInMiddleChild = keyAt(0, false);
        VALUE firstValueInMiddleChild = valueAt(0);
        long seed = getSeed(firstKeyInMiddleChild);
        insert(key(seed + 1), value(seed + 1));
        insert(key(seed + 3), value(seed + 3));
        root.goTo(readCursor);

        assertSiblings(leftChild, middleChild, rightChild);

        // WHEN
        generationManager.checkpoint();
        VALUE removedValue = layout.newValue();
        remove(firstKeyInMiddleChild, removedValue);
        assertNotEquals(0, layout.compareValue(dontCare, removedValue));

        // THEN
        // root have new middle child
        long expectedNewMiddleChild = targetLastId + 1;
        assertThat(id.lastId()).isEqualTo(expectedNewMiddleChild);
        long newMiddleChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        assertThat(newMiddleChild).isEqualTo(expectedNewMiddleChild);

        // old middle child has successor
        goTo(readCursor, middleChild);
        assertThat(successor(readCursor, stableGeneration, unstableGeneration)).isEqualTo(newMiddleChild);

        // old middle child has seen no change
        assertKeyAssociatedWithValue(firstKeyInMiddleChild, firstValueInMiddleChild);

        // new middle child has seen change
        goTo(readCursor, newMiddleChild);
        assertKeyNotFound(firstKeyInMiddleChild);

        // sibling pointers updated
        assertSiblings(leftChild, newMiddleChild, rightChild);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void shouldCreateNewVersionWhenInsertInStableRootAsInternal(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        assumeTrue(isCheckpointing, "No checkpointing, no successor");

        // GIVEN:
        //                       root
        //                   ----   ----
        //                  /           \
        //                 v             v
        //               left <-------> right
        initialize();

        // Fill root
        int keyCount = 0;
        KEY key = key(keyCount);
        VALUE value = value(keyCount);
        while (leaf.overflow(cursor, keyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);
            keyCount++;
            key = key(keyCount);
            value = value(keyCount);
        }

        // Split
        insert(key, value);
        keyCount++;
        key = key(keyCount);
        value = value(keyCount);

        // Fill right child
        root.goTo(readCursor);
        long rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        goTo(readCursor, rightChild);
        int rightChildKeyCount = TreeNodeUtil.keyCount(readCursor);
        while (leaf.overflow(readCursor, rightChildKeyCount, key, value, NULL_CONTEXT) == NO) {
            insert(key, value);
            keyCount++;
            rightChildKeyCount++;
            key = key(keyCount);
            value = value(keyCount);
        }

        long oldRootId = root.id();
        root.goTo(readCursor);
        assertEquals(1, keyCount());
        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        assertSiblings(leftChild, rightChild, TreeNodeUtil.NO_NODE_FLAG);

        // WHEN
        //                       root(successor)
        //                   ----  | ---------------
        //                  /      |                \
        //                 v       v                 v
        //               left <-> right(successor) <--> farRight
        generationManager.checkpoint();
        insert(key, value);
        assertEquals(1, numberOfRootSuccessors);
        root.goTo(readCursor);
        leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);

        // THEN
        // siblings are correct
        long farRightChild = childAt(readCursor, 2, stableGeneration, unstableGeneration);
        assertSiblings(leftChild, rightChild, farRightChild);

        // old root points to successor of root
        goTo(readCursor, oldRootId);
        assertThat(successor(readCursor, stableGeneration, unstableGeneration)).isEqualTo(root.id());
    }

    @ParameterizedTest
    @MethodSource("generators")
    void shouldCreateNewVersionWhenInsertInStableInternal(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        assumeTrue(isCheckpointing, "No checkpointing, no successor");

        // GIVEN
        initialize();
        long someHighMultiplier = 1000;
        for (int i = 0; numberOfRootSplits < 2; i++) {
            long seed = i * someHighMultiplier;
            insert(key(seed), value(seed));
        }
        long rootAfterInitialData = root.id();
        root.goTo(readCursor);
        assertEquals(1, keyCount());
        long leftInternal = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long rightInternal = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        assertSiblings(leftInternal, rightInternal, TreeNodeUtil.NO_NODE_FLAG);
        goTo(readCursor, leftInternal);
        int leftInternalKeyCount = keyCount();
        assertThat(TreeNodeUtil.isInternal(readCursor)).isTrue();
        long leftLeaf = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        goTo(readCursor, leftLeaf);
        KEY firstKeyInLeaf = keyAt(0, false);
        long seedOfFirstKeyInLeaf = getSeed(firstKeyInLeaf);

        // WHEN
        generationManager.checkpoint();
        long targetLastId =
                id.lastId() + 3; /*one for successor in leaf, one for split leaf, one for successor in internal*/
        for (int i = 0; id.lastId() < targetLastId; i++) {
            insert(key(seedOfFirstKeyInLeaf + i), value(seedOfFirstKeyInLeaf + i));
            assertThat(structurePropagation.hasRightKeyInsert).isFalse(); // there should be no root split
        }

        // THEN
        // root hasn't been split further
        assertThat(root.id()).isEqualTo(rootAfterInitialData);

        // there's an successor to left internal w/ one more key in
        root.goTo(readCursor);
        long successorLeftInternal = id.lastId();
        assertThat(childAt(readCursor, 0, stableGeneration, unstableGeneration)).isEqualTo(successorLeftInternal);
        goTo(readCursor, successorLeftInternal);
        int successorLeftInternalKeyCount = keyCount();
        assertEquals(leftInternalKeyCount + 1, successorLeftInternalKeyCount);

        // and left internal points to the successor
        goTo(readCursor, leftInternal);
        assertThat(successor(readCursor, stableGeneration, unstableGeneration)).isEqualTo(successorLeftInternal);
        assertSiblings(successorLeftInternal, rightInternal, TreeNodeUtil.NO_NODE_FLAG);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void shouldOverwriteInheritedSuccessorOnSuccessor(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // GIVEN
        assumeTrue(isCheckpointing, "No checkpointing, no successor");
        initialize();
        long originalNodeId = root.id();
        generationManager.checkpoint();
        insert(key(1L), value(10L)); // TX1 will create successor
        assertEquals(1, numberOfRootSuccessors);

        // WHEN
        // recovery happens
        generationManager.recovery();
        // start up on stable root
        goTo(cursor, originalNodeId);
        treeLogic.initialize(cursor, InternalTreeLogic.DEFAULT_SPLIT_RATIO, StructureWriteLog.EMPTY);
        // replay transaction TX1 will create a new successor
        insert(key(1L), value(10L));
        assertEquals(2, numberOfRootSuccessors);

        // THEN
        root.goTo(readCursor);
        // successor pointer for successor should not have broken or crashed GSPP slot
        assertSuccessorPointerNotCrashOrBroken();
        // and previously crashed successor GSPP slot should have been overwritten
        goTo(readCursor, originalNodeId);
        assertSuccessorPointerNotCrashOrBroken();
    }

    @ParameterizedTest
    @MethodSource("generators")
    void mustThrowIfReachingNodeWithValidSuccessor(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // GIVEN
        // root with two children
        assumeTrue(isCheckpointing, "No checkpointing, no successor");
        initialize();
        long someHighMultiplier = 1000;
        for (int i = 1; numberOfRootSplits < 1; i++) {
            long seed = i * someHighMultiplier;
            insert(key(seed), value(seed));
        }
        generationManager.checkpoint();

        // and leftmost child has successor that is not pointed to by parent (root)
        root.goTo(readCursor);
        long leftmostChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        giveSuccessor(readCursor, leftmostChild);

        // WHEN
        // insert in leftmostChild
        var e = assertThrows(TreeInconsistencyException.class, () -> insert(key(0), value(0)));
        // THEN
        assertThat(e.getMessage()).contains(PointerChecking.WRITER_TRAVERSE_OLD_STATE_MESSAGE);
    }

    @ParameterizedTest
    @MethodSource("generators")
    void mustThrowIfLeafToUpdateIsNotTreeNode(String name, GenerationManager generationManager, boolean isCheckpointing)
            throws Exception {
        // GIVEN
        // root with two children
        initialize();
        long someHighMultiplier = 1000;
        for (int i = 1; numberOfRootSplits < 1; i++) {
            long seed = i * someHighMultiplier;
            insert(key(seed), value(seed));
        }
        generationManager.checkpoint();

        // Set type of the left child to something other than tree node
        long leftmostChild = setTypeInvalidOnLeftChildOfRoot(readCursor);

        // WHEN
        // insert in leftmostChild that has invalid type
        KEY key = key(0);
        var e = assertThrows(TreeInconsistencyException.class, () -> insert(key, value(0)));
        // THEN
        assertThat(e.getMessage())
                .contains(format(
                        "Index update aborted due to finding tree node that doesn't have correct type (pageId: %d, type: %d),"
                                + " when moving cursor towards " + key
                                + ". This is most likely caused by an inconsistency in the index.",
                        leftmostChild,
                        0));
    }

    @ParameterizedTest
    @MethodSource("generators")
    void mustThrowIfFoundNonTreeNodeWhenMovingToLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // GIVEN
        // root with children on two levels
        initialize();
        long someHighMultiplier = 1000;
        for (int i = 1; numberOfRootSplits < 2; i++) {
            long seed = i * someHighMultiplier;
            insert(key(seed), value(seed));
        }
        generationManager.checkpoint();

        // Set type of the left child (internal node on level 1) to something other than tree node.
        long leftmostInternal = setTypeInvalidOnLeftChildOfRoot(cursor);

        // WHEN
        // insert in leftmost child (on level 2) that will pass leftmostInternal (that has invalid type)
        KEY key = key(0);
        var e = assertThrows(TreeInconsistencyException.class, () -> insert(key, value(0)));
        // THEN
        assertThat(e.getMessage())
                .contains(format(
                        "Index update aborted due to finding tree node that doesn't have correct type (pageId: %d, type: %d),"
                                + " when moving cursor towards " + key
                                + ". This is most likely caused by an inconsistency in the index.",
                        leftmostInternal,
                        0));
    }

    @ParameterizedTest
    @MethodSource("generators")
    void mustThrowIfEndOnNonLeafWhenMovingToLeaf(
            String name, GenerationManager generationManager, boolean isCheckpointing) throws Exception {
        // GIVEN
        // root with two children
        initialize();
        long someHighMultiplier = 1000;
        for (int i = 1; numberOfRootSplits < 1; i++) {
            long seed = i * someHighMultiplier;
            insert(key(seed), value(seed));
        }
        generationManager.checkpoint();

        // Set type of the left child to something invalid (not leaf or internal)
        root.goTo(cursor);
        long leftmostChild = childAt(cursor, 0, stableGeneration, unstableGeneration);
        goTo(cursor, leftmostChild);
        cursor.putByte(TreeNodeUtil.BYTE_POS_TYPE, (byte) 2);
        root.goTo(cursor);

        // WHEN
        // insert that should take us to leftmostChild that is not a leaf
        KEY key = key(0);
        var e = assertThrows(TreeInconsistencyException.class, () -> insert(key, value(0)));
        // THEN
        assertThat(e.getMessage())
                .contains(
                        "Index update aborted due to ending up on a tree node which isn't a leaf after moving cursor towards "
                                + key
                                + ", cursor is at pageId " + cursor.getCurrentPageId()
                                + ". This is most likely caused by an inconsistency " + "in the index.");
    }

    private long setTypeInvalidOnLeftChildOfRoot(PageCursor cursor) throws IOException {
        root.goTo(cursor);
        long leftChild = childAt(cursor, 0, stableGeneration, unstableGeneration);
        goTo(cursor, leftChild);
        cursor.putByte(TreeNodeUtil.BYTE_POS_NODE_TYPE, (byte) 0);
        root.goTo(cursor);
        return leftChild;
    }

    private void consistencyCheck() throws IOException {
        long currentPageId = readCursor.getCurrentPageId();
        root.goTo(readCursor);
        ThrowingConsistencyCheckVisitor visitor = new ThrowingConsistencyCheckVisitor();
        var numThreads = Runtime.getRuntime().availableProcessors();
        try (ConsistencyCheckState state = new ConsistencyCheckState(
                null, id, visitor, CursorCreator.bind(readCursor), numThreads, ProgressMonitorFactory.NONE)) {
            var consistencyChecker = new GBPTreeConsistencyChecker<>(
                    leaf,
                    internal,
                    layout,
                    state,
                    numThreads,
                    stableGeneration,
                    unstableGeneration,
                    true,
                    Path.of("file"),
                    ctx -> cursor.duplicate(),
                    root,
                    NULL_CONTEXT_FACTORY);
            consistencyChecker.check(visitor, state.progress, GBPTreeConsistencyChecker.NO_MONITOR);
            state.awaitAllSubtasks();
        }
        goTo(readCursor, currentPageId);
    }

    private void remove(KEY toRemove, List<KEY> list, Comparator<KEY> comparator) {
        int i = indexOf(toRemove, list, comparator);
        list.remove(i);
    }

    private int indexOf(KEY theKey, List<KEY> keys, Comparator<KEY> comparator) {
        int i = 0;
        for (KEY key : keys) {
            if (comparator.compare(theKey, key) == 0) {
                return i;
            }
            i++;
        }
        return -1;
    }

    private static void giveSuccessor(PageCursor cursor, long nodeId) throws IOException {
        goTo(cursor, nodeId);
        TreeNodeUtil.setSuccessor(cursor, 42, stableGeneration, unstableGeneration);
    }

    private KEY rightmostInternalKeyInSubtree(long parentNodeId, int subtreePosition) throws IOException {
        long current = readCursor.getCurrentPageId();
        goToSubtree(parentNodeId, subtreePosition);
        boolean found = false;
        KEY rightmostKeyInSubtree = layout.newKey();
        while (TreeNodeUtil.isInternal(readCursor)) {
            int keyCount = TreeNodeUtil.keyCount(readCursor);
            if (keyCount <= 0) {
                break;
            }
            rightmostKeyInSubtree = keyAt(keyCount - 1, true);
            found = true;
            long rightmostChild = childAt(readCursor, keyCount, stableGeneration, unstableGeneration);
            goTo(readCursor, rightmostChild);
        }
        if (!found) {
            throw new IllegalArgumentException("Subtree on position " + subtreePosition + " in node " + parentNodeId
                    + " did not contain a rightmost internal key.");
        }

        goTo(readCursor, current);
        return rightmostKeyInSubtree;
    }

    private void goToSubtree(long parentNodeId, int subtreePosition) throws IOException {
        goTo(readCursor, parentNodeId);
        long subtree = childAt(readCursor, subtreePosition, stableGeneration, unstableGeneration);
        goTo(readCursor, subtree);
    }

    private long leftmostLeafInSubtree(long parentNodeId, int subtreePosition) throws IOException {
        long current = readCursor.getCurrentPageId();
        goToSubtree(parentNodeId, subtreePosition);
        long leftmostChild = current;
        while (TreeNodeUtil.isInternal(readCursor)) {
            leftmostChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
            goTo(readCursor, leftmostChild);
        }

        goTo(readCursor, current);
        return leftmostChild;
    }

    private long rightmostLeafInSubtree(long parentNodeId, int subtreePosition) throws IOException {
        long current = readCursor.getCurrentPageId();
        goToSubtree(parentNodeId, subtreePosition);
        long rightmostChild = current;
        while (TreeNodeUtil.isInternal(readCursor)) {
            int keyCount = TreeNodeUtil.keyCount(readCursor);
            rightmostChild = childAt(readCursor, keyCount, stableGeneration, unstableGeneration);
            goTo(readCursor, rightmostChild);
        }

        goTo(readCursor, current);
        return rightmostChild;
    }

    private void assertLeafNodeContainsExpectedKeys(List<KEY> expectedKeys) {
        List<KEY> actualKeys = allLeafKeys(readCursor);
        for (KEY actualKey : actualKeys) {
            GBPTreeTestUtil.contains(expectedKeys, actualKey, layout);
        }
        for (KEY expectedKey : expectedKeys) {
            GBPTreeTestUtil.contains(actualKeys, expectedKey, layout);
        }
    }

    private List<KEY> allLeafKeys(PageCursor cursor) {
        List<KEY> keys = new ArrayList<>();
        return allLeafKeys(cursor, keys);
    }

    private List<KEY> allLeafKeys(PageCursor cursor, List<KEY> keys) {
        int keyCount = TreeNodeUtil.keyCount(cursor);
        for (int i = 0; i < keyCount; i++) {
            KEY into = layout.newKey();
            leaf.keyAt(cursor, into, i, NULL_CONTEXT);
            keys.add(into);
        }
        return keys;
    }

    private int keyCount(long nodeId) throws IOException {
        long prevId = readCursor.getCurrentPageId();
        try {
            goTo(readCursor, nodeId);
            return TreeNodeUtil.keyCount(readCursor);
        } finally {
            goTo(readCursor, prevId);
        }
    }

    private int keyCount() {
        return TreeNodeUtil.keyCount(readCursor);
    }

    void initialize() {
        leaf.initialize(cursor, DATA_LAYER_FLAG, stableGeneration, unstableGeneration);
        updateRoot();
    }

    private void updateRoot() {
        root = new Root(cursor.getCurrentPageId(), unstableGeneration);
        treeLogic.initialize(cursor, ratioToKeepInLeftOnSplit, StructureWriteLog.EMPTY);
    }

    private void assertSuccessorPointerNotCrashOrBroken() throws IOException {
        assertNoCrashOrBrokenPointerInGSPP(
                null,
                readCursor,
                stableGeneration,
                unstableGeneration,
                GBPTreePointerType.successor(),
                TreeNodeUtil.BYTE_POS_SUCCESSOR,
                new ThrowingConsistencyCheckVisitor(),
                false);
    }

    private void assertKeyAssociatedWithValue(KEY key, VALUE expectedValue) throws IOException {
        KEY readKey = layout.newKey();
        var readValue = new ValueHolder<>(layout.newValue());
        int keyCount = TreeNodeUtil.keyCount(readCursor);
        int search = KeySearch.search(readCursor, leaf, key, readKey, keyCount, NULL_CONTEXT);
        assertThat(KeySearch.isHit(search)).isTrue();
        int keyPos = KeySearch.positionOf(search);
        leaf.valueAt(readCursor, readValue, keyPos, NULL_CONTEXT);
        assertEqualsValue(expectedValue, readValue.value);
    }

    private void assertKeyNotFound(KEY key) {
        KEY readKey = layout.newKey();
        int keyCount = TreeNodeUtil.keyCount(readCursor);
        int search = KeySearch.search(readCursor, leaf, key, readKey, keyCount, NULL_CONTEXT);
        assertThat(KeySearch.isHit(search)).isFalse();
    }

    private void assertSiblings(long left, long middle, long right) throws IOException {
        long origin = readCursor.getCurrentPageId();
        goTo(readCursor, middle);
        assertThat(rightSibling(readCursor, stableGeneration, unstableGeneration))
                .isEqualTo(right);
        assertThat(leftSibling(readCursor, stableGeneration, unstableGeneration))
                .isEqualTo(left);
        if (left != TreeNodeUtil.NO_NODE_FLAG) {
            goTo(readCursor, left);
            assertThat(rightSibling(readCursor, stableGeneration, unstableGeneration))
                    .isEqualTo(middle);
        }
        if (right != TreeNodeUtil.NO_NODE_FLAG) {
            goTo(readCursor, right);
            assertThat(leftSibling(readCursor, stableGeneration, unstableGeneration))
                    .isEqualTo(middle);
        }
        goTo(readCursor, origin);
    }

    // KEEP even if unused
    @SuppressWarnings("unused")
    private void printTree() throws IOException {
        long currentPageId = cursor.getCurrentPageId();
        cursor.next(root.id());
        new GBPTreeStructure<>(null, null, null, layout, leaf, internal, stableGeneration, unstableGeneration)
                .visitTree(cursor, new PrintingGBPTreeVisitor<>(PrintConfig.defaults()), NULL_CONTEXT);
        cursor.next(currentPageId);
    }

    KEY key(long seed) {
        return layout.key(seed);
    }

    VALUE value(long seed) {
        return layout.value(seed);
    }

    private long getSeed(KEY key) {
        return layout.keySeed(key);
    }

    private void newRootFromSplit(StructurePropagation<KEY> split) throws IOException {
        assertThat(split.hasRightKeyInsert).isTrue();
        long rootId = id.acquireNewId(stableGeneration, unstableGeneration, CursorCreator.bind(cursor));
        goTo(cursor, rootId);
        internal.initialize(cursor, DATA_LAYER_FLAG, stableGeneration, unstableGeneration);
        internal.setChildAt(cursor, split.midChild, 0, stableGeneration, unstableGeneration);
        internal.insertKeyAndRightChildAt(
                cursor, split.rightKey, split.rightChild, 0, 0, stableGeneration, unstableGeneration, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, 1);
        split.hasRightKeyInsert = false;
        updateRoot();
    }

    private void assertSiblingOrderAndPointers(long... children) throws IOException {
        long currentPageId = readCursor.getCurrentPageId();
        RightmostInChain rightmost = new RightmostInChain(null, true);
        GenerationKeeper generationTarget = new GenerationKeeper();
        ThrowingConsistencyCheckVisitor visitor = new ThrowingConsistencyCheckVisitor();
        for (long child : children) {
            goTo(readCursor, child);
            long leftSibling =
                    TreeNodeUtil.leftSibling(readCursor, stableGeneration, unstableGeneration, generationTarget);
            long leftSiblingGeneration = generationTarget.generation;
            long rightSibling =
                    TreeNodeUtil.rightSibling(readCursor, stableGeneration, unstableGeneration, generationTarget);
            long rightSiblingGeneration = generationTarget.generation;
            rightmost.assertNext(
                    readCursor,
                    TreeNodeUtil.generation(readCursor),
                    pointer(leftSibling),
                    leftSiblingGeneration,
                    pointer(rightSibling),
                    rightSiblingGeneration,
                    visitor);
        }
        rightmost.assertLast(visitor);
        goTo(readCursor, currentPageId);
    }

    KEY keyAt(long nodeId, int pos, boolean isInternal) {
        KEY readKey = layout.newKey();
        long prevId = readCursor.getCurrentPageId();
        try {
            readCursor.next(nodeId);
            if (isInternal) {
                return internal.keyAt(readCursor, readKey, pos, NULL_CONTEXT);
            }
            return leaf.keyAt(readCursor, readKey, pos, NULL_CONTEXT);
        } finally {
            readCursor.next(prevId);
        }
    }

    private KEY keyAt(int pos, boolean isInternal) {
        KEY into = layout.newKey();
        if (isInternal) {
            return internal.keyAt(readCursor, into, pos, NULL_CONTEXT);
        }
        return leaf.keyAt(readCursor, into, pos, NULL_CONTEXT);
    }

    private VALUE valueAt(long nodeId, int pos) throws IOException {
        var readValue = new ValueHolder<>(layout.newValue());
        long prevId = readCursor.getCurrentPageId();
        try {
            readCursor.next(nodeId);
            return leaf.valueAt(readCursor, readValue, pos, NULL_CONTEXT).value;
        } finally {
            readCursor.next(prevId);
        }
    }

    private VALUE valueAt(int pos) throws IOException {
        ValueHolder<VALUE> value = new ValueHolder<>(layout.newValue());
        return leaf.valueAt(readCursor, value, pos, NULL_CONTEXT).value;
    }

    void insert(KEY key, VALUE value) throws IOException {
        insert(key, value, overwrite());
    }

    private void insert(KEY key, VALUE value, ValueMerger<KEY, VALUE> valueMerger) throws IOException {
        structurePropagation.hasRightKeyInsert = false;
        structurePropagation.hasMidChildUpdate = false;
        treeLogic.insert(
                cursor,
                structurePropagation,
                key,
                value,
                valueMerger,
                true,
                stableGeneration,
                unstableGeneration,
                NULL_CONTEXT);
        handleAfterChange();
    }

    private void handleAfterChange() throws IOException {
        if (structurePropagation.hasRightKeyInsert) {
            newRootFromSplit(structurePropagation);
            numberOfRootSplits++;
        }
        if (structurePropagation.hasMidChildUpdate) {
            structurePropagation.hasMidChildUpdate = false;
            updateRoot();
            numberOfRootSuccessors++;
        }
    }

    private void remove(KEY key, VALUE into) throws IOException {
        treeLogic.remove(
                cursor,
                structurePropagation,
                key,
                new ValueHolder<>(into),
                stableGeneration,
                unstableGeneration,
                NULL_CONTEXT);
        handleAfterChange();
    }

    private interface GenerationManager {
        void checkpoint();

        void recovery();

        GenerationManager NO_OP_GENERATION = new GenerationManager() {
            @Override
            public void checkpoint() {
                // Do nothing
            }

            @Override
            public void recovery() {
                // Do nothing
            }
        };

        GenerationManager DEFAULT = new GenerationManager() {
            @Override
            public void checkpoint() {
                stableGeneration = unstableGeneration;
                unstableGeneration++;
            }

            @Override
            public void recovery() {
                unstableGeneration++;
            }
        };
    }

    private static void goTo(PageCursor cursor, long pageId) throws IOException {
        PageCursorUtil.goTo(cursor, "test", pointer(pageId));
    }

    private static void goToSuccessor(PageCursor cursor) throws IOException {
        long newestGeneration = newestGeneration(cursor, stableGeneration, unstableGeneration);
        goTo(cursor, newestGeneration);
    }

    private static void goToSuccessor(PageCursor cursor, long targetNode) throws IOException {
        goTo(cursor, targetNode);
        goToSuccessor(cursor);
    }

    private long childAt(PageCursor cursor, int pos, long stableGeneration, long unstableGeneration) {
        return pointer(internal.childAt(cursor, pos, stableGeneration, unstableGeneration));
    }

    private static long rightSibling(PageCursor cursor, long stableGeneration, long unstableGeneration) {
        return pointer(TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration));
    }

    private static long leftSibling(PageCursor cursor, long stableGeneration, long unstableGeneration) {
        return pointer(TreeNodeUtil.leftSibling(cursor, stableGeneration, unstableGeneration));
    }

    private static long successor(PageCursor cursor, long stableGeneration, long unstableGeneration) {
        return pointer(TreeNodeUtil.successor(cursor, stableGeneration, unstableGeneration));
    }

    private static long newestGeneration(PageCursor cursor, long stableGeneration, long unstableGeneration)
            throws IOException {
        long current = cursor.getCurrentPageId();
        long successor = current;
        do {
            goTo(cursor, successor);
            successor = pointer(TreeNodeUtil.successor(cursor, stableGeneration, unstableGeneration));
        } while (successor != TreeNodeUtil.NO_NODE_FLAG);
        successor = cursor.getCurrentPageId();
        goTo(cursor, current);
        return successor;
    }

    private void assertNotEqualsKey(KEY key1, KEY key2) {
        assertNotEquals(
                0,
                layout.compare(key1, key2),
                format("expected no not equal, key1=%s, key2=%s", key1.toString(), key2.toString()));
    }

    private void assertEqualsKey(KEY expected, KEY actual) {
        assertEquals(
                0,
                layout.compare(expected, actual),
                format("expected equal, expected=%s, actual=%s", expected, actual));
    }

    private void assertEqualsValue(VALUE expected, VALUE actual) {
        assertEquals(
                0,
                layout.compareValue(expected, actual),
                format("expected equal, expected=%s, actual=%s", expected, actual));
    }
}
