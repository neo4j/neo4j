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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.SeekCursor.DEFAULT_MAX_READ_AHEAD;
import static org.neo4j.index.internal.gbptree.SeekCursor.LEAF_LEVEL;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.DATA_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.ValueMergers.overwrite;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;
import org.neo4j.io.pagecache.impl.DelegatingPageCursor;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
abstract class SeekCursorTestBase<KEY, VALUE> {
    private static final int PAGE_SIZE = 256;
    private static long stableGeneration = GenerationSafePointer.MIN_GENERATION;
    private static long unstableGeneration = stableGeneration + 1;
    private static final LongSupplier generationSupplier =
            () -> Generation.generation(stableGeneration, unstableGeneration);
    private static final RootCatchup failingRootCatchup = (id, context) -> {
        throw new AssertionError("Should not happen");
    };
    private static final Consumer<Throwable> exceptionDecorator = t -> {};

    @Inject
    private RandomSupport random;

    private TestLayout<KEY, VALUE> layout;
    private LeafNodeBehaviour<KEY, VALUE> leaf;
    private InternalNodeBehaviour<KEY> internal;
    private InternalTreeLogic<KEY, VALUE> treeLogic;
    private StructurePropagation<KEY> structurePropagation;

    private PageAwareByteArrayCursor cursor;
    private PageAwareByteArrayCursor utilCursor;
    private SimpleIdProvider id;

    private long rootId;
    private long rootGeneration;
    private int numberOfRootSplits;

    @BeforeEach
    void setUp() throws IOException {
        cursor = new PageAwareByteArrayCursor(PAGE_SIZE);
        utilCursor = cursor.duplicate();
        id = new SimpleIdProvider(cursor::duplicate);

        layout = getLayout();
        OffloadPageCursorFactory pcFactory = (id, flags, cursorContext) -> cursor.duplicate(id);
        OffloadIdValidator idValidator = OffloadIdValidator.ALWAYS_TRUE;
        OffloadStoreImpl<KEY, VALUE> offloadStore =
                new OffloadStoreImpl<>(layout, id, pcFactory, idValidator, PAGE_SIZE);
        leaf = getLeaf(PAGE_SIZE, layout, offloadStore);
        internal = getInternal(PAGE_SIZE, layout, offloadStore);
        treeLogic = new InternalTreeLogic<>(
                id, leaf, internal, layout, NO_MONITOR, TreeWriterCoordination.NO_COORDINATION, DATA_LAYER_FLAG);
        structurePropagation = new StructurePropagation<>(layout.newKey(), layout.newKey(), layout.newKey());

        long firstPage = id.acquireNewId(stableGeneration, unstableGeneration, CursorCreator.bind(cursor));
        goTo(cursor, firstPage);
        goTo(utilCursor, firstPage);

        leaf.initialize(cursor, DATA_LAYER_FLAG, stableGeneration, unstableGeneration);
        updateRoot();
    }

    abstract TestLayout<KEY, VALUE> getLayout();

    protected abstract LeafNodeBehaviour<KEY, VALUE> getLeaf(
            int pageSize, Layout<KEY, VALUE> layout, OffloadStore<KEY, VALUE> offloadStore);

    protected abstract InternalNodeBehaviour<KEY> getInternal(
            int pageSize, Layout<KEY, VALUE> layout, OffloadStore<KEY, VALUE> offloadStore);

    private static void goTo(PageCursor cursor, long pageId) throws IOException {
        PageCursorUtil.goTo(cursor, "test", pointer(pageId));
    }

    private void updateRoot() {
        rootId = cursor.getCurrentPageId();
        rootGeneration = unstableGeneration;
        treeLogic.initialize(cursor, InternalTreeLogic.DEFAULT_SPLIT_RATIO, StructureWriteLog.EMPTY);
    }

    /* NO CONCURRENT INSERT */

    @Test
    void mustFindEntriesWithinRangeInBeginningOfSingleLeaf() throws Exception {
        // GIVEN
        long lastSeed = fullLeaf();
        long fromInclusive = 0;
        long toExclusive = lastSeed / 2;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertRangeInSingleLeaf(fromInclusive, toExclusive, cursor);
        }
    }

    @Test
    void mustFindEntriesWithinRangeInBeginningOfSingleLeafBackwards() throws Exception {
        // GIVEN
        long maxKeyCount = fullLeaf();
        long fromInclusive = maxKeyCount / 2;
        long toExclusive = -1;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertRangeInSingleLeaf(fromInclusive, toExclusive, cursor);
        }
    }

    @Test
    void mustFindEntriesWithinRangeInEndOfSingleLeaf() throws Exception {
        // GIVEN
        long maxKeyCount = fullLeaf();
        long fromInclusive = maxKeyCount / 2;
        long toExclusive = maxKeyCount;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertRangeInSingleLeaf(fromInclusive, toExclusive, cursor);
        }
    }

    @Test
    void mustFindEntriesWithinRangeInEndOfSingleLeafBackwards() throws Exception {
        // GIVEN
        long maxKeyCount = fullLeaf();
        long fromInclusive = maxKeyCount - 1;
        long toExclusive = maxKeyCount / 2;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertRangeInSingleLeaf(fromInclusive, toExclusive, cursor);
        }
    }

    @Test
    void mustFindEntriesWithinRangeInMiddleOfSingleLeaf() throws Exception {
        // GIVEN
        long maxKeyCount = fullLeaf();
        long middle = maxKeyCount / 2;
        long fromInclusive = middle / 2;
        long toExclusive = (middle + maxKeyCount) / 2;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertRangeInSingleLeaf(fromInclusive, toExclusive, cursor);
        }
    }

    @Test
    void mustFindEntriesWithinRangeInMiddleOfSingleLeafBackwards() throws Exception {
        // GIVEN
        long maxKeyCount = fullLeaf();
        long middle = maxKeyCount / 2;
        long fromInclusive = (middle + maxKeyCount) / 2;
        long toExclusive = middle / 2;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertRangeInSingleLeaf(fromInclusive, toExclusive, cursor);
        }
    }

    @Test
    void mustFindEntriesSpanningTwoLeaves() throws Exception {
        // GIVEN
        long i = fullLeaf();
        long left = createRightSibling(cursor);
        i = fullLeaf(i);
        cursor.next(left);

        long fromInclusive = 0;
        long toExclusive = i;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertRangeInSingleLeaf(fromInclusive, toExclusive, cursor);
        }
    }

    @Test
    void mustFindEntriesSpanningTwoLeavesBackwards() throws Exception {
        // GIVEN
        long i = fullLeaf();
        createRightSibling(cursor);
        i = fullLeaf(i);

        long fromInclusive = i - 1;
        long toExclusive = -1;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertRangeInSingleLeaf(fromInclusive, toExclusive, cursor);
        }
    }

    @Test
    void mustFindEntriesOnSecondLeafWhenStartingFromFirstLeaf() throws Exception {
        // GIVEN
        long i = fullLeaf();
        long left = createRightSibling(cursor);
        long j = fullLeaf(i);
        cursor.next(left);

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(i, j)) {
            // THEN
            assertRangeInSingleLeaf(i, j, cursor);
        }
    }

    @Test
    void mustFindEntriesOnSecondLeafWhenStartingFromFirstLeafBackwards() throws Exception {
        // GIVEN
        long leftKeyCount = fullLeaf();
        long left = createRightSibling(cursor);
        fullLeaf(leftKeyCount);
        cursor.next(left);

        long fromInclusive = leftKeyCount - 1;
        long toExclusive = -1;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertRangeInSingleLeaf(fromInclusive, toExclusive, cursor);
        }
    }

    @Test
    void mustNotContinueToSecondLeafAfterFindingEndOfRangeInFirst() throws Exception {
        AtomicBoolean nextCalled = new AtomicBoolean();
        PageCursor pageCursorSpy = new DelegatingPageCursor(cursor) {
            @Override
            public boolean next(long pageId) throws IOException {
                nextCalled.set(true);
                return super.next(pageId);
            }
        };

        // GIVEN
        long i = fullLeaf();
        long left = createRightSibling(cursor);
        long j = fullLeaf(i);

        long fromInclusive = j - 1;
        long toExclusive = i;

        // Reset
        nextCalled.set(false);

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive, pageCursorSpy)) {
            // THEN
            assertRangeInSingleLeaf(fromInclusive, toExclusive, cursor);
        }
        assertFalse(nextCalled.get(), "Cursor continued to next leaf even though end of range is within first leaf");
    }

    @Test
    void shouldHandleEmptyRange() throws IOException {
        // GIVEN
        insert(0);
        insert(2);
        long fromInclusive = 1;
        long toExclusive = 2;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldHandleEmptyRangeBackwards() throws IOException {
        // GIVEN
        insert(0);
        insert(2);
        long fromInclusive = 1;
        long toExclusive = 0;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldHandleBackwardsWithNoExactHitOnFromInclusive() throws IOException {
        // GIVEN
        insert(0);
        insert(2);
        long fromInclusive = 3;
        long toExclusive = 0;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertTrue(cursor.next());
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldHandleBackwardsWithExactHitOnFromInclusive() throws IOException {
        // GIVEN
        insert(0);
        insert(2);
        long fromInclusive = 2;
        long toExclusive = 0;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            assertTrue(cursor.next());
            assertFalse(cursor.next());
        }
    }

    @Test
    void mustFindKeysWhenGivenRangeStartingOutsideStartOfData() throws Exception {
        // Given
        // [ 0 1... maxKeyCount-1]
        long maxKeyCount = fullLeaf();

        long expectedKey = 0;
        try (SeekCursor<KEY, VALUE> seekCursor = seekCursor(-1, maxKeyCount - 1)) {
            while (seekCursor.next()) {
                assertKeyAndValue(seekCursor, expectedKey);
                expectedKey++;
            }
        }
        assertEquals(expectedKey, maxKeyCount - 1);
    }

    @Test
    void mustFindKeysWhenGivenRangeStartingOutsideStartOfDataBackwards() throws Exception {
        // Given
        // [ 0 1... maxKeyCount-1]
        long maxKeyCount = fullLeaf();

        long expectedKey = maxKeyCount - 1;
        try (SeekCursor<KEY, VALUE> seekCursor = seekCursor(maxKeyCount, 0)) {
            while (seekCursor.next()) {
                assertKeyAndValue(seekCursor, expectedKey);
                expectedKey--;
            }
        }
        assertEquals(expectedKey, 0);
    }

    @Test
    void mustFindKeysWhenGivenRangeEndingOutsideEndOfData() throws Exception {
        // Given
        // [ 0 1... maxKeyCount-1]
        long maxKeyCount = fullLeaf();

        long expectedKey = 0;
        try (SeekCursor<KEY, VALUE> seekCursor = seekCursor(0, maxKeyCount + 1)) {
            while (seekCursor.next()) {
                assertKeyAndValue(seekCursor, expectedKey);
                expectedKey++;
            }
        }
        assertEquals(expectedKey, maxKeyCount);
    }

    @Test
    void mustFindKeysWhenGivenRangeEndingOutsideEndOfDataBackwards() throws Exception {
        // Given
        // [ 0 1... maxKeyCount-1]
        long maxKeyCount = fullLeaf();

        long expectedKey = maxKeyCount - 1;
        try (SeekCursor<KEY, VALUE> seekCursor = seekCursor(maxKeyCount - 1, -2)) {
            while (seekCursor.next()) {
                assertKeyAndValue(seekCursor, expectedKey);
                expectedKey--;
            }
        }
        assertEquals(expectedKey, -1);
    }

    @Test
    void mustStartReadingFromCorrectLeafWhenRangeStartWithKeyEqualToPrimKey() throws Exception {
        // given
        long lastSeed = rootWithTwoLeaves();
        KEY primKey = layout.newKey();
        internal.keyAt(cursor, primKey, 0, NULL_CONTEXT);
        long expectedNext = getSeed(primKey);
        long rightChild =
                GenerationSafePointerPair.pointer(internal.childAt(cursor, 1, stableGeneration, unstableGeneration));

        // when
        try (SeekCursor<KEY, VALUE> seek = seekCursor(expectedNext, lastSeed)) {
            assertEquals(rightChild, cursor.getCurrentPageId());
            while (seek.next()) {
                assertKeyAndValue(seek, expectedNext);
                expectedNext++;
            }
        }

        // then
        assertEquals(lastSeed, expectedNext);
    }

    @Test
    void mustStartReadingFromCorrectLeafWhenRangeStartWithKeyEqualToPrimKeyBackwards() throws Exception {
        // given
        rootWithTwoLeaves();
        KEY primKey = layout.newKey();
        internal.keyAt(cursor, primKey, 0, NULL_CONTEXT);
        long expectedNext = getSeed(primKey);
        long rightChild =
                GenerationSafePointerPair.pointer(internal.childAt(cursor, 1, stableGeneration, unstableGeneration));

        // when
        try (SeekCursor<KEY, VALUE> seek = seekCursor(expectedNext, -1)) {
            assertEquals(rightChild, cursor.getCurrentPageId());
            while (seek.next()) {
                assertKeyAndValue(seek, expectedNext);
                expectedNext--;
            }
        }

        // then
        assertEquals(-1, expectedNext);
    }

    @Test
    void exactMatchInStableRoot() throws Exception {
        // given
        long maxKeyCount = fullLeaf();

        // when
        for (long i = 0; i < maxKeyCount; i++) {
            assertExactMatch(i);
        }
    }

    @Test
    void exactMatchInLeaves() throws Exception {
        // given
        long lastSeed = rootWithTwoLeaves();

        // when
        for (long i = 0; i < lastSeed; i++) {
            assertExactMatch(i);
        }
    }

    private long rootWithTwoLeaves() throws IOException {
        long i = 0;
        for (; numberOfRootSplits < 1; i++) {
            insert(i);
        }
        return i;
    }

    private void assertExactMatch(long i) throws IOException {
        try (SeekCursor<KEY, VALUE> seeker = seekCursor(i, i)) {
            // then
            assertTrue(seeker.next());
            assertEqualsKey(key(i), seeker.key());
            assertEqualsValue(value(i), seeker.value());
            assertFalse(seeker.next());
        }
    }

    /* INSERT */

    @Test
    void mustFindNewKeyInsertedAfterOfSeekPoint() throws Exception {
        // GIVEN
        int middle = 2;
        for (int i = 0; i < middle; i++) {
            append(i);
        }
        long fromInclusive = 0;
        long toExclusive = middle + 1; // Will insert middle later

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while (readKeys < stopPoint && cursor.next()) {
                assertKeyAndValue(cursor, readKeys);
                readKeys++;
            }

            // Seeker pauses and writer insert new key at the end of leaf
            append(middle);
            this.cursor.forceRetry();

            // Seeker continue
            while (cursor.next()) {
                assertKeyAndValue(cursor, readKeys);
                readKeys++;
            }
            assertEquals(toExclusive, readKeys);
        }
    }

    @Test
    void mustFindNewKeyInsertedAfterOfSeekPointBackwards() throws Exception {
        // GIVEN
        int middle = 2;
        for (int i = 1; i <= middle; i++) {
            append(i);
        }
        long fromInclusive = middle;
        long toExclusive = 0; // Will insert 0 later

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while (readKeys < stopPoint && cursor.next()) {
                assertKeyAndValue(cursor, middle - readKeys);
                readKeys++;
            }

            // Seeker pauses and writer insert new key at the end of leaf
            insertIn(0, 0);
            this.cursor.forceRetry();

            // Seeker continue
            while (cursor.next()) {
                assertKeyAndValue(cursor, middle - readKeys);
                readKeys++;
            }
            assertEquals(toExclusive, middle - readKeys);
        }
    }

    @Test
    void mustFindKeyInsertedOnSeekPosition() throws Exception {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        int middle = 2;
        for (int i = 0; i < middle; i++) {
            long key = i * 2;
            append(key);
            expected.add(key);
        }
        long fromInclusive = 0;
        long toExclusive = middle * 2;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while (readKeys < stopPoint && cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }

            // Seeker pauses and writer insert new key in position where seeker will read next
            long midInsert = expected.get(stopPoint) - 1;
            insertIn(stopPoint, midInsert);
            expected.add(stopPoint, midInsert);
            this.cursor.forceRetry();

            while (cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }
            assertEquals(expected.size(), readKeys);
        }
    }

    @Test
    void mustFindKeyInsertedOnSeekPositionBackwards() throws Exception {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        int middle = 2;
        for (int i = middle; i > 0; i--) {
            long key = i * 2;
            insert(key);
            expected.add(key);
        }
        long fromInclusive = middle * 2;
        long toExclusive = 0;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while (readKeys < stopPoint && cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }

            // Seeker pauses and writer insert new key in position where seeker will read next
            long midInsert = expected.get(stopPoint) + 1;
            insert(midInsert);
            expected.add(stopPoint, midInsert);
            this.cursor.forceRetry();

            while (cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }
            assertEquals(expected.size(), readKeys);
        }
    }

    @Test
    void mustNotFindKeyInsertedBeforeOfSeekPoint() throws Exception {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        int middle = 2;
        for (int i = 0; i < middle; i++) {
            long key = i * 2;
            append(key);
            expected.add(key);
        }
        long fromInclusive = 0;
        long toExclusive = middle * 2;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while (readKeys < stopPoint && cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }

            // Seeker pauses and writer insert new key to the left of seekers next position
            long midInsert = expected.get(readKeys - 1) - 1;
            insertIn(stopPoint - 1, midInsert);
            this.cursor.forceRetry();

            while (cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }
            assertEquals(expected.size(), readKeys);
        }
    }

    @Test
    void mustNotFindKeyInsertedBeforeOfSeekPointBackwards() throws Exception {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        int middle = 2;
        for (int i = middle; i > 0; i--) {
            long key = i * 2;
            insert(key);
            expected.add(key);
        }
        long fromInclusive = middle * 2;
        long toExclusive = 0;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while (readKeys < stopPoint && cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }

            // Seeker pauses and writer insert new key to the left of seekers next position
            long midInsert = expected.get(readKeys - 1) + 1;
            insert(midInsert);
            this.cursor.forceRetry();

            while (cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }
            assertEquals(expected.size(), readKeys);
        }
    }

    /* INSERT INTO SPLIT */

    @Test
    void mustContinueToNextLeafWhenRangeIsSplitIntoRightLeafAndPosToLeft() throws Exception {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        long maxKeyCount = fullLeaf(expected);
        long fromInclusive = 0;
        long toExclusive = maxKeyCount + 1; // We will add maxKeyCount later

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            long middle = maxKeyCount / 2;
            long stopPoint = middle / 2;
            int readKeys = 0;
            while (readKeys < stopPoint && cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add(maxKeyCount);
            insert(maxKeyCount);

            seekCursor.forceRetry();

            while (cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }
            assertEquals(expected.size(), readKeys);
        }
    }

    @Test
    void mustContinueToNextLeafWhenRangeIsSplitIntoRightLeafAndPosToRightBackwards() throws Exception {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        long lastSeed = fullLeaf(1, expected);
        Collections.reverse(expected); // Because backwards
        long fromInclusive = lastSeed - 1;
        long toExclusive = -1; // We will add 0 later

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> seeker = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            long middle = lastSeed / 2;
            long stopPoint = middle / 2;
            int readKeys = 0;
            while (readKeys < stopPoint && seeker.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(seeker, key);
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add(0L);
            insert(0L);

            seekCursor.forceRetry();

            while (seeker.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(seeker, key);
                readKeys++;
            }
            assertEquals(expected.size(), readKeys);
        }
    }

    @Test
    void mustContinueToNextLeafWhenRangeIsSplitIntoRightLeafAndPosToRight() throws Exception {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        long maxKeyCount = fullLeaf(expected);
        long fromInclusive = 0;
        long toExclusive = maxKeyCount + 1; // We will add maxKeyCount later

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            long middle = maxKeyCount / 2;
            long stopPoint = middle + (middle / 2);
            int readKeys = 0;
            while (readKeys < stopPoint && cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add(maxKeyCount);
            insert(maxKeyCount);
            seekCursor.forceRetry();

            while (cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }
            assertEquals(expected.size(), readKeys);
        }
    }

    @Test
    void mustContinueToNextLeafWhenRangeIsSplitIntoRightLeafAndPosToLeftBackwards() throws Exception {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        long lastSeed = fullLeaf(1, expected);
        Collections.reverse(expected); // Because backwards
        long fromInclusive = lastSeed - 1;
        long toExclusive = -1; // We will add 0 later

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            long middle = lastSeed / 2;
            long stopPoint = middle + (middle / 2);
            int readKeys = 0;
            while (readKeys < stopPoint && cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add(0L);
            insert(0L);
            seekCursor.forceRetry();

            while (cursor.next()) {
                long key = expected.get(readKeys);
                assertKeyAndValue(cursor, key);
                readKeys++;
            }
            assertEquals(expected.size(), readKeys);
        }
    }

    /* REMOVE */

    @Test
    void mustNotFindKeyRemovedInFrontOfSeeker() throws Exception {
        // GIVEN
        // [0 1 ... maxKeyCount-1]
        long maxKeyCount = fullLeaf();
        long fromInclusive = 0;
        long toExclusive = maxKeyCount;

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive)) {
            // THEN
            long middle = maxKeyCount / 2;
            int readKeys = 0;
            while (readKeys < middle && cursor.next()) {
                long key = readKeys;
                assertKeyAndValue(cursor, key);
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            // [0 1 ... maxKeyCount-2]
            removeAtPos((int) maxKeyCount - 1);
            this.cursor.forceRetry();

            while (cursor.next()) {
                long key = readKeys;
                assertKeyAndValue(cursor, key);
                readKeys++;
            }
            assertEquals(maxKeyCount - 1, readKeys);
        }
    }

    /* INCONSISTENCY */

    @Test
    void mustThrowIfStuckInInfiniteRootCatchup() throws IOException {
        // given
        rootWithTwoLeaves();

        // Find left child and corrupt it by overwriting type to make it look like freelist node instead of tree node.
        goTo(utilCursor, rootId);
        long leftChild = internal.childAt(utilCursor, 0, stableGeneration, unstableGeneration);
        goTo(utilCursor, leftChild);
        utilCursor.putByte(TreeNodeUtil.BYTE_POS_NODE_TYPE, TreeNodeUtil.NODE_TYPE_FREE_LIST_NODE);

        // when
        RootCatchup tripCountingRootCatchup = new TripCountingRootCatchup(context -> new Root(rootId, rootGeneration));
        assertThrows(TreeInconsistencyException.class, () -> {
            try (SeekCursor<KEY, VALUE> seek =
                    seekCursor(0, 0, cursor, stableGeneration, unstableGeneration, tripCountingRootCatchup)) {
                seek.next();
            }
        });
    }

    private long fullLeaf(List<Long> expectedSeeds) throws IOException {
        return fullLeaf(0, expectedSeeds);
    }

    private long fullLeaf(long firstSeed) throws IOException {
        return fullLeaf(firstSeed, new ArrayList<>());
    }

    private long fullLeaf(long firstSeed, List<Long> expectedSeeds) throws IOException {
        int keyCount = 0;
        KEY key = key(firstSeed + keyCount);
        VALUE value = value(firstSeed + keyCount);
        while (leaf.overflow(cursor, keyCount, key, value) == Overflow.NO) {
            leaf.insertKeyValueAt(
                    cursor, key, value, keyCount, keyCount, stableGeneration, unstableGeneration, NULL_CONTEXT);
            expectedSeeds.add(firstSeed + keyCount);
            keyCount++;
            key = key(firstSeed + keyCount);
            value = value(firstSeed + keyCount);
        }
        TreeNodeUtil.setKeyCount(cursor, keyCount);
        return firstSeed + keyCount;
    }

    /**
     * @return next seed to be inserted
     */
    private long fullLeaf() throws IOException {
        return fullLeaf(0);
    }

    private KEY key(long seed) {
        return layout.key(seed);
    }

    private VALUE value(long seed) {
        return layout.value(seed);
    }

    private long getSeed(KEY primKey) {
        return layout.keySeed(primKey);
    }

    @Test
    void mustNotFindKeyRemovedInFrontOfSeekerBackwards() throws Exception {
        // GIVEN
        // [1 2 ... maxKeyCount]
        long lastSeed = fullLeaf(1);
        long maxKeyCount = lastSeed - 1;
        long fromInclusive = maxKeyCount;
        long toExclusive = 0;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> seeker = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            // THEN
            long middle = maxKeyCount / 2;
            int readKeys = 0;
            while (readKeys < middle && seeker.next()) {
                assertKeyAndValue(seeker, maxKeyCount - readKeys);
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            // [2 ... maxKeyCount]
            remove(1);
            seekCursor.forceRetry();

            while (seeker.next()) {
                assertKeyAndValue(seeker, maxKeyCount - readKeys);
                readKeys++;
            }
            assertEquals(maxKeyCount - 1, readKeys);
        }
    }

    @Test
    void mustFindKeyMovedPassedSeekerBecauseOfRemove() throws Exception {
        // GIVEN
        // [0 1 ... maxKeyCount-1]
        long maxKeyCount = fullLeaf();
        long fromInclusive = 0;
        long toExclusive = maxKeyCount;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            // THEN
            long middle = maxKeyCount / 2;
            int readKeys = 0;
            while (readKeys < middle && cursor.next()) {
                long key = readKeys;
                assertKeyAndValue(cursor, key);
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            // [1 ... maxKeyCount-1]
            removeAtPos(0);
            seekCursor.forceRetry();

            while (cursor.next()) {
                long key = readKeys;
                assertKeyAndValue(cursor, key);
                readKeys++;
            }
            assertEquals(maxKeyCount, readKeys);
        }
    }

    @Test
    void mustFindKeyMovedPassedSeekerBecauseOfRemoveBackwards() throws Exception {
        // GIVEN
        // [1 2... maxKeyCount]
        long lastSeed = fullLeaf(1);
        long maxKeyCount = lastSeed - 1;
        long fromInclusive = maxKeyCount;
        long toExclusive = 0;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            // THEN
            long middle = maxKeyCount / 2;
            int readKeys = 0;
            while (readKeys < middle && cursor.next()) {
                assertKeyAndValue(cursor, maxKeyCount - readKeys);
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            // [1 ... maxKeyCount-1]
            remove(maxKeyCount);
            seekCursor.forceRetry();

            while (cursor.next()) {
                assertKeyAndValue(cursor, maxKeyCount - readKeys);
                readKeys++;
            }
            assertEquals(maxKeyCount, readKeys);
        }
    }

    @Test
    void mustFindKeyMovedSeekerBecauseOfRemoveOfMostRecentReturnedKey() throws Exception {
        // GIVEN
        long maxKeyCount = fullLeaf();
        long fromInclusive = 0;
        long toExclusive = maxKeyCount;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            // THEN
            long middle = maxKeyCount / 2;
            int readKeys = 0;
            while (readKeys < middle && cursor.next()) {
                assertKeyAndValue(cursor, readKeys);
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            remove(readKeys - 1);
            seekCursor.forceRetry();

            while (cursor.next()) {
                assertKeyAndValue(cursor, readKeys);
                readKeys++;
            }
            assertEquals(maxKeyCount, readKeys);
        }
    }

    @Test
    void mustFindKeyMovedSeekerBecauseOfRemoveOfMostRecentReturnedKeyBackwards() throws Exception {
        // GIVEN
        long i = fullLeaf(1);
        long maxKeyCount = i - 1;
        long fromInclusive = i - 1;
        long toExclusive = 0;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            // THEN
            long middle = maxKeyCount / 2;
            int readKeys = 0;
            while (readKeys < middle && cursor.next()) {
                assertKeyAndValue(cursor, maxKeyCount - readKeys);
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            remove(maxKeyCount - readKeys + 1);
            seekCursor.forceRetry();

            while (cursor.next()) {
                assertKeyAndValue(cursor, maxKeyCount - readKeys);
                readKeys++;
            }
            assertEquals(maxKeyCount, readKeys);
        }
    }

    @Test
    void mustRereadHeadersOnRetry() throws Exception {
        // GIVEN
        int keyCount = 2;
        insertKeysAndValues(keyCount);
        KEY from = key(0);
        KEY to = key(keyCount + 1); // +1 because we're adding one more down below

        // WHEN
        try (SeekCursor<KEY, VALUE> cursor = new SeekCursor<>(
                        this.cursor, layout, leaf, internal, generationSupplier, exceptionDecorator, NULL_CONTEXT)
                .initialize(
                        rootInitializer(unstableGeneration),
                        failingRootCatchup,
                        from,
                        to,
                        1,
                        LEAF_LEVEL,
                        SeekCursor.NO_MONITOR)) {
            // reading a couple of keys
            assertTrue(cursor.next());
            assertEqualsKey(key(0), cursor.key());

            // and WHEN a change happens
            append(keyCount);
            this.cursor.forceRetry();

            // THEN at least keyCount should be re-read on next()
            assertTrue(cursor.next());

            // and the new key should be found in the end as well
            assertEqualsKey(key(1), cursor.key());
            long lastFoundKey = 1;
            while (cursor.next()) {
                assertEqualsKey(key(lastFoundKey + 1), cursor.key());
                lastFoundKey = getSeed(cursor.key());
            }
            assertEquals(keyCount, lastFoundKey);
        }
    }

    /* REBALANCE (when rebalance is implemented) */

    @Test
    void mustFindRangeWhenCompletelyRebalancedToTheRightBeforeCallToNext() throws Exception {
        // given
        long key = 10;
        while (numberOfRootSplits == 0) {
            insert(key);
            key++;
        }

        // ... enough keys in left child to be rebalanced to the right
        for (long smallKey = 0; smallKey < 2; smallKey++) {
            insert(smallKey);
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate(rootId);
        readCursor.next();
        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        readCursor.next(pointer(leftChild));
        int keyCount = TreeNodeUtil.keyCount(readCursor);
        KEY readKey = layout.newKey();
        leaf.keyAt(readCursor, readKey, keyCount - 1, NULL_CONTEXT);
        long fromInclusive = getSeed(readKey);
        long toExclusive = fromInclusive + 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor(cursor.duplicate(rootId));
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> seeker = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            triggerUnderflowAndSeekRange(seeker, seekCursor, fromInclusive, toExclusive, rightChild);
        }
    }

    @Test
    void mustFindRangeWhenCompletelyRebalancedToTheRightBeforeCallToNextBackwards() throws Exception {
        // given
        long key = 10;
        while (numberOfRootSplits == 0) {
            insert(key);
            key++;
        }

        // ... enough keys in left child to be rebalanced to the right
        for (long smallKey = 0; smallKey < 2; smallKey++) {
            insert(smallKey);
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate(rootId);
        readCursor.next();
        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        readCursor.next(pointer(leftChild));
        int keyCount = TreeNodeUtil.keyCount(readCursor);
        KEY from = layout.newKey();
        leaf.keyAt(readCursor, from, keyCount - 1, NULL_CONTEXT);
        long fromInclusive = getSeed(from);
        long toExclusive = fromInclusive - 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor(cursor.duplicate(rootId));
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> seeker = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            triggerUnderflowAndSeekRange(seeker, seekCursor, fromInclusive, toExclusive, rightChild);
        }
    }

    @Test
    void mustFindRangeWhenCompletelyRebalancedToTheRightAfterCallToNext() throws Exception {
        // given
        long key = 10;
        while (numberOfRootSplits == 0) {
            insert(key);
            key++;
        }

        // ... enough keys in left child to be rebalanced to the right
        for (long smallKey = 0; smallKey < 2; smallKey++) {
            insert(smallKey);
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate(rootId);
        readCursor.next();
        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        readCursor.next(pointer(leftChild));
        int keyCount = TreeNodeUtil.keyCount(readCursor);
        KEY from = layout.newKey();
        KEY to = layout.newKey();
        leaf.keyAt(readCursor, from, keyCount - 2, NULL_CONTEXT);
        leaf.keyAt(readCursor, to, keyCount - 1, NULL_CONTEXT);
        long fromInclusive = getSeed(from);
        long toExclusive = getSeed(to) + 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor(cursor.duplicate(rootId));
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> seeker = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            seekRangeWithUnderflowMidSeek(seeker, seekCursor, fromInclusive, toExclusive, rightChild);
        }
    }

    @Test
    void mustFindRangeWhenCompletelyRebalancedToTheRightAfterCallToNextBackwards() throws Exception {
        // given
        long key = 10;
        while (numberOfRootSplits == 0) {
            insert(key);
            key++;
        }

        // ... enough keys in left child to be rebalanced to the right
        for (long smallKey = 0; smallKey < 2; smallKey++) {
            insert(smallKey);
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate(rootId);
        readCursor.next();
        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);
        readCursor.next(pointer(leftChild));
        int keyCount = TreeNodeUtil.keyCount(readCursor);
        KEY from = layout.newKey();
        KEY to = layout.newKey();
        leaf.keyAt(readCursor, from, keyCount - 1, NULL_CONTEXT);
        leaf.keyAt(readCursor, to, keyCount - 2, NULL_CONTEXT);
        long fromInclusive = getSeed(from);
        long toExclusive = getSeed(to) - 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor(cursor.duplicate(rootId));
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> seeker = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            seekRangeWithUnderflowMidSeek(seeker, seekCursor, fromInclusive, toExclusive, rightChild);
        }
    }

    /* MERGE */

    @Test
    void mustFindRangeWhenMergingFromCurrentSeekNode() throws Exception {
        // given
        long key = 0;
        while (numberOfRootSplits == 0) {
            insert(key);
            key++;
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate(rootId);
        readCursor.next();
        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);

        // from first key in left child
        readCursor.next(pointer(leftChild));
        KEY from = layout.newKey();
        leaf.keyAt(readCursor, from, 0, NULL_CONTEXT);
        long fromInclusive = getSeed(from);
        long toExclusive = getSeed(from) + 2;

        // when
        TestPageCursor seekCursor = new TestPageCursor(cursor.duplicate(rootId));
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> seeker = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            assertThat(seekCursor.getCurrentPageId()).isEqualTo(leftChild);
            seekRangeWithUnderflowMidSeek(seeker, seekCursor, fromInclusive, toExclusive, rightChild);
            readCursor.next(rootId);
            assertTrue(TreeNodeUtil.isLeaf(readCursor));
        }
    }

    @Test
    void mustFindRangeWhenMergingToCurrentSeekNode() throws Exception {
        // given
        long key = 0;
        while (numberOfRootSplits == 0) {
            insert(key);
            key++;
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate(rootId);
        readCursor.next();
        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);

        // from first key in left child
        readCursor.next(pointer(rightChild));
        int keyCount = TreeNodeUtil.keyCount(readCursor);
        long fromInclusive = keyAt(readCursor, keyCount - 3);
        long toExclusive = keyAt(readCursor, keyCount - 1);

        // when
        TestPageCursor seekCursor = new TestPageCursor(cursor.duplicate(rootId));
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> seeker = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            assertThat(seekCursor.getCurrentPageId()).isEqualTo(rightChild);
            seekRangeWithUnderflowMidSeek(seeker, seekCursor, fromInclusive, toExclusive, leftChild);
            readCursor.next(rootId);
            assertTrue(TreeNodeUtil.isLeaf(readCursor));
        }
    }

    @Test
    void mustFindRangeWhenMergingToCurrentSeekNodeBackwards() throws Exception {
        // given
        long key = 0;
        while (numberOfRootSplits == 0) {
            insert(key);
            key++;
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate(rootId);
        readCursor.next();
        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);

        // from first key in left child
        readCursor.next(pointer(rightChild));
        int keyCount = TreeNodeUtil.keyCount(readCursor);
        long fromInclusive = keyAt(readCursor, keyCount - 1);
        long toExclusive = keyAt(readCursor, keyCount - 3);

        // when
        TestPageCursor seekCursor = new TestPageCursor(cursor.duplicate(rootId));
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> seeker = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            assertThat(seekCursor.getCurrentPageId()).isEqualTo(rightChild);
            seekRangeWithUnderflowMidSeek(seeker, seekCursor, fromInclusive, toExclusive, leftChild);
            readCursor.next(rootId);
            assertTrue(TreeNodeUtil.isLeaf(readCursor));
        }
    }

    @Test
    void mustFindRangeWhenMergingFromCurrentSeekNodeBackwards() throws Exception {
        // given
        long key = 0;
        while (numberOfRootSplits == 0) {
            insert(key);
            key++;
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate(rootId);
        readCursor.next();
        long leftChild = childAt(readCursor, 0, stableGeneration, unstableGeneration);
        long rightChild = childAt(readCursor, 1, stableGeneration, unstableGeneration);

        // from first key in left child
        readCursor.next(pointer(leftChild));
        KEY from = layout.newKey();
        leaf.keyAt(readCursor, from, 0, NULL_CONTEXT);
        long fromInclusive = getSeed(from) + 2;
        long toExclusive = getSeed(from);

        // when
        TestPageCursor seekCursor = new TestPageCursor(cursor.duplicate(rootId));
        seekCursor.next();
        try (SeekCursor<KEY, VALUE> seeker = seekCursor(fromInclusive, toExclusive, seekCursor)) {
            assertThat(seekCursor.getCurrentPageId()).isEqualTo(leftChild);
            seekRangeWithUnderflowMidSeek(seeker, seekCursor, fromInclusive, toExclusive, rightChild);
            readCursor.next(rootId);
            assertTrue(TreeNodeUtil.isLeaf(readCursor));
        }
    }

    /* POINTER GENERATION TESTING */

    @Test
    void shouldRereadSiblingIfReadFailureCausedByConcurrentCheckpoint() throws Exception {
        // given
        long i = 0L;
        while (numberOfRootSplits == 0) {
            insert(i);
            i++;
        }

        long currentNode = cursor.getCurrentPageId();
        try (SeekCursor<KEY, VALUE> seek = seekCursor(0L, i, cursor)) {
            // when right sibling gets an successor
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate(currentNode);
            duplicate.next();
            insert(i, i * 10, duplicate);

            // then
            // we should not fail to read right sibling
            //noinspection StatementWithEmptyBody
            while (seek.next()) {
                // ignore
            }
        }
    }

    @Test
    void shouldFailOnSiblingReadFailureIfNotCausedByConcurrentCheckpoint() throws Exception {
        // given
        long i = 0L;
        while (numberOfRootSplits == 0) {
            insert(i);
            i++;
        }

        long currentNode = cursor.getCurrentPageId();
        try (SeekCursor<KEY, VALUE> seek = seekCursor(0L, i, cursor)) {
            // when right sibling pointer is corrupt
            PageAwareByteArrayCursor duplicate = cursor.duplicate(currentNode);
            duplicate.next();
            long leftChild = childAt(duplicate, 0, stableGeneration, unstableGeneration);
            duplicate.next(leftChild);
            corruptGSPP(duplicate, TreeNodeUtil.BYTE_POS_RIGHTSIBLING);

            // even if we DO have a checkpoint
            checkpoint();

            // then
            // we should fail to read right sibling
            assertThrows(TreeInconsistencyException.class, () -> {
                while (seek.next()) {
                    // ignore
                }
            });
        }
    }

    @Test
    void shouldRereadSuccessorIfReadFailureCausedByCheckpointInLeaf() throws Exception {
        // given
        List<Long> expected = new ArrayList<>();
        List<Long> actual = new ArrayList<>();
        long i = 0L;
        for (; i < 2; i++) {
            insert(i);
            expected.add(i);
        }

        long currentNode = cursor.getCurrentPageId();
        try (SeekCursor<KEY, VALUE> seek = seekCursor(0L, 5, cursor)) {
            // when
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate(currentNode);
            duplicate.next();
            insert(i, i, duplicate); // Create successor of leaf
            expected.add(i);
            cursor.forceRetry();

            while (seek.next()) {
                actual.add(getSeed(seek.key()));
            }
        }

        // then
        assertEquals(expected, actual);
    }

    @Test
    void shouldFailSuccessorIfReadFailureNotCausedByCheckpointInLeaf() throws Exception {
        // given
        long i = 0L;
        for (; i < 2; i++) {
            insert(i);
        }

        long currentNode = cursor.getCurrentPageId();
        try (SeekCursor<KEY, VALUE> seek = seekCursor(0L, 5, cursor)) {
            // when
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate(currentNode);
            duplicate.next();
            insert(i, i, duplicate); // Create successor of leaf

            // and corrupt successor pointer
            corruptGSPP(duplicate, TreeNodeUtil.BYTE_POS_SUCCESSOR);
            cursor.forceRetry();

            // then
            assertThrows(TreeInconsistencyException.class, () -> {
                while (seek.next()) {
                    // ignore
                }
            });
        }
    }

    @Test
    void shouldRereadSuccessorIfReadFailureCausedByCheckpointInInternal() throws Exception {
        // given
        // a root with two leaves in old generation
        long i = 0L;
        while (numberOfRootSplits == 0) {
            insert(i);
            i++;
        }

        // a checkpoint
        long oldRootId = rootId;
        long oldStableGeneration = stableGeneration;
        long oldUnstableGeneration = unstableGeneration;
        checkpoint();
        int keyCount = TreeNodeUtil.keyCount(cursor);

        // and update root with an insert in new generation
        while (keyCount(rootId) == keyCount) {
            insert(i);
            i++;
        }
        TreeNodeUtil.goTo(cursor, "root", rootId);
        long rightChild = childAt(cursor, 2, stableGeneration, unstableGeneration);

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate(oldRootId);
        BreadcrumbPageCursor breadcrumbCursor = new BreadcrumbPageCursor(pageCursorForSeeker);
        breadcrumbCursor.next();
        try (SeekCursor<KEY, VALUE> seek =
                seekCursor(i, i + 1, breadcrumbCursor, oldStableGeneration, oldUnstableGeneration)) {
            //noinspection StatementWithEmptyBody
            while (seek.next()) {}
        }

        // then
        // make sure seek cursor went to successor of root node
        assertEquals(Arrays.asList(oldRootId, rootId, rightChild), breadcrumbCursor.getBreadcrumbs());
    }

    private int keyCount(long nodeId) throws IOException {
        long prevId = cursor.getCurrentPageId();
        try {
            TreeNodeUtil.goTo(cursor, "supplied", nodeId);
            return TreeNodeUtil.keyCount(cursor);
        } finally {
            TreeNodeUtil.goTo(cursor, "prev", prevId);
        }
    }

    @Test
    void shouldFailSuccessorIfReadFailureNotCausedByCheckpointInInternal() throws Exception {
        // given
        // a root with two leaves in old generation
        long i = 0L;
        while (numberOfRootSplits == 0) {
            insert(i);
            i++;
        }

        // a checkpoint
        long oldRootId = rootId;
        long oldStableGeneration = stableGeneration;
        long oldUnstableGeneration = unstableGeneration;
        checkpoint();
        int keyCount = TreeNodeUtil.keyCount(cursor);

        // and update root with an insert in new generation
        while (keyCount(rootId) == keyCount) {
            insert(i);
            i++;
        }

        // and corrupt successor pointer
        cursor.next(oldRootId);
        corruptGSPP(cursor, TreeNodeUtil.BYTE_POS_SUCCESSOR);

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate(oldRootId);
        pageCursorForSeeker.next();
        long position = i;
        assertThrows(
                TreeInconsistencyException.class,
                () -> seekCursor(
                        position, position + 1, pageCursorForSeeker, oldStableGeneration, oldUnstableGeneration));
    }

    @Test
    void shouldRereadChildPointerIfReadFailureCausedByCheckpoint() throws Exception {
        // given
        // a root with two leaves in old generation
        long i = 0L;
        while (numberOfRootSplits == 0) {
            insert(i);
            i++;
        }

        // a checkpoint
        long oldStableGeneration = stableGeneration;
        long oldUnstableGeneration = unstableGeneration;
        checkpoint();

        // and an update to root with a child pointer in new generation
        insert(i);
        i++;
        long newRightChild = childAt(cursor, 1, stableGeneration, unstableGeneration);

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate(rootId);
        BreadcrumbPageCursor breadcrumbCursor = new BreadcrumbPageCursor(pageCursorForSeeker);
        breadcrumbCursor.next();
        try (SeekCursor<KEY, VALUE> seek =
                seekCursor(i, i + 1, breadcrumbCursor, oldStableGeneration, oldUnstableGeneration)) {
            //noinspection StatementWithEmptyBody
            while (seek.next()) {}
        }

        // then
        // make sure seek cursor went to successor of root node
        assertEquals(Arrays.asList(rootId, newRightChild), breadcrumbCursor.getBreadcrumbs());
    }

    @Test
    void shouldFailChildPointerIfReadFailureNotCausedByCheckpoint() throws Exception {
        // given
        // a root with two leaves in old generation
        long i = 0L;
        while (numberOfRootSplits == 0) {
            insert(i);
            i++;
        }

        // a checkpoint
        long oldStableGeneration = stableGeneration;
        long oldUnstableGeneration = unstableGeneration;
        checkpoint();

        // and update root with an insert in new generation
        insert(i);
        i++;

        // and corrupt successor pointer
        corruptGSPP(cursor, internal.childOffset(1));

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate(rootId);
        pageCursorForSeeker.next();
        long position = i;
        assertThrows(
                TreeInconsistencyException.class,
                () -> seekCursor(
                        position, position + 1, pageCursorForSeeker, oldStableGeneration, oldUnstableGeneration));
    }

    @Test
    void shouldCatchupRootWhenRootNodeHasTooNewGeneration() throws Exception {
        // given
        long id = cursor.getCurrentPageId();
        long generation = TreeNodeUtil.generation(cursor);
        MutableBoolean triggered = new MutableBoolean(false);
        RootCatchup rootCatchup = (fromId, context) -> {
            triggered.setTrue();
            return new Root(id, generation);
        };

        // when
        //noinspection EmptyTryBlock
        try (SeekCursor<KEY, VALUE> ignored = new SeekCursor<>(
                        cursor, layout, leaf, internal, generationSupplier, exceptionDecorator, NULL_CONTEXT)
                .initialize(
                        rootInitializer(generation - 1),
                        rootCatchup,
                        key(0),
                        key(1),
                        1,
                        LEAF_LEVEL,
                        SeekCursor.NO_MONITOR)) {
            // do nothing
        }

        // then
        assertTrue(triggered.getValue());
    }

    @Test
    void shouldCatchupRootWhenNodeHasTooNewGenerationWhileTraversingDownTree() throws Exception {
        // given
        long generation = TreeNodeUtil.generation(cursor);
        MutableBoolean triggered = new MutableBoolean(false);
        long rightChild = 999; // We don't care

        // a newer leaf
        long leftChild = cursor.getCurrentPageId();
        // A newer leaf
        leaf.initialize(cursor, DATA_LAYER_FLAG, stableGeneration + 1, unstableGeneration + 1);
        cursor.next();

        // a root
        long rootId = cursor.getCurrentPageId();
        internal.initialize(cursor, DATA_LAYER_FLAG, stableGeneration, unstableGeneration);
        long keyInRoot = 10L;
        KEY key = key(keyInRoot);
        internal.insertKeyAndRightChildAt(
                cursor, key, rightChild, 0, 0, stableGeneration, unstableGeneration, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, 1);
        // with old pointer to child (simulating reuse of child node)
        internal.setChildAt(cursor, leftChild, 0, stableGeneration, unstableGeneration);

        // a root catchup that records usage
        RootCatchup rootCatchup = (fromId, context) -> {
            triggered.setTrue();

            // and set child generation to match pointer
            cursor.next(leftChild);
            cursor.zapPage();
            leaf.initialize(cursor, DATA_LAYER_FLAG, stableGeneration, unstableGeneration);

            cursor.next(rootId);
            return new Root(rootId, generation);
        };

        // when
        KEY from = key(1L);
        KEY to = key(2L);
        //noinspection EmptyTryBlock
        try (SeekCursor<KEY, VALUE> ignored = new SeekCursor<>(
                        cursor, layout, leaf, internal, generationSupplier, exceptionDecorator, NULL_CONTEXT)
                .initialize(
                        rootInitializer(unstableGeneration),
                        rootCatchup,
                        from,
                        to,
                        1,
                        LEAF_LEVEL,
                        SeekCursor.NO_MONITOR)) {
            // do nothing
        }

        // then
        assertTrue(triggered.getValue());
    }

    @Test
    void shouldCatchupRootWhenNodeHasTooNewGenerationWhileTraversingLeaves() throws Exception {
        // given
        MutableBoolean triggered = new MutableBoolean(false);
        long oldRightChild = 666; // We don't care

        // a newer right leaf
        long rightChild = cursor.getCurrentPageId();
        leaf.initialize(cursor, DATA_LAYER_FLAG, stableGeneration, unstableGeneration);
        cursor.next();

        RootCatchup rootCatchup = (fromId, context) -> {
            // Use right child as new start over root to terminate test
            cursor.next(rightChild);
            triggered.setTrue();
            return new Root(cursor.getCurrentPageId(), TreeNodeUtil.generation(cursor));
        };

        // a left leaf
        long leftChild = cursor.getCurrentPageId();
        leaf.initialize(cursor, DATA_LAYER_FLAG, stableGeneration - 1, unstableGeneration - 1);
        // with an old pointer to right sibling
        TreeNodeUtil.setRightSibling(cursor, rightChild, stableGeneration - 1, unstableGeneration - 1);
        cursor.next();

        // a root
        internal.initialize(cursor, DATA_LAYER_FLAG, stableGeneration - 1, unstableGeneration - 1);
        long keyInRoot = 10L;
        KEY key = key(keyInRoot);
        internal.insertKeyAndRightChildAt(
                cursor, key, oldRightChild, 0, 0, stableGeneration, unstableGeneration, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, 1);
        // with old pointer to child (simulating reuse of internal node)
        internal.setChildAt(cursor, leftChild, 0, stableGeneration, unstableGeneration);

        // when
        KEY from = key(1L);
        KEY to = key(20L);
        LongSupplier firstOlderThenCurrentGenerationSupplier =
                firstCustomThenCurrentGenerationSupplier(stableGeneration - 1, unstableGeneration - 1);
        try (SeekCursor<KEY, VALUE> seek = new SeekCursor<>(
                        cursor,
                        layout,
                        leaf,
                        internal,
                        firstOlderThenCurrentGenerationSupplier,
                        exceptionDecorator,
                        NULL_CONTEXT)
                .initialize(
                        rootInitializer(unstableGeneration),
                        rootCatchup,
                        from,
                        to,
                        1,
                        LEAF_LEVEL,
                        SeekCursor.NO_MONITOR)) {
            while (seek.next()) {
                seek.key();
            }
        }

        // then
        assertTrue(triggered.getValue());
    }

    private LongSupplier firstCustomThenCurrentGenerationSupplier(
            long firstStableGeneration, long firstUnstableGeneration) {
        return new LongSupplier() {
            private boolean first = true;

            @Override
            public long getAsLong() {
                long generation = first
                        ? Generation.generation(firstStableGeneration, firstUnstableGeneration)
                        : generationSupplier.getAsLong();
                first = false;
                return generation;
            }
        };
    }

    @Test
    void shouldThrowTreeInconsistencyExceptionOnBadReadWithoutShouldRetryWhileTraversingTree() throws Exception {
        // GIVEN
        int keyCount = 10000;

        // WHEN
        cursor.setOffset(TreeNodeUtil.BYTE_POS_KEYCOUNT);
        cursor.putInt(keyCount); // Bad key count

        // THEN
        //noinspection EmptyTryBlock
        try (SeekCursor<KEY, VALUE> ignored = seekCursor(0L, Long.MAX_VALUE)) {
            // Do nothing
        } catch (TreeInconsistencyException e) {
            assertThat(e.getMessage()).contains("keyCount:" + keyCount);
        }
    }

    @Test
    void shouldThrowTreeInconsistencyExceptionOnBadReadWithoutShouldRetryWhileTraversingLeaves() throws Exception {
        // GIVEN
        // a root with two leaves in old generation
        int keyCount = 10000;
        long i = 0L;
        while (numberOfRootSplits == 0) {
            insert(i);
            i++;
        }
        long rootId = cursor.getCurrentPageId();
        long leftChild = internal.childAt(cursor, 0, stableGeneration, unstableGeneration);

        // WHEN
        goTo(cursor, leftChild);
        cursor.setOffset(TreeNodeUtil.BYTE_POS_KEYCOUNT);
        cursor.putInt(keyCount); // Bad key count
        goTo(cursor, rootId);

        // THEN
        try (SeekCursor<KEY, VALUE> seek = seekCursor(0L, Long.MAX_VALUE)) {
            //noinspection StatementWithEmptyBody
            while (seek.next()) {
                // Do nothing
            }
        } catch (TreeInconsistencyException e) {
            assertThat(e.getMessage()).contains("keyCount:" + keyCount);
        }
    }

    /* READ LEVEL */

    @Test
    void shouldReadWholeRangeOnLevel() throws IOException {
        // GIVEN
        long i = 0L;
        while (numberOfRootSplits < 2) {
            insert(i);
            i++;
        }

        for (int level = 0; level <= 2; level++) {
            // WHEN
            List<Long> readBySeeker = new ArrayList<>();
            goTo(cursor, rootId);
            try (SeekCursor<KEY, VALUE> seek = seekCursorOnLevel(level, 0, i)) {
                while (seek.next()) {
                    readBySeeker.add(layout.keySeed(seek.key()));
                }
            }

            // THEN
            List<Long> expected = allKeysOnLevel(level, 0, i);
            assertThat(readBySeeker).as("seek at level " + level).isEqualTo(expected);
        }
    }

    @Test
    void shouldReadSubRangeOnLevel() throws IOException {
        // GIVEN
        long i = 0L;
        int nbrOfLevels = random.nextInt(2, 4);
        while (numberOfRootSplits < nbrOfLevels - 1) {
            insert(i);
            i++;
        }

        for (int level = 0; level < nbrOfLevels; level++) {
            // WHEN
            long fromInclusive = random.nextLong(i - 1);
            long toExclusive = random.nextLong(fromInclusive, i);
            List<Long> readBySeeker = new ArrayList<>();
            goTo(cursor, rootId);

            try (SeekCursor<KEY, VALUE> seek = seekCursorOnLevel(level, fromInclusive, toExclusive)) {
                while (seek.next()) {
                    readBySeeker.add(layout.keySeed(seek.key()));
                }
            }

            // THEN
            List<Long> expected = allKeysOnLevel(level, fromInclusive, toExclusive);
            assertThat(readBySeeker).isEqualTo(expected);
        }
    }

    @Test
    void avoidDoubleCloseOfUnderlyingCursor() throws IOException {
        try (SeekCursor<KEY, VALUE> cursor = seekCursor(0, Long.MAX_VALUE)) {
            while (cursor.next()) {
                // empty
            }
        }
        assertEquals(1, cursor.getCloseCount());
    }

    private List<Long> allKeysOnLevel(int level, long fromInclusive, long toExclusive) throws IOException {
        List<Long> allKeysOnLevel = new ArrayList<>();
        long prevPageId = cursor.getCurrentPageId();
        try {
            goToLeftmostOnLevel(cursor, level);
            boolean hasRightSibling;
            do {
                List<Long> allKeysInNode = allKeysInNode(cursor, fromInclusive, toExclusive);
                allKeysOnLevel.addAll(allKeysInNode);
                hasRightSibling = goToRightSibling(cursor);
            } while (hasRightSibling);
            return allKeysOnLevel;
        } finally {
            goTo(cursor, prevPageId);
        }
    }

    private void goToLeftmostOnLevel(PageCursor cursor, int level) throws IOException {
        goTo(cursor, rootId);
        int currentLevel = 0;
        while (currentLevel < level && TreeNodeUtil.isInternal(cursor)) {
            long child = childAt(cursor, 0, stableGeneration, unstableGeneration);
            goTo(cursor, child);
            currentLevel++;
        }
        if (currentLevel < level) {
            throw new RuntimeException(
                    "Could not traverse down to level " + level + " because last level is " + currentLevel);
        }
    }

    private List<Long> allKeysInNode(PageCursor cursor, long fromInclusive, long toExclusive) {
        // If we are currently in an internal node it's not enough to compare the seed for
        // the keys that we find here. We need to compare the actual keys with the keys that
        // fromInclusive and toExclusive would generate if used as seeds. This is because
        // the keys in the internal nodes might have been stripped down due to 'minimalSplitter'
        // and will this not match the deterministic key generation from seeds.
        KEY fromInclusiveKey = layout.key(fromInclusive);
        KEY toExclusiveKey = layout.key(toExclusive);
        var isInternal = TreeNodeUtil.isInternal(cursor);
        List<Long> allKeysOnNode = new ArrayList<>();
        int keyCount = TreeNodeUtil.keyCount(cursor);
        boolean exactMatch = fromInclusive == toExclusive;
        for (int pos = 0; pos < keyCount; pos++) {
            KEY key = layout.newKey();
            if (isInternal) {
                internal.keyAt(cursor, key, pos, NULL_CONTEXT);
            } else {
                leaf.keyAt(cursor, key, pos, NULL_CONTEXT);
            }
            if (layout.compare(fromInclusiveKey, key) <= 0 && layout.compare(key, toExclusiveKey) < 0
                    || exactMatch && layout.compare(key, fromInclusiveKey) == 0) {
                allKeysOnNode.add(layout.keySeed(key));
            }
        }
        return allKeysOnNode;
    }

    private static boolean goToRightSibling(PageCursor cursor) throws IOException {
        long rightSibling = pointer(TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration));
        boolean hasRightSibling = rightSibling != TreeNodeUtil.NO_NODE_FLAG;
        if (hasRightSibling) {
            goTo(cursor, rightSibling);
        }
        return hasRightSibling;
    }

    private void triggerUnderflowAndSeekRange(
            SeekCursor<KEY, VALUE> seeker,
            TestPageCursor seekCursor,
            long fromInclusive,
            long toExclusive,
            long rightChild)
            throws IOException {
        // ... then seeker should still find range
        int stride = fromInclusive <= toExclusive ? 1 : -1;
        triggerUnderflowAndSeekRange(seeker, seekCursor, fromInclusive, toExclusive, rightChild, stride);
    }

    private void seekRangeWithUnderflowMidSeek(
            SeekCursor<KEY, VALUE> seeker,
            TestPageCursor seekCursor,
            long fromInclusive,
            long toExclusive,
            long underflowNode)
            throws IOException {
        // ... seeker has started seeking in range
        assertTrue(seeker.next());
        assertThat(getSeed(seeker.key())).isEqualTo(fromInclusive);

        int stride = fromInclusive <= toExclusive ? 1 : -1;
        triggerUnderflowAndSeekRange(seeker, seekCursor, fromInclusive + stride, toExclusive, underflowNode, stride);
    }

    private void triggerUnderflowAndSeekRange(
            SeekCursor<KEY, VALUE> seeker,
            TestPageCursor seekCursor,
            long fromInclusive,
            long toExclusive,
            long rightChild,
            int stride)
            throws IOException {
        // ... rebalance happens before first call to next
        triggerUnderflow(rightChild);
        seekCursor.changed(); // ByteArrayPageCursor is not aware of should retry, so fake it here

        for (long expected = fromInclusive; Long.compare(expected, toExclusive) * stride < 0; expected += stride) {
            assertTrue(seeker.next());
            assertThat(getSeed(seeker.key())).isEqualTo(expected);
        }
        assertFalse(seeker.next());
    }

    private void triggerUnderflow(long nodeId) throws IOException {
        // On underflow keys will move from left to right
        // and key count of the right will increase.
        // We don't know if keys will move from nodeId to
        // right sibling or to nodeId from left sibling.
        // So we monitor both nodeId and rightSibling.
        PageCursor readCursor = cursor.duplicate(nodeId);
        readCursor.next();
        int midKeyCount = TreeNodeUtil.keyCount(readCursor);
        int prevKeyCount = midKeyCount + 1;

        PageCursor rightSiblingCursor = null;
        long rightSibling = TreeNodeUtil.rightSibling(readCursor, stableGeneration, unstableGeneration);
        int rightKeyCount = 0;
        int prevRightKeyCount = 1;
        boolean monitorRight = TreeNodeUtil.isNode(rightSibling);
        if (monitorRight) {
            rightSiblingCursor = cursor.duplicate(GenerationSafePointerPair.pointer(rightSibling));
            rightSiblingCursor.next();
            rightKeyCount = TreeNodeUtil.keyCount(rightSiblingCursor);
            prevRightKeyCount = rightKeyCount + 1;
        }

        while (midKeyCount < prevKeyCount && rightKeyCount <= prevRightKeyCount) {
            long toRemove = keyAt(readCursor, 0);
            remove(toRemove);
            prevKeyCount = midKeyCount;
            midKeyCount = TreeNodeUtil.keyCount(readCursor);
            if (monitorRight) {
                prevRightKeyCount = rightKeyCount;
                rightKeyCount = TreeNodeUtil.keyCount(rightSiblingCursor);
            }
        }
    }

    private static void checkpoint() {
        stableGeneration = unstableGeneration;
        unstableGeneration++;
    }

    private void newRootFromSplit(StructurePropagation<KEY> split) throws IOException {
        assertTrue(split.hasRightKeyInsert);
        long rootId = id.acquireNewId(stableGeneration, unstableGeneration, CursorCreator.bind(cursor));
        cursor.next(rootId);
        internal.initialize(cursor, DATA_LAYER_FLAG, stableGeneration, unstableGeneration);
        internal.setChildAt(cursor, split.midChild, 0, stableGeneration, unstableGeneration);
        internal.insertKeyAndRightChildAt(
                cursor, split.rightKey, split.rightChild, 0, 0, stableGeneration, unstableGeneration, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, 1);
        split.hasRightKeyInsert = false;
        numberOfRootSplits++;
        updateRoot();
    }

    private static void corruptGSPP(PageAwareByteArrayCursor duplicate, int offset) {
        int someBytes = duplicate.getInt(offset);
        duplicate.putInt(offset, ~someBytes);
        someBytes = duplicate.getInt(offset + GenerationSafePointer.SIZE);
        duplicate.putInt(offset + GenerationSafePointer.SIZE, ~someBytes);
    }

    private void insert(long key) throws IOException {
        insert(key, key);
    }

    private void insert(long key, long value) throws IOException {
        insert(key, value, cursor);
    }

    private void insert(long key, long value, PageCursor cursor) throws IOException {
        treeLogic.insert(
                cursor,
                structurePropagation,
                key(key),
                value(value),
                overwrite(),
                true,
                stableGeneration,
                unstableGeneration,
                NULL_CONTEXT);
        handleAfterChange();
    }

    private void remove(long key) throws IOException {
        treeLogic.remove(
                cursor,
                structurePropagation,
                key(key),
                new ValueHolder<>(layout.newValue()),
                stableGeneration,
                unstableGeneration,
                NULL_CONTEXT);
        handleAfterChange();
    }

    private void handleAfterChange() throws IOException {
        if (structurePropagation.hasRightKeyInsert) {
            newRootFromSplit(structurePropagation);
        }
        if (structurePropagation.hasMidChildUpdate) {
            structurePropagation.hasMidChildUpdate = false;
            updateRoot();
        }
    }

    private SeekCursor<KEY, VALUE> seekCursorOnLevel(int level, long fromInclusive, long toExclusive)
            throws IOException {
        return new SeekCursor<>(cursor, layout, leaf, internal, generationSupplier, exceptionDecorator, NULL_CONTEXT)
                .initialize(
                        rootInitializer(unstableGeneration),
                        failingRootCatchup,
                        key(fromInclusive),
                        key(toExclusive),
                        random.nextInt(1, DEFAULT_MAX_READ_AHEAD),
                        level,
                        SeekCursor.NO_MONITOR);
    }

    private SeekCursor<KEY, VALUE> seekCursor(long fromInclusive, long toExclusive) throws IOException {
        return seekCursor(fromInclusive, toExclusive, cursor);
    }

    private SeekCursor<KEY, VALUE> seekCursor(long fromInclusive, long toExclusive, PageCursor pageCursor)
            throws IOException {
        return seekCursor(fromInclusive, toExclusive, pageCursor, stableGeneration, unstableGeneration);
    }

    private SeekCursor<KEY, VALUE> seekCursor(
            long fromInclusive, long toExclusive, PageCursor pageCursor, long stableGeneration, long unstableGeneration)
            throws IOException {
        return seekCursor(
                fromInclusive, toExclusive, pageCursor, stableGeneration, unstableGeneration, failingRootCatchup);
    }

    private SeekCursor<KEY, VALUE> seekCursor(
            long fromInclusive,
            long toExclusive,
            PageCursor pageCursor,
            long stableGeneration,
            long unstableGeneration,
            RootCatchup rootCatchup)
            throws IOException {
        LongSupplier generationSupplier =
                firstCustomThenCurrentGenerationSupplier(stableGeneration, unstableGeneration);
        return new SeekCursor<>(
                        pageCursor, layout, leaf, internal, generationSupplier, exceptionDecorator, NULL_CONTEXT)
                .initialize(
                        rootInitializer(unstableGeneration),
                        rootCatchup,
                        key(fromInclusive),
                        key(toExclusive),
                        random.nextInt(1, DEFAULT_MAX_READ_AHEAD),
                        LEAF_LEVEL,
                        SeekCursor.NO_MONITOR);
    }

    /**
     * Create a right sibling to node pointed to by cursor. Leave cursor on new right sibling when done,
     * and return id of left sibling.
     */
    private long createRightSibling(PageCursor pageCursor) throws IOException {
        long left = pageCursor.getCurrentPageId();
        long right = left + 1;

        TreeNodeUtil.setRightSibling(pageCursor, right, stableGeneration, unstableGeneration);

        pageCursor.next(right);
        leaf.initialize(pageCursor, DATA_LAYER_FLAG, stableGeneration, unstableGeneration);
        TreeNodeUtil.setLeftSibling(pageCursor, left, stableGeneration, unstableGeneration);
        return left;
    }

    private void assertRangeInSingleLeaf(long fromInclusive, long toExclusive, SeekCursor<KEY, VALUE> cursor)
            throws IOException {
        int stride = fromInclusive <= toExclusive ? 1 : -1;
        long expected = fromInclusive;
        while (cursor.next()) {
            KEY key = key(expected);
            VALUE value = value(expected);
            assertKeyAndValue(cursor, key, value);
            expected += stride;
        }
        assertEquals(toExclusive, expected);
    }

    private void assertKeyAndValue(SeekCursor<KEY, VALUE> cursor, long expectedKeySeed) {
        KEY key = key(expectedKeySeed);
        VALUE value = value(expectedKeySeed);
        assertKeyAndValue(cursor, key, value);
    }

    private void assertKeyAndValue(SeekCursor<KEY, VALUE> cursor, KEY expectedKey, VALUE expectedValue) {
        KEY foundKey = cursor.key();
        VALUE foundValue = cursor.value();
        assertEqualsKey(expectedKey, foundKey);
        assertEqualsValue(expectedValue, foundValue);
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

    private void insertKeysAndValues(int keyCount) throws IOException {
        for (int i = 0; i < keyCount; i++) {
            append(i);
        }
    }

    private void append(long k) throws IOException {
        int keyCount = TreeNodeUtil.keyCount(cursor);
        KEY key = key(k);
        VALUE value = value(k);
        leaf.insertKeyValueAt(
                cursor, key, value, keyCount, keyCount, stableGeneration, unstableGeneration, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, keyCount + 1);
    }

    private void insertIn(int pos, long k) throws IOException {
        int keyCount = TreeNodeUtil.keyCount(cursor);
        KEY key = key(k);
        VALUE value = value(k);
        Overflow overflow = leaf.overflow(cursor, keyCount, key, value);
        if (overflow != Overflow.NO) {
            throw new IllegalStateException("Can not insert another key in current node");
        }
        leaf.insertKeyValueAt(cursor, key, value, pos, keyCount, stableGeneration, unstableGeneration, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, keyCount + 1);
    }

    private void removeAtPos(int pos) throws IOException {
        int keyCount = TreeNodeUtil.keyCount(cursor);
        leaf.removeKeyValueAt(cursor, pos, keyCount, stableGeneration, unstableGeneration, NULL_CONTEXT);
        TreeNodeUtil.setKeyCount(cursor, keyCount - 1);
    }

    private static class BreadcrumbPageCursor extends DelegatingPageCursor {
        private final List<Long> breadcrumbs = new ArrayList<>();

        BreadcrumbPageCursor(PageCursor delegate) {
            super(delegate);
        }

        @Override
        public boolean next() throws IOException {
            boolean next = super.next();
            breadcrumbs.add(getCurrentPageId());
            return next;
        }

        @Override
        public boolean next(long pageId) throws IOException {
            boolean next = super.next(pageId);
            breadcrumbs.add(getCurrentPageId());
            return next;
        }

        List<Long> getBreadcrumbs() {
            return breadcrumbs;
        }
    }

    private long childAt(PageCursor cursor, int pos, long stableGeneration, long unstableGeneration) {
        return pointer(internal.childAt(cursor, pos, stableGeneration, unstableGeneration));
    }

    private long keyAt(PageCursor cursor, int pos) {
        KEY readKey = layout.newKey();
        leaf.keyAt(cursor, readKey, pos, NULL_CONTEXT);
        return getSeed(readKey);
    }

    // KEEP even if unused
    @SuppressWarnings("unused")
    private void printTree() throws IOException {
        long currentPageId = cursor.getCurrentPageId();
        cursor.next(rootId);
        new GBPTreeStructure<>(null, null, null, layout, leaf, internal, stableGeneration, unstableGeneration)
                .visitTree(cursor, new PrintingGBPTreeVisitor<>(PrintConfig.defaults()), NULL_CONTEXT);
        cursor.next(currentPageId);
    }

    private RootInitializer rootInitializer(long generation) {
        return (cursor, context) -> generation;
    }
}
