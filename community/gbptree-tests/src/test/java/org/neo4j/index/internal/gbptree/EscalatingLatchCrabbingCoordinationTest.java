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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.LatchCrabbingCoordination.DEFAULT_RESET_FREQUENCY;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.PageCursor;

class EscalatingLatchCrabbingCoordinationTest {
    private static final int MERGE_THRESHOLD = 100;

    private final TreeNodeLatchService latchService = mock(TreeNodeLatchService.class);
    private final PageCursor cursor = mock(PageCursor.class);
    private final EscalatingLatchCrabbingCoordination.EntrySizeLookup entrySizeLookup =
            mock(EscalatingLatchCrabbingCoordination.EntrySizeLookup.class);
    private final EscalatingLatchCrabbingCoordination coordination = new EscalatingLatchCrabbingCoordination(
            latchService, entrySizeLookup, MERGE_THRESHOLD, DEFAULT_RESET_FREQUENCY, MultiRootGBPTree.NO_MONITOR);

    @BeforeEach
    void setUp() {
        when(latchService.latch(anyLong())).thenAnswer(invocationOnMock -> {
            LongSpinLatch latch = mock(LongSpinLatch.class);
            when(latch.treeNodeId()).thenReturn(invocationOnMock.getArgument(0, Long.class));
            return latch;
        });
        coordination.initialize(cursor);
        coordination.beginOperation();
    }

    @Test
    void shouldOptimisticallyUpgradeParentOnLeafSplitWhenParentFits() {
        var parentLatch = latch(1L, true, true);
        var leafLatch = latch(2L, true, true);
        descendToLeaf(50, 10);

        assertTrue(coordination.beforeSplittingLeaf(10));

        verify(leafLatch).tryUpgradeToWrite();
        verify(parentLatch).tryUpgradeToWrite();
        verifyNoInteractions(entrySizeLookup);
        assertFalse(coordination.checkForceReset());
    }

    @Test
    void shouldEscalateToGrandparentWhenParentIsFull() {
        var grandparentLatch = latch(1L, true, true);
        var parentLatch = latch(2L, true, true);
        var leafLatch = latch(3L, true, true);
        scannableNode(2L, 8, 20, 6);

        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 50, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 3));
        coordination.beforeTraversingToChild(3L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        assertTrue(coordination.beforeSplittingLeaf(10));

        verify(leafLatch).tryUpgradeToWrite();
        verify(parentLatch).tryUpgradeToWrite();
        verify(grandparentLatch).tryUpgradeToWrite();
        assertTrue(coordination.checkForceReset());
    }

    @Test
    void shouldAllowPreAuthorizedInternalSplitDuringCascade() {
        givenSuccessfulEscalationToGrandparent();

        coordination.up();
        coordination.beforeSplitInternal(2L);

        coordination.up();
        assertThrows(IllegalStateException.class, () -> coordination.beforeSplitInternal(1L));
    }

    @Test
    void shouldClearEscalationExactlyAtNonRootAbsorbingLevel() {
        var rootLatch = latch(1L, true, true);
        var grandparentLatch = latch(2L, true, true);
        var parentLatch = latch(3L, true, true);
        var leafLatch = latch(4L, true, true);
        scannableNode(3L, 15, 15, 15);

        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 100, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(true, 50, false, 4));
        coordination.beforeTraversingToChild(3L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 3));
        coordination.beforeTraversingToChild(4L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        assertTrue(coordination.beforeSplittingLeaf(10));

        verify(leafLatch).tryUpgradeToWrite();
        verify(parentLatch).tryUpgradeToWrite();
        verify(grandparentLatch).tryUpgradeToWrite();
        verify(rootLatch, never()).tryUpgradeToWrite();

        coordination.up();
        coordination.beforeSplitInternal(3L);

        coordination.up();
        assertThrows(IllegalStateException.class, () -> coordination.beforeSplitInternal(2L));
    }

    @Test
    void shouldEscalateThreeLevels() {
        var greatGrandparentLatch = latch(1L, true, true);
        var grandparentLatch = latch(2L, true, true);
        var parentLatch = latch(3L, true, true);
        var leafLatch = latch(4L, true, true);
        scannableNode(3L, 15, 15, 15);
        scannableNode(2L, 25, 25, 25, 25);

        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 100, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 4));
        coordination.beforeTraversingToChild(3L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 3));
        coordination.beforeTraversingToChild(4L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        assertTrue(coordination.beforeSplittingLeaf(10));

        verify(leafLatch).tryUpgradeToWrite();
        verify(parentLatch).tryUpgradeToWrite();
        verify(grandparentLatch).tryUpgradeToWrite();
        verify(greatGrandparentLatch).tryUpgradeToWrite();
    }

    @Test
    void shouldFailEscalationWhenAncestorNeedsSuccessor() {
        var grandparentLatch = latch(1L, true, true);
        var parentLatch = latch(2L, true, true);
        latch(3L, true, true);
        scannableNode(2L, 15, 15, 15);

        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 50, true, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 3));
        coordination.beforeTraversingToChild(3L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        assertFalse(coordination.beforeSplittingLeaf(10));
        verify(parentLatch, never()).tryUpgradeToWrite();
        verify(grandparentLatch, never()).tryUpgradeToWrite();
    }

    @Test
    void shouldFailEscalationWithoutScanningWhenAncestorIsContended() {
        latch(1L, true, true);
        latch(2L, true, false);
        latch(3L, true, true);

        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 50, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 3));
        coordination.beforeTraversingToChild(3L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        assertFalse(coordination.beforeSplittingLeaf(10));
        verifyNoInteractions(entrySizeLookup);
    }

    @Test
    void shouldFailEscalationWhenCascadeWouldSplitRoot() {
        latch(1L, true, true);
        latch(2L, true, true);
        latch(3L, true, true);
        scannableNode(2L, 15, 15, 15);

        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 3));
        coordination.beforeTraversingToChild(3L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        assertFalse(coordination.beforeSplittingLeaf(10));
    }

    @Test
    void shouldFailEscalationWhenSplittingLevelIsRightmostChild() {
        latch(1L, true, true);
        latch(2L, true, true);
        latch(3L, true, true);

        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 50, false, 5));
        coordination.beforeTraversingToChild(2L, 5);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 3));
        coordination.beforeTraversingToChild(3L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        assertFalse(coordination.beforeSplittingLeaf(10));
        verifyNoInteractions(entrySizeLookup);
    }

    @Test
    void shouldFailEscalationOnUpgradeRace() {
        var grandparentLatch = latch(1L, false, true);
        var parentLatch = latch(2L, true, true);
        latch(3L, true, true);
        scannableNode(2L, 15, 15, 15);

        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 50, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 3));
        coordination.beforeTraversingToChild(3L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        assertFalse(coordination.beforeSplittingLeaf(10));
        verify(grandparentLatch).tryUpgradeToWrite();
        verify(parentLatch, never()).tryUpgradeToWrite();
    }

    @Test
    void shouldFailLeafSplitWhenLeafIsRoot() {
        latch(1L, true, true);
        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        assertFalse(coordination.beforeSplittingLeaf(10));
    }

    @Test
    void shouldThrowOnInternalSplitWithoutAuthorization() {
        latch(1L, true, true);
        latch(2L, true, true);
        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 50, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        assertThrows(IllegalStateException.class, () -> coordination.beforeSplitInternal(1L));
    }

    @Test
    void shouldAllowInternalSplitAndLeafUnderflowInPessimisticMode() {
        coordination.flipToPessimisticMode();
        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD - 5, false, 5));

        assertTrue(coordination.beforeSplittingLeaf(1000));
        coordination.beforeSplitInternal(1L);
        coordination.beforeUnderflowInLeaf(2L);
        assertTrue(coordination.beforeRemovalFromLeaf(10));
        assertTrue(coordination.beforeAccessingRightSiblingLeaf(7L));
        assertTrue(coordination.checkForceReset());
    }

    @Test
    void shouldKeepDeepEscalationResetStickyAcrossEscalationsInSameOperation() {
        givenSuccessfulEscalationToGrandparent();
        coordination.up();
        coordination.up();

        latch(4L, true, true);
        latch(5L, true, true);
        coordination.beforeTraversingToChild(4L, 2);
        assertTrue(coordination.arrivedAtChild(true, 50, false, 5));
        coordination.beforeTraversingToChild(5L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));
        assertTrue(coordination.beforeSplittingLeaf(10));

        assertTrue(coordination.checkForceReset());
    }

    @Test
    void shouldClearEscalationStateOnBeginOperation() {
        givenSuccessfulEscalationToGrandparent();

        coordination.beginOperation();

        assertFalse(coordination.checkForceReset());
        coordination.up();
        assertThrows(IllegalStateException.class, () -> coordination.beforeSplitInternal(2L));
    }

    @Test
    void shouldFailArriveAtLeafNeedingSuccessorAtRightEdgeOfParent() {
        var parentLatch = latch(1L, true, true);
        latch(2L, true, true);
        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 50, false, 5));
        coordination.beforeTraversingToChild(2L, 5);

        assertFalse(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, true, 5));
        verify(parentLatch, never()).tryUpgradeToWrite();
    }

    @Test
    void shouldAllowArriveAtLeafNeedingSuccessorWhenChildPosOnlyMatchesOwnKeyCount() {
        var parentLatch = latch(1L, true, true);
        latch(2L, true, true);
        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 50, false, 10));
        coordination.beforeTraversingToChild(2L, 5);

        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, true, 5));
        verify(parentLatch).tryUpgradeToWrite();
    }

    @Test
    void shouldFailArriveAtLeafNeedingSuccessorAtLeftEdge() {
        var parentLatch = latch(1L, true, true);
        latch(2L, true, true);
        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 50, false, 5));
        coordination.beforeTraversingToChild(2L, 0);

        assertFalse(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, true, 5));
        verify(parentLatch, never()).tryUpgradeToWrite();
    }

    @Test
    void shouldFailRemovalIfLeafWillUnderflow() {
        latch(1L, true, true);
        latch(2L, true, true);
        descendToLeaf(50, MERGE_THRESHOLD - 5);

        assertFalse(coordination.beforeRemovalFromLeaf(10));
        assertTrue(coordination.beforeRemovalFromLeaf(2));
    }

    @Test
    void shouldFailAccessingRightSiblingLeafOptimistically() {
        assertFalse(coordination.beforeAccessingRightSiblingLeaf(7L));
    }

    @Test
    void shouldReleaseEscalatedLatchesWithCorrectTypesOnReset() {
        var rootLatch = latch(1L, true, true);
        var grandparentLatch = latch(2L, true, true);
        var parentLatch = latch(3L, false, true);
        var leafLatch = latch(4L, true, true);
        scannableNode(3L, 15, 15, 15);

        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 100, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(true, 100, false, 4));
        coordination.beforeTraversingToChild(3L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 3));
        coordination.beforeTraversingToChild(4L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));
        assertFalse(coordination.beforeSplittingLeaf(10));

        coordination.flipToPessimisticMode();

        verify(leafLatch).releaseWrite();
        verify(grandparentLatch).releaseWrite();
        verify(parentLatch).releaseRead();
        verify(rootLatch).releaseRead();
    }

    private void givenSuccessfulEscalationToGrandparent() {
        latch(1L, true, true);
        latch(2L, true, true);
        latch(3L, true, true);
        scannableNode(2L, 15, 15, 15);

        coordination.beforeTraversingToChild(1L, 0);
        assertTrue(coordination.arrivedAtChild(true, 50, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 3));
        coordination.beforeTraversingToChild(3L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));
        assertTrue(coordination.beforeSplittingLeaf(10));
    }

    private void descendToLeaf(int parentAvailableSpace, int leafAvailableSpace) {
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, parentAvailableSpace, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(false, leafAvailableSpace, false, 5));
    }

    private LongSpinLatch latch(long treeNodeId, boolean upgradeSucceeds, boolean peekSaysSoleReader) {
        LongSpinLatch latch = mock(LongSpinLatch.class);
        when(latch.treeNodeId()).thenReturn(treeNodeId);
        when(latch.tryUpgradeToWrite()).thenReturn(upgradeSucceeds);
        when(latch.couldUpgradeToWrite()).thenReturn(peekSaysSoleReader);
        when(latchService.latch(treeNodeId)).thenReturn(latch);
        return latch;
    }

    private void scannableNode(long treeNodeId, int... entrySizes) {
        int max = 0;
        for (int size : entrySizes) {
            max = Math.max(max, size);
        }
        try {
            when(entrySizeLookup.maxEntrySizeBound(any(CursorCreator.class), eq(treeNodeId), anyInt()))
                    .thenReturn(max);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
