/*
 * Copyright (c) "Neo4j"
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LatchCrabbingCoordinationTest {
    private static final int MERGE_THRESHOLD = 100;

    private final TreeNodeLatchService latchService = mock(TreeNodeLatchService.class);
    private final LatchCrabbingCoordination coordination = new LatchCrabbingCoordination(latchService, MERGE_THRESHOLD);

    @BeforeEach
    void setUp() {
        when(latchService.acquireRead(anyLong())).thenAnswer(invocationOnMock -> mock(LongSpinLatch.class));
        coordination.initialize();
    }

    @Test
    void shouldOptimisticallyAcquireReadLatchesWhenTraversingDown() {
        // when
        coordination.beforeTraversingToChild(1L, 0);
        coordination.beforeTraversingToChild(2L, 0);
        coordination.beforeTraversingToChild(3L, 0);

        // then
        verify(latchService).acquireRead(1L);
        verify(latchService).acquireRead(2L);
        verify(latchService).acquireRead(3L);
    }

    @Test
    void shouldOptimisticallyUpgradeToWriteWhenArrivingAtLeaf() {
        // given
        LongSpinLatch leafLatch = mock(LongSpinLatch.class);
        when(leafLatch.tryUpgradeToWrite()).thenReturn(true);
        when(latchService.acquireRead(2L)).thenReturn(leafLatch);

        // when
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        // then
        verify(latchService).acquireRead(1L);
        verify(latchService).acquireRead(2L);
        verify(leafLatch).tryUpgradeToWrite();
    }

    @Test
    void shouldOptimisticallyUpgradeParentOnLeafSplit() {
        // given
        LongSpinLatch parentLatch = mock(LongSpinLatch.class);
        when(parentLatch.tryUpgradeToWrite()).thenReturn(true);
        when(latchService.acquireRead(1L)).thenReturn(parentLatch);
        LongSpinLatch leafLatch = mock(LongSpinLatch.class);
        when(leafLatch.tryUpgradeToWrite()).thenReturn(true);
        when(latchService.acquireRead(2L)).thenReturn(leafLatch);

        // when
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));
        assertTrue(coordination.beforeSplittingLeaf(10));

        // then
        verify(latchService).acquireRead(1L);
        verify(latchService).acquireRead(2L);
        verify(leafLatch).tryUpgradeToWrite();
        verify(parentLatch).tryUpgradeToWrite();
    }

    @Test
    void shouldOptimisticallyUpgradeParentOnLeafNeedsSuccessor() {
        // given
        LongSpinLatch parentLatch = mock(LongSpinLatch.class);
        when(parentLatch.tryUpgradeToWrite()).thenReturn(true);
        when(latchService.acquireRead(1L)).thenReturn(parentLatch);
        LongSpinLatch leafLatch = mock(LongSpinLatch.class);
        when(leafLatch.tryUpgradeToWrite()).thenReturn(true);
        when(latchService.acquireRead(2L)).thenReturn(leafLatch);

        // when
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, true, 5));

        // then
        verify(latchService).acquireRead(1L);
        verify(latchService).acquireRead(2L);
        verify(leafLatch).tryUpgradeToWrite();
        verify(parentLatch).tryUpgradeToWrite();
    }

    @Test
    void shouldOptimisticallyFailLeafSplitResultingInParentSplit() {
        // given
        LongSpinLatch leafLatch = mock(LongSpinLatch.class);
        when(leafLatch.tryUpgradeToWrite()).thenReturn(true);
        when(latchService.acquireRead(2L)).thenReturn(leafLatch);

        // when
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        // then
        assertFalse(coordination.beforeSplittingLeaf(10)); // <-- bigger than parent available space
        verify(latchService).acquireRead(1L);
        verify(latchService).acquireRead(2L);
        verify(leafLatch).tryUpgradeToWrite();
    }

    @Test
    void shouldOptimisticallyFailArriveAtChildOnLeafNeedsSuccessorAndFailToUpgradeParent() {
        // given
        LongSpinLatch parentLatch = mock(LongSpinLatch.class);
        when(parentLatch.tryUpgradeToWrite()).thenReturn(false);
        when(latchService.acquireRead(1L)).thenReturn(parentLatch);
        LongSpinLatch leafLatch = mock(LongSpinLatch.class);
        when(leafLatch.tryUpgradeToWrite()).thenReturn(true);
        when(latchService.acquireRead(2L)).thenReturn(leafLatch);

        // when
        coordination.beforeTraversingToChild(1L, 2);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 2);

        // then
        assertFalse(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, true, 5));
        verify(latchService).acquireRead(1L);
        verify(latchService).acquireRead(2L);
        verify(leafLatch).tryUpgradeToWrite();
        verify(parentLatch).tryUpgradeToWrite();
    }

    @Test
    void shouldOptimisticallyFailArriveAtChildOnLeafNeedsSuccessorForEdgeChildPosLeft() {
        // given
        LongSpinLatch parentLatch = mock(LongSpinLatch.class);
        when(parentLatch.tryUpgradeToWrite()).thenReturn(false);
        when(latchService.acquireRead(1L)).thenReturn(parentLatch);
        LongSpinLatch leafLatch = mock(LongSpinLatch.class);
        when(leafLatch.tryUpgradeToWrite()).thenReturn(true);
        when(latchService.acquireRead(2L)).thenReturn(leafLatch);

        // when
        coordination.beforeTraversingToChild(1L, 2);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 0);

        // then
        assertFalse(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, true, 5));
        verify(latchService).acquireRead(1L);
        verify(latchService).acquireRead(2L);
        verify(leafLatch).tryUpgradeToWrite();
        verify(parentLatch, never()).tryUpgradeToWrite();
    }

    @Test
    void shouldOptimisticallyFailArriveAtChildOnLeafNeedsSuccessorForEdgeChildPosRight() {
        // given
        LongSpinLatch parentLatch = mock(LongSpinLatch.class);
        when(parentLatch.tryUpgradeToWrite()).thenReturn(false);
        when(latchService.acquireRead(1L)).thenReturn(parentLatch);
        LongSpinLatch leafLatch = mock(LongSpinLatch.class);
        when(leafLatch.tryUpgradeToWrite()).thenReturn(true);
        when(latchService.acquireRead(2L)).thenReturn(leafLatch);

        // when
        coordination.beforeTraversingToChild(1L, 2);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 5);

        // then
        assertFalse(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, true, 5));
        verify(latchService).acquireRead(1L);
        verify(latchService).acquireRead(2L);
        verify(leafLatch).tryUpgradeToWrite();
        verify(parentLatch, never()).tryUpgradeToWrite();
    }

    @Test
    void shouldOptimisticallySucceedRemovalIfLeafWillNotUnderflow() {
        // given
        LongSpinLatch leafLatch = mock(LongSpinLatch.class);
        when(leafLatch.tryUpgradeToWrite()).thenReturn(true);
        when(latchService.acquireRead(2L)).thenReturn(leafLatch);

        // when
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));

        // then
        assertTrue(coordination.beforeRemovalFromLeaf(10));
        verify(latchService).acquireRead(1L);
        verify(latchService).acquireRead(2L);
    }

    @Test
    void shouldOptimisticallyFailRemovalIfLeafUnderflow() {
        // given
        LongSpinLatch leafLatch = mock(LongSpinLatch.class);
        when(leafLatch.tryUpgradeToWrite()).thenReturn(true);
        when(latchService.acquireRead(2L)).thenReturn(leafLatch);

        // when
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD - 5, false, 5));

        // then
        assertFalse(coordination.beforeRemovalFromLeaf(10));
    }

    @Test
    void shouldPessimisticallyAcquireWriteLatchesWhenTraversingDown() {
        // given
        coordination.flipToPessimisticMode();

        // when
        coordination.beforeTraversingToChild(1L, 1);
        coordination.beforeTraversingToChild(2L, 2);
        coordination.beforeTraversingToChild(3L, 3);

        // then
        verify(latchService).acquireWrite(1L);
        verify(latchService).acquireWrite(2L);
        verify(latchService).acquireWrite(3L);
    }

    @Test
    void shouldPessimisticallySucceedLeafSplitResultingInParentSplit() {
        // given
        coordination.flipToPessimisticMode();

        // when
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, 3, false, 5));
        coordination.beforeTraversingToChild(2L, 2);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5));
        assertTrue(coordination.beforeSplittingLeaf(10)); // <-- bigger than parent available space

        // then
        verify(latchService).acquireWrite(1L);
        verify(latchService).acquireWrite(2L);
    }

    @Test
    void shouldPessimisticallySucceedArriveAtChildOnLeafNeedsSuccessor() {
        // given
        coordination.flipToPessimisticMode();

        // when
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 1);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, true, 5));

        // then
        verify(latchService).acquireWrite(1L);
        verify(latchService).acquireWrite(2L);
    }

    @Test
    void shouldPessimisticallySucceedRemovalIfLeafUnderflow() {
        // given
        coordination.flipToPessimisticMode();

        // when
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 2);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD - 5, false, 5));

        // then
        assertTrue(coordination.beforeRemovalFromLeaf(10));
        verify(latchService).acquireWrite(1L);
        verify(latchService).acquireWrite(2L);
    }

    @Test
    void shouldOptimisticallyReleaseAllLatchesWhenGoingBackUp() {
        // given
        LongSpinLatch latch1 = mock(LongSpinLatch.class);
        when(latchService.acquireRead(1L)).thenReturn(latch1);
        LongSpinLatch latch2 = mock(LongSpinLatch.class);
        when(latchService.acquireRead(2L)).thenReturn(latch2);
        LongSpinLatch latch3 = mock(LongSpinLatch.class);
        when(latchService.acquireRead(3L)).thenReturn(latch3);
        coordination.beforeTraversingToChild(1L, 1);
        coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5);
        coordination.beforeTraversingToChild(2L, 2);
        coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5);
        coordination.beforeTraversingToChild(3L, 3);
        coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5);

        // when
        coordination.reset();

        // then
        verify(latchService).acquireRead(1L);
        verify(latchService).acquireRead(2L);
        verify(latchService).acquireRead(3L);
        verify(latch1).releaseRead();
        verify(latch2).releaseRead();
        verify(latch3).releaseRead();
    }

    @Test
    void shouldPessimisticallyReleaseAllLatchesWhenGoingBackUp() {
        // given
        LongSpinLatch latch1 = mock(LongSpinLatch.class);
        when(latchService.acquireWrite(1L)).thenReturn(latch1);
        LongSpinLatch latch2 = mock(LongSpinLatch.class);
        when(latchService.acquireWrite(2L)).thenReturn(latch2);
        LongSpinLatch latch3 = mock(LongSpinLatch.class);
        when(latchService.acquireWrite(3L)).thenReturn(latch3);
        coordination.flipToPessimisticMode();
        coordination.beforeTraversingToChild(1L, 1);
        coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5);
        coordination.beforeTraversingToChild(2L, 2);
        coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5);
        coordination.beforeTraversingToChild(3L, 3);
        coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, false, 5);

        // when
        coordination.reset();

        // then
        verify(latchService).acquireWrite(1L);
        verify(latchService).acquireWrite(2L);
        verify(latchService).acquireWrite(3L);
        verify(latch1).releaseWrite();
        verify(latch2).releaseWrite();
        verify(latch3).releaseWrite();
    }

    @Test
    void shouldHandleFirstGoDownWithOptimisticThenWithPessimisticOnFailure() {
        // given
        LongSpinLatch leafLatch = mock(LongSpinLatch.class);
        when(leafLatch.tryUpgradeToWrite()).thenReturn(false);
        when(latchService.acquireRead(2L)).thenReturn(leafLatch);
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 2);
        assertFalse(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, true, 5));

        // when
        coordination.flipToPessimisticMode();
        coordination.beforeTraversingToChild(1L, 1);
        assertTrue(coordination.arrivedAtChild(true, MERGE_THRESHOLD / 2, false, 5));
        coordination.beforeTraversingToChild(2L, 2);
        assertTrue(coordination.arrivedAtChild(false, MERGE_THRESHOLD / 2, true, 5));

        // then
        verify(latchService).acquireRead(1L);
        verify(latchService).acquireRead(2L);
        verify(leafLatch).tryUpgradeToWrite();
        verify(latchService).acquireWrite(1L);
        verify(latchService).acquireWrite(2L);
    }

    @Test
    void shouldPreventOptimisticSplitLeafWhenParentPositionedAtEdgeAndNeedsSuccessor() {
        // given
        long grandParent = 10;
        long parent = 11;
        long leaf = 12;
        coordination.beforeTraversingToChild(grandParent, 0);
        coordination.arrivedAtChild(true, 100, false, 10);
        coordination.beforeTraversingToChild(parent, 0);
        coordination.arrivedAtChild(true, 100, true, 10);
        coordination.beforeTraversingToChild(leaf, 3);
        coordination.arrivedAtChild(false, 0, false, 10);

        // when
        boolean splitLeafAllowed = coordination.beforeSplittingLeaf(10);

        // then
        assertFalse(splitLeafAllowed);
    }
}
