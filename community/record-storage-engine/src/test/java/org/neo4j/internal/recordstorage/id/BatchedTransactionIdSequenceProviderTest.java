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
package org.neo4j.internal.recordstorage.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.StoreType.NODE;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.range.ArrayBasedRange;
import org.neo4j.internal.id.range.ContinuousIdRange;
import org.neo4j.internal.id.range.PageIdRange;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

class BatchedTransactionIdSequenceProviderTest {

    private NeoStores neoStores;

    @BeforeEach
    void setUp() {
        neoStores = mock(NeoStores.class);
        RecordStore<AbstractBaseRecord> nodeStore = mock(RecordStore.class);
        IdGenerator idGenerator = mock(IdGenerator.class);

        when(neoStores.getRecordStore(NODE)).thenReturn(nodeStore);
        when(nodeStore.getIdGenerator()).thenReturn(idGenerator);
        when(idGenerator.nextPageRange(any(), anyInt()))
                .thenReturn(new ContinuousIdRange(100, 10, 120))
                .thenReturn(new ContinuousIdRange(200, 10, 120));
    }

    @Test
    void createAndReuseBatchSequences() {
        var sequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequence1 = sequenceProvider.getIdSequence(NODE);
        var idSequence2 = sequenceProvider.getIdSequence(NODE);
        var idSequence3 = sequenceProvider.getIdSequence(NODE);
        var idSequence4 = sequenceProvider.getIdSequence(NODE);

        assertSame(idSequence1, idSequence2);
        assertSame(idSequence2, idSequence3);
        assertSame(idSequence3, idSequence4);
        assertSame(idSequence4, idSequence1);
    }

    @Test
    void releaseIdSequencesOnRelease() {
        var sequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequence1 = sequenceProvider.getIdSequence(NODE);

        sequenceProvider.release(CursorContext.NULL_CONTEXT);

        var idSequence2 = sequenceProvider.getIdSequence(NODE);

        assertNotSame(idSequence1, idSequence2);
    }

    @Test
    void rangeIsEmptyAfterReleaseAndNewRequestedOnNextCall() {
        var sequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequence = sequenceProvider.getIdSequence(NODE);

        assertEquals(100, idSequence.nextId(CursorContext.NULL_CONTEXT));

        sequenceProvider.release(CursorContext.NULL_CONTEXT);

        assertEquals(200, idSequence.nextId(CursorContext.NULL_CONTEXT));
    }

    @Test
    void idRangeOfReusedIds() {
        var pageIdRange = PageIdRange.wrap(new long[] {4, 5, 6}, 280);
        assertThat(pageIdRange).isInstanceOf(ContinuousIdRange.class);
        assertEquals(4, pageIdRange.nextId());

        pageIdRange = PageIdRange.wrap(new long[] {4, 6}, 280);
        assertThat(pageIdRange).isInstanceOf(ArrayBasedRange.class);
        assertEquals(4, pageIdRange.nextId());

        pageIdRange = PageIdRange.wrap(new long[] {6}, 280);
        assertThat(pageIdRange).isInstanceOf(ContinuousIdRange.class);
        assertEquals(6, pageIdRange.nextId());

        pageIdRange = PageIdRange.wrap(new long[] {2, 5, 6, 7, 8, 9}, 280);
        assertThat(pageIdRange).isInstanceOf(ArrayBasedRange.class);
        assertEquals(2, pageIdRange.nextId());
    }

    @Test
    void idRangePage() {
        assertEquals(0, new ContinuousIdRange(4, 3, 280).pageId());
        assertEquals(0, new ContinuousIdRange(217, 3, 280).pageId());
        assertEquals(1, new ContinuousIdRange(280, 3, 280).pageId());
        assertEquals(2, new ContinuousIdRange(700, 3, 280).pageId());

        assertEquals(0, new ArrayBasedRange(new long[] {0, 1, 2}, 140).pageId());
        assertEquals(1, new ArrayBasedRange(new long[] {140, 141, 142}, 140).pageId());
        assertEquals(2, new ArrayBasedRange(new long[] {300}, 140).pageId());
    }

    @Test
    void failToResetSequencesWhenRangesWereSwitched() {
        var sequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);

        var idSequence = sequenceProvider.getIdSequence(NODE);
        for (int i = 0; i < 19; i++) {
            idSequence.nextId(CursorContext.NULL_CONTEXT);
        }

        assertFalse(sequenceProvider.reset());
    }

    @Test
    void ableToResetSequencesWhenRangesNotSwitched() {
        var sequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);

        var idSequence = sequenceProvider.getIdSequence(NODE);
        for (int i = 0; i < 9; i++) {
            idSequence.nextId(CursorContext.NULL_CONTEXT);
        }

        assertTrue(sequenceProvider.reset());
    }

    @Test
    void ableToResetProviderWithoutAnyAllocations() {
        var sequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
        assertTrue(sequenceProvider.reset());
    }
}
