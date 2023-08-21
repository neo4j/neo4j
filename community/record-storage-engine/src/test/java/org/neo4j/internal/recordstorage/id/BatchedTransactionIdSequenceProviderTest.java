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

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

class BatchedTransactionIdSequenceProviderTest {

    private NeoStores neoStores;

    @BeforeEach
    void setUp() {
        neoStores = mock(NeoStores.class);
        RecordStore<AbstractBaseRecord> nodeStore = mock(RecordStore.class);
        when(neoStores.getRecordStore(StoreType.NODE)).thenReturn(nodeStore);
    }

    @Test
    void createAndReuseBatchSequences() {
        var sequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequence1 = sequenceProvider.getIdSequence(StoreType.NODE);
        var idSequence2 = sequenceProvider.getIdSequence(StoreType.NODE);
        var idSequence3 = sequenceProvider.getIdSequence(StoreType.NODE);
        var idSequence4 = sequenceProvider.getIdSequence(StoreType.NODE);

        assertSame(idSequence1, idSequence2);
        assertSame(idSequence2, idSequence3);
        assertSame(idSequence3, idSequence4);
        assertSame(idSequence4, idSequence1);
    }

    @Test
    void releaseIdSequencesOnRelease() {
        var sequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequence1 = sequenceProvider.getIdSequence(StoreType.NODE);

        sequenceProvider.release(CursorContext.NULL_CONTEXT);

        var idSequence2 = sequenceProvider.getIdSequence(StoreType.NODE);

        assertNotSame(idSequence1, idSequence2);
    }
}
