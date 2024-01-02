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

import static org.neo4j.kernel.impl.store.StoreType.STORE_TYPES;

import java.util.Arrays;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.id.range.PageIdRange;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreType;

/**
 * Id sequence provider that performs id reservations in batches (range of ids that cover the whole page).
 * Sequence provided by this provider instead of allocating individual ids on each #nextId calls
 * will use underlying range. On close of batch sequence any leftovers will be released back to id generator.
 */
public class BatchedTransactionIdSequenceProvider implements IdSequenceProvider {
    private final NeoStores neoStores;
    private final BatchedIdSequence[] transactionSequences = new BatchedIdSequence[STORE_TYPES.length];

    public BatchedTransactionIdSequenceProvider(NeoStores neoStores) {
        this.neoStores = neoStores;
    }

    @Override
    public IdSequence getIdSequence(StoreType storeType) {
        return getOrCreateSequence(storeType);
    }

    private IdSequence getOrCreateSequence(StoreType storeType) {
        int typeIndex = storeType.ordinal();
        var sequence = transactionSequences[typeIndex];
        if (sequence != null) {
            return sequence;
        }

        var newSequence = new BatchedIdSequence(storeType);
        transactionSequences[typeIndex] = newSequence;
        return newSequence;
    }

    @Override
    public void release(CursorContext cursorContext) {
        for (BatchedIdSequence batchedIdSequence : transactionSequences) {
            if (batchedIdSequence != null) {
                batchedIdSequence.close(cursorContext);
            }
        }
        Arrays.fill(transactionSequences, null);
    }

    private class BatchedIdSequence implements IdSequence {
        private final int recordsPerPage;
        private PageIdRange range = PageIdRange.EMPTY;
        private final IdGenerator idGenerator;

        public BatchedIdSequence(StoreType storeType) {
            var store = neoStores.getRecordStore(storeType);
            this.idGenerator = store.getIdGenerator();
            this.recordsPerPage = store.getRecordsPerPage();
        }

        @Override
        public long nextId(CursorContext cursorContext) {
            if (!range.hasNext()) {
                close(cursorContext);
                range = idGenerator.nextPageRange(cursorContext, recordsPerPage);
            }
            return range.nextId();
        }

        public void close(CursorContext cursorContext) {
            if (range == PageIdRange.EMPTY) {
                return;
            }
            idGenerator.releasePageRange(range, cursorContext);
            range = PageIdRange.EMPTY;
        }
    }
}
