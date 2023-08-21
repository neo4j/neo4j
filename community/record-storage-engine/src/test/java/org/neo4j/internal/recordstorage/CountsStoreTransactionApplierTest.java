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
package org.neo4j.internal.recordstorage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

import org.junit.jupiter.api.Test;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.internal.counts.DegreeUpdater;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;

class CountsStoreTransactionApplierTest {
    @Test
    void shouldNotifyCacheAccessOnHowManyUpdatesOnCountsWeHadSoFar() throws Exception {
        // GIVEN
        final GBPTreeCountsStore counts = mock(GBPTreeCountsStore.class);
        final CountsUpdater updater = mock(CountsUpdater.class);
        when(counts.updater(anyLong(), anyBoolean(), any(CursorContext.class))).thenReturn(updater);
        final RelationshipGroupDegreesStore groupDegreesStore = mock(RelationshipGroupDegreesStore.class);
        when(groupDegreesStore.updater(anyLong(), anyBoolean(), any(CursorContext.class)))
                .thenReturn(mock(DegreeUpdater.class));
        final CountsStoreTransactionApplierFactory applier =
                new CountsStoreTransactionApplierFactory(counts, groupDegreesStore);

        // WHEN
        try (TransactionApplier txApplier =
                applier.startTx(new GroupOfCommands(2L, StoreCursors.NULL), mock(BatchContext.class))) {
            txApplier.visitNodeCountsCommand(new Command.NodeCountsCommand(
                    RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION),
                    ANY_LABEL,
                    1));
        }

        // THEN
        verify(updater).incrementNodeCount(ANY_LABEL, 1);
    }
}
