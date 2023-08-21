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
package org.neo4j.kernel.impl.transaction.state.storeview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveArrays.intsToLongs;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.impl.api.index.StoreScan.NO_EXTERNAL_UPDATES;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.ArrayList;
import java.util.List;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

@ExtendWith(RandomExtension.class)
class FullScanStoreViewNodeIdsTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldScanRelationshipsFromNodes() {
        // given
        var storageReader = new StubStorageCursors();
        var jobScheduler = new ThreadPoolJobScheduler();
        var storageEngine = mock(StorageEngine.class);
        when(storageEngine.newReader()).thenReturn(storageReader);
        var indexingBehaviour = mock(StorageEngineIndexingBehaviour.class);
        when(indexingBehaviour.useNodeIdsInRelationshipTokenIndex()).thenReturn(true);
        when(storageEngine.indexingBehaviour()).thenReturn(indexingBehaviour);
        when(storageEngine.createStorageCursors(any())).thenReturn(StoreCursors.NULL);
        var fullScanStoreView = new FullScanStoreView(NO_LOCK_SERVICE, storageEngine, Config.defaults(), jobScheduler);
        var relationshipTypes = new int[] {1, 2, 3};
        createData(storageReader, relationshipTypes);

        // when
        var consumer = new TestTokenScanConsumer();
        var scan = fullScanStoreView.visitRelationships(
                relationshipTypes,
                PropertySelection.ALL_PROPERTIES,
                null,
                consumer,
                true,
                false,
                NULL_CONTEXT_FACTORY,
                INSTANCE);
        scan.run(NO_EXTERNAL_UPDATES);

        // then
        MutableLongObjectMap<long[]> actual = LongObjectMaps.mutable.empty();
        consumer.batches.forEach(
                batch -> batch.forEach(record -> actual.put(record.getEntityId(), record.getTokens())));
        try (var nodeCursor = storageReader.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL)) {
            nodeCursor.scan();
            while (nodeCursor.next()) {
                long[] actualRelationshipTypes = actual.remove(nodeCursor.entityReference());
                int[] expectedRelationshipTypes = nodeCursor.relationshipTypes();
                assertThat(LongSets.immutable.of(actualRelationshipTypes))
                        .isEqualTo(LongSets.immutable.of(intsToLongs(expectedRelationshipTypes)));
            }
            assertThat(actual.isEmpty()).isTrue();
        }
    }

    private void createData(StubStorageCursors storageReader, int[] relationshipTypes) {
        List<StubStorageCursors.NodeData> nodes = new ArrayList<>();
        for (int n = 0; n < 10; n++) {
            nodes.add(storageReader.withNode(n));
        }
        for (int r = 0; r < 20; r++) {
            storageReader.withRelationship(
                    r,
                    random.among(nodes).getId(),
                    random.among(relationshipTypes),
                    random.among(nodes).getId());
        }
    }
}
