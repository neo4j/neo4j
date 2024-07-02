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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.Arrays;
import java.util.PrimitiveIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.lock.LockService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.api.TokenConstants;

class RelationshipIndexedRelationshipStoreScanTest {
    private final StubStorageCursors cursors = new StubStorageCursors();
    private final TokenIndexReader relationshipTypeScanReader = mock(TokenIndexReader.class);
    private final TokenScanConsumer typeScanConsumer = mock(TokenScanConsumer.class);
    private final PropertyScanConsumer propertyScanConsumer = mock(PropertyScanConsumer.class);
    private final JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();

    @AfterEach
    void tearDown() throws Exception {
        jobScheduler.close();
    }

    @Test
    void iterateOverRelationshipIds() {
        long highId = 15L;
        for (long i = 0; i < highId; i++) {
            cursors.withRelationship(i, 1, 0, 1);
        }
        int[] types = new int[] {1, 2};

        mockIdsReturnedFromTokenQueries();

        RelationshipIndexedRelationshipStoreScan storeScan = getRelationshipTypeScanViewStoreScan(types);
        PrimitiveLongResourceIterator idIterator =
                storeScan.getEntityIdIterator(CursorContext.NULL_CONTEXT, StoreCursors.NULL);

        // See that the idIterator asked about both types and was able to merge the results correctly.
        assertThat(idIterator.next()).isEqualTo(1L);
        assertThat(idIterator.next()).isEqualTo(2L);
        assertThat(idIterator.next()).isEqualTo(4L);
        assertThat(idIterator.next()).isEqualTo(5L);
        assertThat(idIterator.next()).isEqualTo(6L);
        assertThat(idIterator.next()).isEqualTo(8L);
        assertThat(idIterator.hasNext()).isEqualTo(false);
    }

    private void mockIdsReturnedFromTokenQueries() {
        // Make token index reader return different ids for the different tokens
        doAnswer(invocation -> {
                    IndexProgressor.EntityTokenClient client = invocation.getArgument(0);
                    TokenPredicate token = invocation.getArgument(2);
                    client.initializeQuery(
                            new IndexProgressor() {
                                private final PrimitiveIterator.OfLong relationshipsWithType1 =
                                        Arrays.stream(new long[] {1, 2, 4, 8}).iterator();
                                private final PrimitiveIterator.OfLong relationshipsWithType2 =
                                        Arrays.stream(new long[] {2, 5, 6}).iterator();

                                @Override
                                public boolean next() {
                                    PrimitiveIterator.OfLong relationshipsWithType = relationshipsWithType1;
                                    if (token.tokenId() == 2) {
                                        relationshipsWithType = relationshipsWithType2;
                                    }

                                    if (relationshipsWithType.hasNext()) {
                                        client.acceptEntity(relationshipsWithType.nextLong(), TokenConstants.NO_TOKEN);
                                        return true;
                                    }
                                    return false;
                                }

                                @Override
                                public void close() {}
                            },
                            token.tokenId(),
                            IndexOrder.NONE);
                    return null;
                })
                .when(relationshipTypeScanReader)
                .query(any(), any(), any(), any(), any());
    }

    private RelationshipIndexedRelationshipStoreScan getRelationshipTypeScanViewStoreScan(int[] relationshipTypeIds) {
        return new RelationshipIndexedRelationshipStoreScan(
                Config.defaults(),
                cursors,
                any -> StoreCursors.NULL,
                LockService.NO_LOCK_SERVICE,
                relationshipTypeScanReader,
                typeScanConsumer,
                propertyScanConsumer,
                relationshipTypeIds,
                PropertySelection.ALL_PROPERTIES,
                false,
                jobScheduler,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                INSTANCE,
                false);
    }
}
