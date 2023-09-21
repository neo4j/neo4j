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
package org.neo4j.kernel.impl.api.index;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.schema.SchemaTestUtil;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.DatabaseIndexStats;

class OnlineIndexProxyTest {
    private final long indexId = 1;
    private final IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2))
            .withName("index")
            .materialise(indexId);
    private final IndexAccessor accessor = mock(IndexAccessor.class);
    private final IndexStoreView storeView = mock(IndexStoreView.class);
    private final IndexStatisticsStore indexStatisticsStore = mock(IndexStatisticsStore.class);
    private final IndexProxyStrategy indexProxyStrategy =
            new ValueIndexProxyStrategy(descriptor, indexStatisticsStore, SchemaTestUtil.SIMPLE_NAME_LOOKUP);

    @Test
    void shouldRemoveIndexCountsWhenTheIndexItselfIsDropped() {
        // given
        OnlineIndexProxy index =
                new OnlineIndexProxy(indexProxyStrategy, accessor, false, NO_USAGE_TRACKING, new DatabaseIndexStats());

        // when
        index.drop();

        // then
        verify(accessor).drop();
        verify(indexStatisticsStore).removeIndex(indexId);
        verifyNoMoreInteractions(accessor, storeView);
    }
}
