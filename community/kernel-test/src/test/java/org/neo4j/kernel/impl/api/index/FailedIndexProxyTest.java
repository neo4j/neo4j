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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.api.schema.SchemaTestUtil;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

class FailedIndexProxyTest {
    private final MinimalIndexAccessor minimalIndexAccessor = mock(MinimalIndexAccessor.class);
    private final IndexPopulationFailure indexPopulationFailure = mock(IndexPopulationFailure.class);
    private final IndexStatisticsStore indexStatisticsStore = mock(IndexStatisticsStore.class);

    @Test
    void shouldRemoveIndexCountsWhenTheIndexItselfIsDropped() {
        // given
        String userDescription = "description";
        IndexProxyStrategy indexProxyStrategy = new ValueIndexProxyStrategy(
                forSchema(forLabel(1, 2), AllIndexProviderDescriptors.UNDECIDED)
                        .withName(userDescription)
                        .materialise(1),
                indexStatisticsStore,
                SchemaTestUtil.SIMPLE_NAME_LOOKUP);
        FailedIndexProxy index = new FailedIndexProxy(
                indexProxyStrategy, minimalIndexAccessor, indexPopulationFailure, NullLogProvider.getInstance());

        // when
        index.drop();

        // then
        verify(minimalIndexAccessor).drop();
        verify(indexStatisticsStore).removeIndex(anyLong());
        verifyNoMoreInteractions(minimalIndexAccessor, indexStatisticsStore);
    }

    @Test
    void shouldLogReasonForDroppingIndex() {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        // when
        IndexProxyStrategy indexProxyStrategy = new ValueIndexProxyStrategy(
                forSchema(forLabel(0, 0), AllIndexProviderDescriptors.UNDECIDED)
                        .withName("foo")
                        .materialise(1),
                indexStatisticsStore,
                SchemaTestUtil.SIMPLE_NAME_LOOKUP);
        new FailedIndexProxy(
                        indexProxyStrategy,
                        mock(IndexPopulator.class),
                        IndexPopulationFailure.failure("it broke"),
                        logProvider)
                .drop();

        // then
        assertThat(logProvider)
                .forClass(FailedIndexProxy.class)
                .forLevel(INFO)
                .containsMessages("FailedIndexProxy#drop index on Index( id=1, name='foo', type='RANGE', "
                        + "schema=(:Label0 {property0}), indexProvider='Undecided-0' ) dropped due to:\nit broke");
    }
}
