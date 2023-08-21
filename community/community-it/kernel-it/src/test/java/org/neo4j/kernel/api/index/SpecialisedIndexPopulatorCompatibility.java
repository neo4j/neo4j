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
package org.neo4j.kernel.api.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;

abstract class SpecialisedIndexPopulatorCompatibility
        extends SpecialisedIndexProviderCompatibilityTestSuite.Compatibility {
    SpecialisedIndexPopulatorCompatibility(SpecialisedIndexProviderCompatibilityTestSuite testSuite) {
        super(testSuite, testSuite.indexPrototype());
    }

    @Test
    void shouldStorePopulationFailedForRetrievalFromProviderLater() throws Exception {
        // GIVEN
        String failure = "The contrived failure";
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig(config);
        // WHEN (this will attempt to call close)
        withPopulator(
                indexProvider.getPopulator(
                        descriptor,
                        indexSamplingConfig,
                        heapBufferFactory(1024),
                        INSTANCE,
                        tokenNameLookup,
                        immutable.empty(),
                        StorageEngineIndexingBehaviour.EMPTY),
                p -> p.markAsFailed(failure),
                false);
        // THEN
        assertThat(indexProvider.getPopulationFailure(descriptor, NULL_CONTEXT, immutable.empty()))
                .contains(failure);
    }

    @Test
    void shouldReportInitialStateAsFailedIfPopulationFailed() throws Exception {
        // GIVEN
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig(config);
        withPopulator(
                indexProvider.getPopulator(
                        descriptor,
                        indexSamplingConfig,
                        heapBufferFactory(1024),
                        INSTANCE,
                        tokenNameLookup,
                        immutable.empty(),
                        StorageEngineIndexingBehaviour.EMPTY),
                p -> {
                    String failure = "The contrived failure";

                    // WHEN
                    p.markAsFailed(failure);
                    p.close(false, NULL_CONTEXT);

                    // THEN
                    assertEquals(FAILED, indexProvider.getInitialState(descriptor, NULL_CONTEXT, immutable.empty()));
                },
                false);
    }

    @Test
    void shouldBeAbleToDropAClosedIndexPopulator() {
        // GIVEN
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig(config);
        final IndexPopulator p = indexProvider.getPopulator(
                descriptor,
                indexSamplingConfig,
                heapBufferFactory(1024),
                INSTANCE,
                tokenNameLookup,
                immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        p.close(false, NULL_CONTEXT);

        // WHEN
        p.drop();

        // THEN - no exception should be thrown (it's been known to!)
    }
}
