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
package org.neo4j.kernel.api.impl.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.collector.StubValuesIterator;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor.EntityValueClient;
import org.neo4j.values.storable.Value;

class LuceneScoredEntityIndexProgressorTest {
    @Test
    void mustSkipAndLimitEntriesPerConstraints() {
        StubValuesIterator iterator = new StubValuesIterator();
        iterator.add(1, 1.0f);
        iterator.add(2, 2.0f);
        iterator.add(3, 3.0f);
        iterator.add(4, 4.0f);

        IndexQueryConstraints constraints = unconstrained().skip(1).limit(2);

        StubEntityValueClient client = new StubEntityValueClient();
        IndexProgressor progressor = new LuceneScoredEntityIndexProgressor(iterator, client, constraints);
        boolean keepGoing;
        do {
            keepGoing = progressor.next();
        } while (keepGoing);

        assertThat(client.entityIds).containsExactly(2L, 3L);
        assertThat(client.scores).containsExactly(2.0f, 3.0f);
    }

    @Test
    void mustLimitToOneEntryPerConstraints() {
        StubValuesIterator iterator = new StubValuesIterator();
        iterator.add(1, 1.0f);
        iterator.add(2, 2.0f);
        iterator.add(3, 3.0f);
        iterator.add(4, 4.0f);

        IndexQueryConstraints constraints = unconstrained().skip(1).limit(1);

        StubEntityValueClient client = new StubEntityValueClient();
        IndexProgressor progressor = new LuceneScoredEntityIndexProgressor(iterator, client, constraints);
        boolean keepGoing;
        do {
            keepGoing = progressor.next();
        } while (keepGoing);

        assertThat(client.entityIds).containsExactly(2L);
        assertThat(client.scores).containsExactly(2.0f);
    }

    @Test
    void mustReturnNoElementsWhenSkipIsGreaterThanIterator() {
        StubValuesIterator iterator = new StubValuesIterator();
        iterator.add(1, 1.0f);
        iterator.add(2, 2.0f);
        iterator.add(3, 3.0f);
        iterator.add(4, 4.0f);

        IndexQueryConstraints constraints = unconstrained().skip(4).limit(1);

        StubEntityValueClient client = new StubEntityValueClient();
        IndexProgressor progressor = new LuceneScoredEntityIndexProgressor(iterator, client, constraints);
        boolean keepGoing;
        do {
            keepGoing = progressor.next();
        } while (keepGoing);

        assertThat(client.entityIds).isEmpty();
        assertThat(client.scores).isEmpty();
    }

    @Test
    void mustExhaustIteratorWhenLimitIsGreaterThanIterator() {
        StubValuesIterator iterator = new StubValuesIterator();
        iterator.add(1, 1.0f);
        iterator.add(2, 2.0f);
        iterator.add(3, 3.0f);
        iterator.add(4, 4.0f);

        IndexQueryConstraints constraints = unconstrained().limit(5);

        StubEntityValueClient client = new StubEntityValueClient();
        IndexProgressor progressor = new LuceneScoredEntityIndexProgressor(iterator, client, constraints);
        boolean keepGoing;
        do {
            keepGoing = progressor.next();
        } while (keepGoing);

        assertThat(client.entityIds).containsExactly(1L, 2L, 3L, 4L);
        assertThat(client.scores).containsExactly(1.0f, 2.0f, 3.0f, 4.0f);
    }

    private static class StubEntityValueClient implements EntityValueClient {
        private final List<Long> entityIds = new ArrayList<>();
        private final List<Float> scores = new ArrayList<>();

        @Override
        public void initializeQuery(
                IndexDescriptor descriptor,
                IndexProgressor progressor,
                boolean indexIncludesTransactionState,
                boolean needStoreFilter,
                IndexQueryConstraints constraints,
                PropertyIndexQuery... query) {}

        @Override
        public boolean acceptEntity(long reference, float score, Value... values) {
            entityIds.add(reference);
            scores.add(score);
            return true;
        }

        @Override
        public boolean needsValues() {
            return false;
        }
    }
}
