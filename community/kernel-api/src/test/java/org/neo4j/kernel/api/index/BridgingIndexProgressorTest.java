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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class BridgingIndexProgressorTest {
    @Inject
    private RandomSupport random;

    @Test
    void closeMustCloseAll() {
        IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2, 3))
                .withName("a")
                .materialise(0);
        BridgingIndexProgressor progressor =
                new BridgingIndexProgressor(null, index.schema().getPropertyIds());

        IndexProgressor[] parts = {mock(IndexProgressor.class), mock(IndexProgressor.class)};

        // Given
        for (IndexProgressor part : parts) {
            progressor.initializeQuery(index, part, false, false, unconstrained());
        }

        // When
        progressor.close();

        // Then
        for (IndexProgressor part : parts) {
            verify(part).close();
        }
    }

    @Test
    void aggregateNeedStoreFilter() {
        IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2, 3))
                .withName("a")
                .materialise(0);
        BridgingIndexProgressor progressor =
                new BridgingIndexProgressor(null, index.schema().getPropertyIds());

        IndexProgressor[] parts = {mock(IndexProgressor.class), mock(IndexProgressor.class)};

        // Given
        boolean anyNeedStoreFilter = false;
        for (IndexProgressor part : parts) {
            var needStoreFilter = random.nextBoolean();
            anyNeedStoreFilter |= needStoreFilter;
            progressor.initializeQuery(index, part, false, needStoreFilter, unconstrained());
        }

        // When
        assertThat(progressor.needStoreFilter()).isEqualTo(anyNeedStoreFilter);
    }
}
