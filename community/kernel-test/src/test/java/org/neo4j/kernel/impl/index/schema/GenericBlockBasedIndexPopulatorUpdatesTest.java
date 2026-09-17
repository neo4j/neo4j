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
package org.neo4j.kernel.impl.index.schema;

import static java.util.Collections.singleton;
import static org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions.assertThatThrownBy;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.IndexEntryTestUtil.generateStringValueResultingInIndexEntrySize;
import static org.neo4j.storageengine.api.EagerValueIndexEntryUpdate.add;

import java.io.IOException;
import java.util.Collection;
import org.junit.jupiter.api.Test;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class GenericBlockBasedIndexPopulatorUpdatesTest<KEY extends GenericKey<KEY>>
        extends BlockBasedIndexPopulatorUpdatesTest<KEY> {
    @Test
    void shouldHandleEntriesOfMaxSize()
            throws IndexEntryConflictException, IOException, IndexNotApplicableKernelException {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(INDEX_DESCRIPTOR);
        try {
            int maxKeyValueSize = populator.tree.keyValueSizeCap();
            EagerValueIndexEntryUpdate update = add(
                    1,
                    INDEX_DESCRIPTOR,
                    generateStringValueResultingInIndexEntrySize(populator.layout, maxKeyValueSize));

            // when
            Collection<EagerValueIndexEntryUpdate> updates = singleton(update);
            populator.add(updates, NULL_CONTEXT);
            populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);

            // then
            assertHasEntry(populator, update.values()[0], 1);
        } finally {
            populator.close(true, NULL_CONTEXT);
        }
    }

    @Test
    void shouldThrowForEntriesLargerThanMaxSize() throws IOException {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(INDEX_DESCRIPTOR);
        try {
            int maxKeyValueSize = populator.tree.keyValueSizeCap();
            EagerValueIndexEntryUpdate update = add(
                    1,
                    INDEX_DESCRIPTOR,
                    generateStringValueResultingInIndexEntrySize(populator.layout, maxKeyValueSize + 1));

            assertThatThrownBy(() -> {
                        Collection<EagerValueIndexEntryUpdate> updates = singleton(update);
                        populator.add(updates, NULL_CONTEXT);
                        populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
                    })
                    .hasGqlStatus(GqlStatusInfoCodes.STATUS_22NBH)
                    .hasMessageContainingAll(
                            "The property value size",
                            "of element id",
                            "is too large for the index",
                            "See the Operations Manual for index limitations.");
        } finally {
            populator.close(true, NULL_CONTEXT);
        }
    }

    @Override
    Value supportedValue(int identifier) {
        return Values.stringValue("StringValue " + identifier);
    }
}
