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
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.values.storable.Values.stringValue;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;
import org.neo4j.kernel.impl.newapi.KernelRead;

public class TrigramIndexQueryTest extends TextIndexQueryTest {

    @Override
    protected IndexProviderDescriptor getIndexProviderDescriptor() {
        return TrigramIndexProvider.DESCRIPTOR;
    }

    @Test
    void countIndexEntitiesShouldFindExactEntity() throws Exception {
        try (ValueIndexReader valueIndexReader = ((KernelRead) read).newValueIndexReader(getIndex(NODE_INDEX_NAME))) {
            long countIndexedEntities = valueIndexReader.countIndexedEntities(
                    mikeNodeId, NULL_CONTEXT, new int[] {token.propertyKey(NAME)}, stringValue("Mike Smith"));
            assertThat(countIndexedEntities).isEqualTo(1);
        }
    }

    @Test
    void sadlyCountIndexEntitiesFindNotExactEntity() throws Exception {
        try (ValueIndexReader valueIndexReader = ((KernelRead) read).newValueIndexReader(getIndex(NODE_INDEX_NAME))) {
            // Will find Mike Smith even though we are asking for Smith
            long countIndexedEntities = valueIndexReader.countIndexedEntities(
                    mikeNodeId, NULL_CONTEXT, new int[] {token.propertyKey(NAME)}, stringValue("Smith"));
            assertThat(countIndexedEntities).isEqualTo(1);
        }
    }

    @Test
    void countIndexEntitiesDoesntFindOtherEntity() throws Exception {
        try (ValueIndexReader valueIndexReader = ((KernelRead) read).newValueIndexReader(getIndex(NODE_INDEX_NAME))) {
            // At least we don't find entities with other ids
            long countIndexedEntities = valueIndexReader.countIndexedEntities(
                    noahNodeId, NULL_CONTEXT, new int[] {token.propertyKey(NAME)}, stringValue("Smith"));
            assertThat(countIndexedEntities).isEqualTo(0);
        }
    }
}
