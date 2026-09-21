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
package org.neo4j.internal.batchimport.input;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.importer.SchemaCommandSource.ResolvedSchemaCommands;

class EntityListInputTest {
    @Test
    void shouldIterateAllEntitiesAcrossMultipleChunks() throws IOException {
        var groups = new Groups();
        var global = groups.getOrCreate(null);
        var nodes = new ArrayList<InputEntity>();
        for (int i = 0; i < 250; i++) {
            var node = new InputEntity();
            node.id(i, global);
            node.labels(new String[] {"Node"});
            node.property("index", i, false);
            nodes.add(node);
        }
        var input = new EntityListInput(groups, nodes, List.of(), ResolvedSchemaCommands.of());

        assertThat(visitAll(input.nodes(Collector.EMPTY).iterator())).isEqualTo(250);
        assertThat(visitAll(input.relationships(Collector.EMPTY).iterator())).isZero();
        assertThat(visitAll(input.nodes(Collector.EMPTY).iterator()))
                .as("each iterator() call starts a fresh iteration")
                .isEqualTo(250);
    }

    @Test
    void shouldComputeEstimatesFromLists() {
        var groups = new Groups();
        var global = groups.getOrCreate(null);
        var node = new InputEntity();
        node.id(0, global);
        node.labels(new String[] {"A", "B"});
        node.property("one", 1, false);
        node.property("two", 2, false);
        var input = new EntityListInput(groups, List.of(node), List.of(), ResolvedSchemaCommands.of());

        var estimates = input.validateAndEstimate((values, cursorContext, memoryTracker) -> 0, 1);

        assertThat(estimates.numberOfNodes()).isEqualTo(1);
        assertThat(estimates.numberOfRelationships()).isZero();
        assertThat(estimates.numberOfNodeProperties()).isEqualTo(2);
        assertThat(estimates.numberOfNodeLabels()).isEqualTo(2);
    }

    private static int visitAll(org.neo4j.batchimport.api.InputIterator iterator) throws IOException {
        int visited = 0;
        try (iterator) {
            var chunk = iterator.newChunk();
            while (iterator.next(chunk)) {
                while (chunk.next(new InputEntity())) {
                    visited++;
                }
            }
        }
        return visited;
    }
}
