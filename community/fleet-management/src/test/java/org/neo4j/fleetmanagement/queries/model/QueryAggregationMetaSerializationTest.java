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
package org.neo4j.fleetmanagement.queries.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class QueryAggregationMetaSerializationTest {

    @Test
    void shouldSerializeGettersAsFields() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        QueryAggregationMeta meta = new QueryAggregationMeta();
        meta.count = 2;
        // sumElapsedMs is private, so we can't set it directly easily without reflection or adding to it
        // But we can use addFromExecutingQuery or just trust that the getters work.
        // Let's use reflection to set the private fields for a controlled test if needed,
        // but addFromExecutingQuery is better if we have a mock ExecutingQuery.

        // Actually, let's just check if the fields are present in the JSON.
        String json = mapper.writeValueAsString(meta);

        assertThat(json).as("Should contain avgElapsedMs").contains("\"avgElapsedMs\"");
        assertThat(json).as("Should contain avgWaitTimeMs").contains("\"avgWaitTimeMs\"");
        assertThat(json).as("Should contain avgPageHits").contains("\"avgPageHits\"");
        assertThat(json).as("Should contain avgPageFaults").contains("\"avgPageFaults\"");
        assertThat(json).as("Should contain avgAllocatedBytes").contains("\"avgAllocatedBytes\"");
    }
}
