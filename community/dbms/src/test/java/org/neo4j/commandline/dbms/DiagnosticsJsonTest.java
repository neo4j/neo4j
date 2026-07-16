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
package org.neo4j.commandline.dbms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.diagnostics.DiagnosticsQueryResult;

class DiagnosticsJsonTest {
    @Test
    void rendersEmptyResultAsEmptyArray() {
        var result = new DiagnosticsQueryResult(List.of("section", "data"), List.of());
        assertThat(DiagnosticsJson.toJson(result)).isEqualTo("[]\n");
    }

    @Test
    void rendersNestedMapsAndListsStructurally() {
        // Mimics the shape returned by db.stats.retrieve('GRAPH COUNTS').
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodes", List.of(Map.of("count", 171L), orderedMap("count", 38L, "label", "Movie")));
        Map<String, Object> row = orderedMap("section", "GRAPH COUNTS", "data", data);

        String json = DiagnosticsJson.toJson(new DiagnosticsQueryResult(List.of("section", "data"), List.of(row)));

        assertThat(json)
                .contains("\"section\": \"GRAPH COUNTS\"")
                .contains("\"nodes\": [")
                .contains("\"count\": 171") // numbers are unquoted
                .contains("\"label\": \"Movie\"");
        // None of the Java map/list toString artifacts that a naive String rendering would produce.
        assertThat(json).doesNotContain("count=").doesNotContain("{count=171}").doesNotContain("nodes=[");
    }

    @Test
    void escapesStringsAndRendersNull() {
        Map<String, Object> row = orderedMap("name", "a\"b\\c\nd", "value", null);
        String json = DiagnosticsJson.toJson(new DiagnosticsQueryResult(List.of("name", "value"), List.of(row)));

        assertThat(json).contains("\"name\": \"a\\\"b\\\\c\\nd\"").contains("\"value\": null");
    }

    private static Map<String, Object> orderedMap(Object... keysAndValues) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }
}
