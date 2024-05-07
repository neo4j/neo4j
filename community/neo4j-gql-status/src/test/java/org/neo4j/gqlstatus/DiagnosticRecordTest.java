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
package org.neo4j.gqlstatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DiagnosticRecordTest {
    @Test
    void shouldHaveExpectedKeys() {
        Map<String, Object> diagnosticRecordMap = new DiagnosticRecord("", "", 0, 0, 0, Map.of()).asMap();
        Set<String> expectedKeys = Set.of(
                "OPERATION",
                "OPERATION_CODE",
                "CURRENT_SCHEMA",
                "_severity",
                "_classification",
                "_position",
                "_status_parameters");

        assertEquals(expectedKeys, diagnosticRecordMap.keySet());
    }

    @Test
    void shouldHaveExpectedDefaultValues() {
        Map<String, Object> diagnosticRecordMap = new DiagnosticRecord("", "", 0, 0, 0, Map.of()).asMap();
        assertEquals("/", diagnosticRecordMap.get("CURRENT_SCHEMA"));
        assertEquals("", diagnosticRecordMap.get("OPERATION"));
        assertEquals(0, diagnosticRecordMap.get("OPERATION_CODE"));
    }

    @Test
    void shouldConstructProperPositionMap() {
        Map<String, Object> diagnosticRecordMap = new DiagnosticRecord("", "", 1, 2, 3, Map.of()).asMap();
        assertInstanceOf(Map.class, diagnosticRecordMap.get("_position"));

        @SuppressWarnings("unchecked")
        Map<String, Object> position = (Map<String, Object>) diagnosticRecordMap.get("_position");

        assertEquals(1, position.get("offset"));
        assertEquals(2, position.get("line"));
        assertEquals(3, position.get("column"));
    }
}
