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
package org.neo4j.bolt.protocol.v41.message.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;

class RoutingContextTest {

    @Test
    void shouldCompareTwoEqualContexts() {
        Map<String, String> parametersA = new HashMap<>();
        parametersA.put("policy", "europe");
        parametersA.put("speed", "fast");
        RoutingContext contextA = new RoutingContext(true, parametersA);

        Map<String, String> parametersB = new HashMap<>();
        parametersB.put("policy", "europe");
        parametersB.put("speed", "fast");
        RoutingContext contextB = new RoutingContext(true, parametersB);

        assertTrue(contextA.equals(contextB));
        assertEquals(contextA.hashCode(), contextB.hashCode());
    }

    @Test
    void shouldCompareTwoDifferentContexts() {
        Map<String, String> parametersA = new HashMap<>();
        parametersA.put("policy", "europe");
        parametersA.put("speed", "fast");
        RoutingContext contextA = new RoutingContext(true, parametersA);

        Map<String, String> parametersB = new HashMap<>();
        parametersB.put("policy", "asia");
        parametersB.put("speed", "fast");
        RoutingContext contextB = new RoutingContext(true, parametersB);

        assertFalse(contextA.equals(contextB));
        assertNotEquals(contextA.hashCode(), contextB.hashCode());
    }
}
