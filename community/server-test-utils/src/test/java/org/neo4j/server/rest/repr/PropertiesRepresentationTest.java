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
package org.neo4j.server.rest.repr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.server.rest.repr.RepresentationTestAccess.serialize;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Entity;

class PropertiesRepresentationTest {
    @Test
    void shouldContainAddedPropertiesWhenCreatedFromEntity() {
        Map<String, Object> values = new HashMap<>();
        values.put("foo", "bar");
        Map<String, Object> serialized = serialize(new PropertiesRepresentation(container(values)));
        assertThat(serialized).containsEntry("foo", "bar");
    }

    @Test
    void shouldSerializeToMapWithSamePropertiesWhenCreatedFromEntity() {
        Map<String, Object> values = new HashMap<>();
        values.put("foo", "bar");
        PropertiesRepresentation properties = new PropertiesRepresentation(container(values));
        Map<String, Object> map = serialize(properties);
        assertThat(map).containsExactlyEntriesOf(values);
    }

    @Test
    void shouldSerializeToMap() {
        Map<String, Object> values = new HashMap<>();
        values.put("string", "value");
        values.put("int", 5);
        values.put("long", 17L);
        values.put("double", 3.14);
        values.put("float", 42.0f);
        values.put("string array", new String[] {"one", "two"});
        values.put("long array", new long[] {5L, 17L});
        values.put("double array", new double[] {3.14, 42.0});

        PropertiesRepresentation properties = new PropertiesRepresentation(container(values));
        Map<String, Object> map = serialize(properties);

        assertThat(map.get("string")).isEqualTo("value");
        assertThat(((Number) map.get("int")).longValue()).isEqualTo(5L);
        assertThat(((Number) map.get("long")).longValue()).isEqualTo(17L);
        assertThat(((Number) map.get("double")).doubleValue()).isEqualTo(3.14);
        assertThat(((Number) map.get("float")).doubleValue()).isEqualTo(42.0);
        assertThat(map.get("string array")).asList().containsExactly("one", "two");
        assertThat(map.get("long array")).asList().containsExactly(5L, 17L);
        assertThat(map.get("double array")).asList().containsExactly(3.14, 42.0);
    }

    @Test
    void shouldBeAbleToSignalEmptiness() {
        PropertiesRepresentation properties = new PropertiesRepresentation(container(new HashMap<>()));
        Map<String, Object> values = new HashMap<>();
        values.put("key", "value");
        assertThat(properties.isEmpty()).isTrue();
        properties = new PropertiesRepresentation(container(values));
        assertThat(properties.isEmpty()).isFalse();
    }

    static Entity container(Map<String, Object> values) {
        Entity container = mock(Entity.class);
        when(container.getPropertyKeys()).thenReturn(values.keySet());
        when(container.getAllProperties()).thenReturn(values);
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            when(container.getProperty(entry.getKey(), null)).thenReturn(entry.getValue());
        }
        return container;
    }
}
