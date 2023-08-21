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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.formats.JsonFormat;

class MapRepresentationTest {
    @Test
    void shouldSerializeMapWithSimpleTypes() throws Exception {
        MapRepresentation rep =
                new MapRepresentation(map("nulls", null, "strings", "a string", "numbers", 42, "booleans", true));

        String serializedMap =
                RepresentationBasedMessageBodyWriter.serialize(rep, new JsonFormat(), new URI("http://localhost/"));

        Map<String, Object> map = JsonHelper.jsonToMap(serializedMap);
        assertThat(map.get("nulls")).isNull();
        assertThat(map.get("strings")).isEqualTo("a string");
        assertThat(map.get("numbers")).isEqualTo(42);
        assertThat(map.get("booleans")).isEqualTo(true);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldSerializeMapWithArrayTypes() throws Exception {
        MapRepresentation rep = new MapRepresentation(map(
                "strings", new String[] {"a string", "another string"},
                "numbers", new int[] {42, 87},
                "booleans", new boolean[] {true, false},
                "Booleans", new Boolean[] {TRUE, FALSE}));

        String serializedMap =
                RepresentationBasedMessageBodyWriter.serialize(rep, new JsonFormat(), new URI("http://localhost/"));

        Map<String, Object> map = JsonHelper.jsonToMap(serializedMap);
        assertThat(map.get("strings")).isEqualTo(asList("a string", "another string"));
        assertThat(map.get("numbers")).isEqualTo(asList(42, 87));
        assertThat(map.get("booleans")).isEqualTo(asList(true, false));
        assertThat(map.get("Booleans")).isEqualTo(asList(true, false));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldSerializeMapWithListsOfSimpleTypes() throws Exception {
        MapRepresentation rep = new MapRepresentation(map(
                "lists of nulls",
                asList(null, null),
                "lists of strings",
                asList("a string", "another string"),
                "lists of numbers",
                asList(23, 87, 42),
                "lists of booleans",
                asList(true, false, true)));

        String serializedMap =
                RepresentationBasedMessageBodyWriter.serialize(rep, new JsonFormat(), new URI("http://localhost/"));

        Map<String, Object> map = JsonHelper.jsonToMap(serializedMap);
        assertThat(map.get("lists of nulls")).isEqualTo(asList(null, null));
        assertThat(map.get("lists of strings")).isEqualTo(asList("a string", "another string"));
        assertThat(map.get("lists of numbers")).isEqualTo(asList(23, 87, 42));
        assertThat(map.get("lists of booleans")).isEqualTo(asList(true, false, true));
    }

    @Test
    void shouldSerializeMapWithMapsOfSimpleTypes() throws Exception {
        MapRepresentation rep = new MapRepresentation(map(
                "maps with nulls",
                map("nulls", null),
                "maps with strings",
                map("strings", "a string"),
                "maps with numbers",
                map("numbers", 42),
                "maps with booleans",
                map("booleans", true)));

        String serializedMap =
                RepresentationBasedMessageBodyWriter.serialize(rep, new JsonFormat(), new URI("http://localhost/"));

        Map<String, Object> map = JsonHelper.jsonToMap(serializedMap);
        assertThat(((Map) map.get("maps with nulls")).get("nulls")).isNull();
        assertThat(((Map) map.get("maps with strings")).get("strings")).isEqualTo("a string");
        assertThat(((Map) map.get("maps with numbers")).get("numbers")).isEqualTo(42);
        assertThat(((Map) map.get("maps with booleans")).get("booleans")).isEqualTo(true);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldSerializeArbitrarilyNestedMapsAndLists() throws Exception {
        MapRepresentation rep = new MapRepresentation(map(
                "a map with a list in it", map("a list", asList(42, 87)),
                "a list with a map in it", asList(map("foo", "bar", "baz", false))));

        String serializedMap =
                RepresentationBasedMessageBodyWriter.serialize(rep, new JsonFormat(), new URI("http://localhost/"));

        Map<String, Object> map = JsonHelper.jsonToMap(serializedMap);
        assertThat(((Map) map.get("a map with a list in it")).get("a list")).isEqualTo(List.of(42, 87));
        assertThat(((Map) ((List) map.get("a list with a map in it")).get(0)).get("foo"))
                .isEqualTo("bar");
        assertThat(((Map) ((List) map.get("a list with a map in it")).get(0)).get("baz"))
                .isEqualTo(false);
    }

    @Test
    void shouldSerializeMapsWithNullKeys() throws Exception {
        Object[] values = {
            null,
            "string",
            42,
            true,
            new String[] {"a string", "another string"},
            new int[] {42, 87},
            new boolean[] {true, false},
            asList(true, false, true),
            map("numbers", 42, null, "something"),
            map("a list", asList(42, 87), null, asList("a", "b")),
            asList(map("foo", "bar", null, false))
        };

        for (Object value : values) {
            MapRepresentation rep = new MapRepresentation(map((Object) null, value));

            String serializedMap =
                    RepresentationBasedMessageBodyWriter.serialize(rep, new JsonFormat(), new URI("http://localhost/"));

            Map<String, Object> map = JsonHelper.jsonToMap(serializedMap);

            assertEquals(1, map.size());
            Object actual = map.get("null");
            if (value == null) {
                assertNull(actual);
            } else {
                assertNotNull(actual);
            }
        }
    }
}
