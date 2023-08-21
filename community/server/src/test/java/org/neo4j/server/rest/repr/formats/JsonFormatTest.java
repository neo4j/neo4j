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
package org.neo4j.server.rest.repr.formats;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationBasedMessageBodyWriter;
import org.neo4j.server.rest.repr.RepresentationType;
import org.neo4j.server.rest.repr.ServerListRepresentation;
import org.neo4j.server.rest.repr.ValueRepresentation;

class JsonFormatTest {
    private JsonFormat json;

    @BeforeEach
    void createOutputFormat() {
        json = new JsonFormat();
    }

    @Test
    void canFormatString() throws URISyntaxException {
        String entity = RepresentationBasedMessageBodyWriter.serialize(
                ValueRepresentation.string("expected value"), json, new URI("http://localhost/"));
        assertEquals("\"expected value\"", entity);
    }

    @Test
    void canFormatListOfStrings() throws URISyntaxException {
        String entity = RepresentationBasedMessageBodyWriter.serialize(
                ListRepresentation.strings("hello", "world"), json, new URI("http://localhost/"));
        String expectedString = createJsonFrom(Arrays.asList("hello", "world"));
        assertEquals(expectedString, entity);
    }

    @Test
    void canFormatInteger() throws URISyntaxException {
        String entity = RepresentationBasedMessageBodyWriter.serialize(
                ValueRepresentation.number(10), json, new URI("http://localhost/"));
        assertEquals("10", entity);
    }

    @Test
    void canFormatEmptyObject() throws URISyntaxException {
        String entity = RepresentationBasedMessageBodyWriter.serialize(
                new MappingRepresentation("empty") {
                    @Override
                    protected void serialize(MappingSerializer serializer) {}
                },
                json,
                new URI("http://localhost/"));
        assertEquals(createJsonFrom(Collections.emptyMap()), entity);
    }

    @Test
    void canFormatObjectWithStringField() throws URISyntaxException {
        String entity = RepresentationBasedMessageBodyWriter.serialize(
                new MappingRepresentation("string") {
                    @Override
                    protected void serialize(MappingSerializer serializer) {
                        serializer.putString("key", "expected string");
                    }
                },
                json,
                new URI("http://localhost/"));
        assertEquals(createJsonFrom(singletonMap("key", "expected string")), entity);
    }

    @Test
    void canFormatObjectWithUriField() throws URISyntaxException {
        String entity = RepresentationBasedMessageBodyWriter.serialize(
                new MappingRepresentation("uri") {
                    @Override
                    protected void serialize(MappingSerializer serializer) {
                        serializer.putRelativeUri("URL", "subpath");
                    }
                },
                json,
                new URI("http://localhost/"));

        assertEquals(createJsonFrom(singletonMap("URL", "http://localhost/subpath")), entity);
    }

    @Test
    void canFormatObjectWithNestedObject() throws URISyntaxException {
        String entity = RepresentationBasedMessageBodyWriter.serialize(
                new MappingRepresentation("nesting") {
                    @Override
                    protected void serialize(MappingSerializer serializer) {
                        serializer.putMapping("nested", new MappingRepresentation("data") {
                            @Override
                            protected void serialize(MappingSerializer nested) {
                                nested.putString("data", "expected data");
                            }
                        });
                    }
                },
                json,
                new URI("http://localhost/"));
        assertEquals(createJsonFrom(singletonMap("nested", singletonMap("data", "expected data"))), entity);
    }

    @Test
    void canFormatNestedMapsAndLists() throws Exception {
        String entity = RepresentationBasedMessageBodyWriter.serialize(
                new MappingRepresentation("test") {
                    @Override
                    protected void serialize(MappingSerializer serializer) {
                        List<Representation> maps = new ArrayList<>();
                        maps.add(new MappingRepresentation("map") {

                            @Override
                            protected void serialize(MappingSerializer serializer) {
                                serializer.putString("foo", "bar");
                            }
                        });
                        serializer.putList("foo", new ServerListRepresentation(RepresentationType.MAP, maps));
                    }
                },
                json,
                new URI("http://localhost/"));

        assertEquals("bar", ((Map) ((List) JsonHelper.jsonToMap(entity).get("foo")).get(0)).get("foo"));
    }
}
