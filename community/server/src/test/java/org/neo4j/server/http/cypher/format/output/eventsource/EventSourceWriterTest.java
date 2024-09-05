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
package org.neo4j.server.http.cypher.format.output.eventsource;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.jsonNode;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.jolt.v1.JoltV1Codec;
import org.neo4j.server.http.cypher.format.output.json.ResultDataContentWriter;
import org.neo4j.server.rest.domain.JsonParseException;

class EventSourceWriterTest {
    @Test
    void shouldWriteSimpleRecord() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonGenerator json = new JoltV1Codec(true).createGenerator(out);

        JsonNode row = serialize(out, json, new EventSourceWriter(), Map.of("value", Map.of("country", "France")));

        assertThat(row.size()).isEqualTo(1);
        JsonNode value = row.get(0).get("{}");
        assertThat(value.get("country").get("U").asText()).isEqualTo("France");
    }

    @Test
    void shouldWriteNestedMaps() throws Exception {
        // Given
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonGenerator json = new JoltV1Codec(true).createGenerator(out);

        Map<String, Object> data = map("ColumnA", map("one", map("two", asList(true, map("three", 42)))));

        // When
        JsonNode record = serialize(out, json, new EventSourceWriter(), data);

        // Then
        assertThat(record.get(0).size()).isEqualTo(1);
        JsonNode value = record.get(0).get("{}");
        assertThat(value.get("one").get("{}").get("two").get("[]").size()).isEqualTo(2);
        assertThat(value.get("one")
                        .get("{}")
                        .get("two")
                        .get("[]")
                        .get(0)
                        .get("?")
                        .asBoolean())
                .isEqualTo(true);
        assertThat(value.get("one")
                        .get("{}")
                        .get("two")
                        .get("[]")
                        .get(1)
                        .get("{}")
                        .get("three")
                        .get("Z")
                        .asInt())
                .isEqualTo(42);
    }

    private static JsonNode serialize(
            ByteArrayOutputStream out,
            JsonGenerator json,
            ResultDataContentWriter resultDataContentWriter,
            Map<String, Object> data)
            throws IOException, JsonParseException {

        RecordEvent recordEvent = new RecordEvent(new ArrayList<>(data.keySet()), data::get);

        resultDataContentWriter.write(json, recordEvent);
        json.flush();
        json.close();

        String jsonAsString = out.toString();
        return jsonNode(jsonAsString);
    }
}
