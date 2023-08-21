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
import static org.neo4j.kernel.api.exceptions.Status.General.UnknownError;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.formats.MapWrappingWriter;

class ExceptionRepresentationTest {
    @Test
    void shouldIncludeCause() throws Exception {
        // Given
        ExceptionRepresentation rep = new ExceptionRepresentation(
                new RuntimeException("Hoho", new RuntimeException("Haha", new RuntimeException("HAHA!"))));

        // When
        JsonNode out = serialize(rep);

        // Then
        assertThat(out.get("cause").get("message").asText()).isEqualTo("Haha");
        assertThat(out.get("cause").get("cause").get("message").asText()).isEqualTo("HAHA!");
    }

    @Test
    void shouldRenderErrorsWithNeo4jStatusCode() throws Exception {
        // Given
        ExceptionRepresentation rep = new ExceptionRepresentation(new KernelException(UnknownError, "Hello") {});

        // When
        JsonNode out = serialize(rep);

        // Then
        assertThat(out.get("errors").get(0).get("code").asText()).isEqualTo("Neo.DatabaseError.General.UnknownError");
        assertThat(out.get("errors").get(0).get("message").asText()).isEqualTo("Hello");
    }

    @Test
    void shouldExcludeLegacyFormatIfAsked() throws Exception {
        // Given
        ExceptionRepresentation rep =
                new ExceptionRepresentation(new KernelException(UnknownError, "Hello") {}, /*legacy*/ false);

        // When
        JsonNode out = serialize(rep);

        // Then
        assertThat(out.get("errors").get(0).get("code").asText()).isEqualTo("Neo.DatabaseError.General.UnknownError");
        assertThat(out.get("errors").get(0).get("message").asText()).isEqualTo("Hello");
        assertThat(out.has("message")).isEqualTo(false);
    }

    private static JsonNode serialize(ExceptionRepresentation rep) throws JsonParseException {
        Map<String, Object> output = new HashMap<>();
        MappingSerializer serializer = new MappingSerializer(new MapWrappingWriter(output), URI.create(""));

        // When
        rep.serialize(serializer);
        return JsonHelper.jsonNode(JsonHelper.createJsonFrom(output));
    }
}
