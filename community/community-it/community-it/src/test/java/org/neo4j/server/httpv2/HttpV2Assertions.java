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
package org.neo4j.server.httpv2;

import static org.neo4j.server.httpv2.response.format.Fieldnames.CYPHER_TYPE;
import static org.neo4j.server.httpv2.response.format.Fieldnames.CYPHER_VALUE;
import static org.neo4j.server.httpv2.response.format.Fieldnames.DATA_KEY;
import static org.neo4j.server.httpv2.response.format.Fieldnames.VALUES_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

final class HttpV2Assertions extends AbstractAssert<HttpV2Assertions, JsonNode> {

    private final JsonNode jsonNode;

    protected HttpV2Assertions(JsonNode jsonNode) {
        super(jsonNode, HttpV2Assertions.class);
        this.jsonNode = jsonNode;
    }

    static HttpV2Assertions assertThat(JsonNode jsonNode) {
        return new HttpV2Assertions(jsonNode);
    }

    HttpV2Assertions hasTypedResultAt(int index, String expectedType, String expectedValue) {
        Assertions.assertThat(jsonNode.get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(index)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo(expectedType);
        Assertions.assertThat(jsonNode.get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(index)
                        .get(CYPHER_VALUE)
                        .asText())
                .isEqualTo(expectedValue);
        return this;
    }

    HttpV2Assertions hasTypedResult(String expectedType, String expectedValue) {
        Assertions.assertThat(jsonNode.get(CYPHER_TYPE).asText()).isEqualTo(expectedType);
        Assertions.assertThat(jsonNode.get(CYPHER_VALUE).asText()).isEqualTo(expectedValue);
        return this;
    }
}
