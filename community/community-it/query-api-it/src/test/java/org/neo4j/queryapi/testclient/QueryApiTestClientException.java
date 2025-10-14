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
package org.neo4j.queryapi.testclient;

import static org.neo4j.server.queryapi.response.format.Fieldnames.ERROR_CODE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.ERROR_MESSAGE;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;
import org.assertj.core.api.Assertions;

public class QueryApiTestClientException extends Exception {
    private final String code;

    QueryApiTestClientException(String message, String code) {
        super(message);
        this.code = code;
    }

    /**
     * Transforms the JsonNode with the errors list into a {@link QueryApiTestClientException}
     *
     * If there is more than one exception, the extra ones will be added as suppressed exceptions.
     */
    static QueryApiTestClientException ofErrors(JsonNode errors) {
        Assertions.assertThat(errors).isNotNull();
        Assertions.assertThat(errors).isNotEmpty();

        var error = ofError(errors.get(0));

        for (var i = 1; i < errors.size(); i++) {
            var suppressed = ofError(errors.get(i));
            error.addSuppressed(suppressed);
        }

        return error;
    }

    static QueryApiTestClientException ofError(JsonNode error) {
        Assertions.assertThat(error).isNotNull();
        var code =
                Optional.ofNullable(error.get(ERROR_CODE)).map(JsonNode::asText).orElse(null);
        var errorMessage = Optional.ofNullable(error.get(ERROR_MESSAGE))
                .map(JsonNode::asText)
                .orElse(null);

        Assertions.assertThat(code).isNotNull().isNotBlank();
        Assertions.assertThat(errorMessage).isNotNull().isNotBlank();
        return new QueryApiTestClientException(errorMessage, code);
    }

    public String getCode() {
        return code;
    }
}
