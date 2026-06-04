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
package org.neo4j.server.rest.dbms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.rest.dbms.AuthorizationHeaders.decode;

import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.neo4j.test.server.HTTP;

class AuthorizationHeadersTest {
    @Test
    void shouldParseBasicAuth() {
        // Given
        String username = "jake";
        String password = "qwerty123456";
        String header = HTTP.basicAuthHeader(username, password);

        // When
        var parsed = decode(header);

        // Then
        assertThat(parsed).isNotNull();
        assertThat(parsed.values()[0]).isEqualTo(username);
        assertThat(parsed.values()[1]).isEqualTo(password);
    }

    @Test
    void shouldParseBearerAuth() {
        // Given
        var token = "are-you-token-to-me?";
        String header = HTTP.bearerAuthHeader(token);

        // When
        var parsed = decode(header);

        // Then
        assertThat(parsed).isNotNull();
        assertThat(parsed.values()).hasSize(1);
        assertThat(parsed.values()[0]).isEqualTo(token);
    }

    @Test
    void shouldHandleSadPaths() {
        // When & then
        assertThat(decode("")).isNull();
        assertThat(decode("Basic")).isNull();
        assertThat(decode("Bearer")).isNull();
        assertThat(decode("Basic not valid value")).isNull();
        assertThat(decode("Bearer too many args")).isNull();
        assertThat(decode("Basic " + Base64.getEncoder().encodeToString("".getBytes())))
                .isNull();
        assertThat(decode("Bearer " + Base64.getEncoder().encodeToString("".getBytes())))
                .isNull();
    }
}
