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
package org.neo4j.server.rest.security;

import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.server.rest.web.CorsFilter.ACCESS_CONTROL_ALLOW_HEADERS;
import static org.neo4j.server.rest.web.CorsFilter.ACCESS_CONTROL_ALLOW_METHODS;
import static org.neo4j.server.rest.web.CorsFilter.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.neo4j.server.rest.web.CorsFilter.ACCESS_CONTROL_REQUEST_HEADERS;
import static org.neo4j.server.rest.web.CorsFilter.ACCESS_CONTROL_REQUEST_METHOD;
import static org.neo4j.server.web.HttpMethod.DELETE;
import static org.neo4j.server.web.HttpMethod.GET;
import static org.neo4j.server.web.HttpMethod.PATCH;
import static org.neo4j.server.web.HttpMethod.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

import org.junit.jupiter.api.Test;
import org.neo4j.server.web.HttpMethod;
import org.neo4j.test.server.HTTP;

class AuthorizationCorsIT extends CommunityWebContainerTestBase {
    @Test
    void shouldAddCorsHeaderWhenAuthDisabled() throws Exception {
        startServer(false);

        HTTP.Response response = runQuery("authDisabled", "authDisabled");

        assertEquals(OK.getStatusCode(), response.status());
        assertCorsHeaderPresent(response);
        assertThat(response.content().toString()).contains("42");
    }

    @Test
    void shouldAddCorsHeaderWhenAuthEnabledAndPasswordChangeRequired() throws Exception {
        startServer(true);

        HTTP.Response response = runQuery("neo4j", "neo4j");

        assertEquals(OK.getStatusCode(), response.status());
        assertCorsHeaderPresent(response);
        assertPermissionErrorAtDataAccess(response);
    }

    @Test
    void shouldAddCorsHeaderWhenAuthEnabledAndPasswordChangeNotRequired() throws Exception {
        startServer(true);
        HTTP.Response passwordChangeResponse = changePassword("neo4j", "neo4j", "newPassword");
        assertEquals(OK.getStatusCode(), passwordChangeResponse.status());
        assertCorsHeaderPresent(passwordChangeResponse);

        HTTP.Response queryResponse = runQuery("neo4j", "newPassword");

        assertEquals(OK.getStatusCode(), queryResponse.status());
        assertCorsHeaderPresent(queryResponse);
        assertThat(queryResponse.content().toString()).contains("42");
    }

    @Test
    void shouldAddCorsHeaderWhenAuthEnabledAndIncorrectPassword() throws Exception {
        startServer(true);

        HTTP.Response response = runQuery("neo4j", "wrongPassword");

        assertEquals(UNAUTHORIZED.getStatusCode(), response.status());
        assertCorsHeaderPresent(response);
        assertThat(response.content().toString()).contains("Neo.ClientError.Security.Unauthorized");
    }

    @Test
    void shouldAddCorsMethodsHeader() throws Exception {
        startServer(false);

        testCorsAllowMethods(POST);
        testCorsAllowMethods(GET);
        testCorsAllowMethods(PATCH);
        testCorsAllowMethods(DELETE);
    }

    @Test
    void shouldAddCorsHeaderWhenConfigured() throws Exception {
        String origin = "https://example.com:7687";
        startServer(false, origin);

        testCorsAllowMethods(POST, origin);
        testCorsAllowMethods(GET, origin);
        testCorsAllowMethods(PATCH, origin);
        testCorsAllowMethods(DELETE, origin);
    }

    @Test
    void shouldAddCorsRequestHeaders() throws Exception {
        startServer(false);

        HTTP.Builder requestBuilder = HTTP.withBasicAuth("authDisabled", "authDisabled")
                .withHeaders(ACCESS_CONTROL_REQUEST_HEADERS, "Accept, X-Not-Accept");
        HTTP.Response response = runQuery(requestBuilder);

        assertEquals(OK.getStatusCode(), response.status());
        assertCorsHeaderPresent(response);
        assertEquals("Accept, X-Not-Accept", response.header(ACCESS_CONTROL_ALLOW_HEADERS));
    }

    private void testCorsAllowMethods(HttpMethod method) {
        testCorsAllowMethods(method, "*");
    }

    private void testCorsAllowMethods(HttpMethod method, String origin) {
        HTTP.Builder requestBuilder = HTTP.withBasicAuth("authDisabled", "authDisabled")
                .withHeaders(ACCESS_CONTROL_REQUEST_METHOD, method.toString());
        HTTP.Response response = runQuery(requestBuilder);

        assertEquals(OK.getStatusCode(), response.status());
        assertCorsHeaderEquals(response, origin);
        assertEquals(method, HttpMethod.valueOf(response.header(ACCESS_CONTROL_ALLOW_METHODS)));
    }

    private HTTP.Response changePassword(String username, String oldPassword, String newPassword) {
        HTTP.RawPayload passwordChange =
                query(String.format("ALTER CURRENT USER SET PASSWORD FROM '%s' TO '%s'", oldPassword, newPassword));
        return HTTP.withBasicAuth(username, oldPassword).POST(txCommitURL("system"), passwordChange);
    }

    private HTTP.Response runQuery(String username, String password) {
        return runQuery(HTTP.withBasicAuth(username, password));
    }

    private HTTP.Response runQuery(HTTP.Builder requestBuilder) {
        HTTP.RawPayload statements = quotedJson("{'statements': [{'statement': 'RETURN 42'}]}");
        return requestBuilder.POST(txCommitURL(), statements);
    }

    private static void assertCorsHeaderPresent(HTTP.Response response) {
        assertCorsHeaderEquals(response, "*");
    }

    private static void assertCorsHeaderEquals(HTTP.Response response, String origin) {
        assertEquals(origin, response.header(ACCESS_CONTROL_ALLOW_ORIGIN));
    }
}
