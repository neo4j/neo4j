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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.RESTRequestGenerator;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.TestData;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.RawPayload;

public class AuthenticationIT extends CommunityWebContainerTestBase {
    @RegisterExtension
    TestData<RESTRequestGenerator> gen = TestData.producedThrough(RESTRequestGenerator.PRODUCER);

    @Test
    @Documented("Missing authorization\n" + "\n"
            + "If an +Authorization+ header is not supplied, the server will reply with an error.")
    void missing_authorization() throws JsonParseException, IOException {
        // Given
        startServerWithConfiguredUser();

        // Document
        RESTRequestGenerator.ResponseEntity response = gen.get()
                .expectedStatus(401)
                .expectedHeader("WWW-Authenticate", "Basic realm=\"Neo4j\", Bearer realm=\"Neo4j\"")
                .get(databaseURL());

        // Then
        JsonNode data = JsonHelper.jsonNode(response.entity());
        JsonNode firstError = data.get("errors").get(0);
        assertThat(firstError.get("code").asText())
                .isEqualTo(Status.Security.Unauthorized.code().serialize());
        assertThat(firstError.get("message").asText()).isEqualTo("No authentication header supplied.");
    }

    @Test
    @Documented("Authenticate to access the server\n" + "\n"
            + "Authenticate by sending a username and a password to Neo4j using HTTP Basic Auth.\n"
            + "Requests should include an +Authorization+ header, with a value of +Basic <payload>+,\n"
            + "where \"payload\" is a base64 encoded string of \"username:password\".")
    void successful_authentication() throws JsonParseException, IOException {
        // Given
        startServerWithConfiguredUser();

        // Then
        HTTP.Response response =
                HTTP.withBasicAuth("neo4j", "secretPassword").POST(txCommitURL("system"), query("SHOW USERS"));

        assertThat(response.status()).isEqualTo(200);

        final JsonNode jsonNode = getResultRow(response);
        assertThat(jsonNode.get(0).asText()).isEqualTo("neo4j");
        assertThat(jsonNode.get(1).asBoolean()).isEqualTo(false);
    }

    @Test
    @Documented("Incorrect authentication\n" + "\n"
            + "If an incorrect username or password is provided, the server replies with an error.")
    void incorrect_authentication() throws JsonParseException, IOException {
        // Given
        startServerWithConfiguredUser();

        // Document
        RESTRequestGenerator.ResponseEntity response = gen.get()
                .expectedStatus(401)
                .withHeader(HttpHeaders.AUTHORIZATION, HTTP.basicAuthHeader("neo4j", "incorrect"))
                .expectedHeader("WWW-Authenticate", "Basic realm=\"Neo4j\", Bearer realm=\"Neo4j\"")
                .post(databaseURL());

        // Then
        JsonNode data = JsonHelper.jsonNode(response.entity());
        JsonNode firstError = data.get("errors").get(0);
        assertThat(firstError.get("code").asText())
                .isEqualTo(Status.Security.Unauthorized.code().serialize());
        assertThat(firstError.get("message").asText()).isEqualTo("Invalid credential.");
    }

    @Test
    @Documented("Required password changes\n" + "\n"
            + "In some cases, like the very first time Neo4j is accessed, the user will be required to choose\n"
            + "a new password. The database will signal that a new password is required and deny access.\n"
            + "\n"
            + "See <<rest-api-security-user-status-and-password-changing>> for how to set a new password.")
    void password_change_required() throws JsonParseException, IOException {
        // Given
        startServer(true);

        // It should be possible to authenticate with password change required
        gen.get().expectedStatus(200).withHeader(HttpHeaders.AUTHORIZATION, HTTP.basicAuthHeader("neo4j", "neo4j"));

        // When
        HTTP.Response responseBeforePasswordChange =
                HTTP.withBasicAuth("neo4j", "neo4j").POST(txCommitURL("system"), query("SHOW USERS"));

        // Then
        // The server should throw error when trying to do something else than changing password
        assertPermissionErrorAtSystemAccess(responseBeforePasswordChange);

        // When
        // Changing the user password
        HTTP.Response response = HTTP.withBasicAuth("neo4j", "neo4j")
                .POST(txCommitURL("system"), query("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secretPassword'"));
        // Then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.get("errors").size()).as("Should have no errors").isEqualTo(0);

        // When
        HTTP.Response responseAfterPasswordChange =
                HTTP.withBasicAuth("neo4j", "secretPassword").POST(txCommitURL("system"), query("SHOW USERS"));

        // Then
        assertThat(responseAfterPasswordChange.status()).isEqualTo(200);
        assertThat(response.get("errors").size()).as("Should have no errors").isEqualTo(0);
    }

    @Test
    void shouldSayMalformedHeaderIfMalformedAuthorization() throws Exception {
        // Given
        startServerWithConfiguredUser();

        // When
        HTTP.Response response = HTTP.withHeaders(HttpHeaders.AUTHORIZATION, "This makes no sense")
                .GET(databaseURL());

        // Then
        assertThat(response.status()).isEqualTo(400);
        assertThat(response.get("errors").get(0).get("code").asText())
                .isEqualTo(Status.Request.InvalidFormat.code().serialize());
        assertThat(response.get("errors").get(0).get("message").asText()).isEqualTo("Invalid authentication header.");
    }

    @Test
    void shouldAllowDataAccess() throws Exception {
        // Given
        startServerWithConfiguredUser();

        // When & then
        assertAuthorizationRequired(
                "POST",
                txCommitEndpoint(),
                RawPayload.quotedJson("{'statements':[{'statement':'MATCH (n) RETURN n'}]}"),
                200);
        assertAuthorizationRequired("GET", "db/data/nowhere", null, 404);

        assertEquals(
                200,
                HTTP.GET(testWebContainer.getBaseUri().resolve("").toString()).status());
    }

    @Test
    void should404IfEndpointDoesNotExist() throws Exception {
        // Given
        startServerWithConfiguredUser();

        assertEquals(
                404,
                HTTP.GET(testWebContainer
                                .getBaseUri()
                                .resolve("this/does/not/exist")
                                .toString())
                        .status());
    }

    @Test
    void shouldNotAllowAnotherUserToAccessTransaction() throws Exception {
        // Given
        startServerWithConfiguredUser();
        setupBobAndAliceUsers();

        // When Bob creates a transaction
        HTTP.Response initiatingUserRequest = HTTP.withBasicAuth("bob", "secretPassword")
                .POST(testWebContainer.getBaseUri().resolve(txEndpoint()).toString(), query("CREATE (n)"));
        assertEquals(201, initiatingUserRequest.status());

        // Then alice cannot access that transaction
        HTTP.Response hijackingUserRequest = HTTP.withBasicAuth("alice", "secretPassword")
                .POST(initiatingUserRequest.location(), query("CREATE (n)"));
        assertEquals(404, hijackingUserRequest.status());
        assertThat(hijackingUserRequest.get("errors").get(0).get("code").asText())
                .isEqualTo(Status.Transaction.TransactionNotFound.code().serialize());

        // And bob can still commit it
        HTTP.Response initiatingUserCommitRequest = HTTP.withBasicAuth("bob", "secretPassword")
                .POST(initiatingUserRequest.location() + "/commit", query("CREATE (n)"));
        assertEquals(200, initiatingUserCommitRequest.status());
    }

    @Test
    void shouldNotAllowAnotherUserToCommitTransaction() throws Exception {
        // Given
        startServerWithConfiguredUser();
        setupBobAndAliceUsers();

        // When Bob creates a transaction
        HTTP.Response initiatingUserRequest = HTTP.withBasicAuth("bob", "secretPassword")
                .POST(testWebContainer.getBaseUri().resolve(txEndpoint()).toString(), query("CREATE (n)"));
        assertEquals(201, initiatingUserRequest.status());

        // Then alice cannot commit that transaction
        HTTP.Response hijackingUserRequest =
                HTTP.withBasicAuth("alice", "secretPassword").POST(initiatingUserRequest.location() + "/commit");
        assertEquals(404, hijackingUserRequest.status());
        assertThat(hijackingUserRequest.get("errors").get(0).get("code").asText())
                .isEqualTo(Status.Transaction.TransactionNotFound.code().serialize());

        // And bob can still commit it
        HTTP.Response initiatingUserCommitRequest = HTTP.withBasicAuth("bob", "secretPassword")
                .POST(initiatingUserRequest.location() + "/commit", query("CREATE (n)"));
        assertEquals(200, initiatingUserCommitRequest.status());
    }

    @Test
    void shouldNotAllowAnotherUserToRollbackTransaction() throws Exception {
        // Given
        startServerWithConfiguredUser();
        setupBobAndAliceUsers();

        // When Bob creates a transaction
        HTTP.Response initiatingUserRequest = HTTP.withBasicAuth("bob", "secretPassword")
                .POST(testWebContainer.getBaseUri().resolve(txEndpoint()).toString(), query("CREATE (n)"));
        assertEquals(201, initiatingUserRequest.status());

        // Then alice cannot rollback that transaction
        HTTP.Response hijackingUserRequest =
                HTTP.withBasicAuth("alice", "secretPassword").DELETE(initiatingUserRequest.location());
        assertEquals(404, hijackingUserRequest.status());
        assertThat(hijackingUserRequest.get("errors").get(0).get("code").asText())
                .isEqualTo(Status.Transaction.TransactionNotFound.code().serialize());

        // And bob can still commit it
        HTTP.Response initiatingUserCommitRequest = HTTP.withBasicAuth("bob", "secretPassword")
                .POST(initiatingUserRequest.location() + "/commit", query("CREATE (n)"));
        assertEquals(200, initiatingUserCommitRequest.status());
    }

    @Test
    void shouldAllowAllAccessIfAuthenticationIsDisabled() throws Exception {
        // Given
        startServer(false);

        // When & then
        assertEquals(
                200,
                HTTP.POST(txCommitURL(), RawPayload.quotedJson("{'statements':[{'statement':'MATCH (n) RETURN n'}]}"))
                        .status());
        assertEquals(
                404,
                HTTP.GET(testWebContainer
                                .getBaseUri()
                                .resolve("db/data/nowhere")
                                .toString())
                        .status());
    }

    @Test
    void shouldReplyNicelyToTooManyFailedAuthAttempts() throws Exception {
        // Given
        startServerWithConfiguredUser();
        long timeout = System.currentTimeMillis() + 30_000;

        // When
        HTTP.Response response = null;
        while (System.currentTimeMillis() < timeout) {
            // Done in a loop because we're racing with the clock to get enough failed requests into 5 seconds
            response = HTTP.withBasicAuth("neo4j", "incorrect")
                    .POST(txCommitURL(), RawPayload.quotedJson("{'statements':[{'statement':'MATCH (n) RETURN n'}]}"));

            if (response.status() == 429) {
                break;
            }
        }

        // Then
        assertNotNull(response);
        assertThat(response.status()).isEqualTo(429);
        JsonNode firstError = response.get("errors").get(0);
        assertThat(firstError.get("code").asText())
                .isEqualTo(Status.Security.AuthenticationRateLimit.code().serialize());
        assertThat(firstError.get("message").asText())
                .isEqualTo("Too many failed authentication requests. Please wait 5 seconds and try again.");
    }

    @Test
    void shouldNotAllowDataAccessWhenPasswordChangeRequired() throws Exception {
        // Given
        startServer(true); // The user should not have read access before changing the password

        // When
        final HTTP.Response response = HTTP.withBasicAuth("neo4j", "neo4j")
                .POST(
                        testWebContainer.getBaseUri().resolve(txCommitURL()).toString(),
                        RawPayload.quotedJson("{'statements':[{'statement':'MATCH (n) RETURN n'}]}"));

        // Then
        assertPermissionErrorAtDataAccess(response);
    }

    private void assertAuthorizationRequired(String method, String path, Object payload, int expectedAuthorizedStatus)
            throws JsonParseException {
        // When no header
        HTTP.Response response =
                HTTP.request(method, testWebContainer.getBaseUri().resolve(path).toString(), payload);
        assertThat(response.status()).isEqualTo(401);
        assertThat(response.get("errors").get(0).get("code").asText())
                .isEqualTo(Status.Security.Unauthorized.code().serialize());
        assertThat(response.get("errors").get(0).get("message").asText())
                .isEqualTo("No authentication header supplied.");
        assertThat(response.header(HttpHeaders.WWW_AUTHENTICATE))
                .isEqualTo("Basic realm=\"Neo4j\", Bearer realm=\"Neo4j\"");

        // When malformed header
        response = HTTP.withHeaders(HttpHeaders.AUTHORIZATION, "This makes no sense")
                .request(method, testWebContainer.getBaseUri().resolve(path).toString(), payload);
        assertThat(response.status()).isEqualTo(400);
        assertThat(response.get("errors").get(0).get("code").asText())
                .isEqualTo(Status.Request.InvalidFormat.code().serialize());
        assertThat(response.get("errors").get(0).get("message").asText()).isEqualTo("Invalid authentication header.");

        // When invalid credential
        response = HTTP.withBasicAuth("neo4j", "incorrect")
                .request(method, testWebContainer.getBaseUri().resolve(path).toString(), payload);
        assertThat(response.status()).isEqualTo(401);
        assertThat(response.get("errors").get(0).get("code").asText())
                .isEqualTo(Status.Security.Unauthorized.code().serialize());
        assertThat(response.get("errors").get(0).get("message").asText()).isEqualTo("Invalid credential.");
        assertThat(response.header(HttpHeaders.WWW_AUTHENTICATE))
                .isEqualTo("Basic realm=\"Neo4j\", Bearer realm=\"Neo4j\"");

        // When authorized
        response = HTTP.withBasicAuth("neo4j", "secretPassword")
                .request(method, testWebContainer.getBaseUri().resolve(path).toString(), payload);
        assertThat(response.status()).isEqualTo(expectedAuthorizedStatus);
    }

    protected void startServerWithConfiguredUser() throws IOException {
        startServer(true);
        // Set the password
        HTTP.Response post = HTTP.withBasicAuth("neo4j", "neo4j")
                .POST(txCommitURL("system"), query("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secretPassword'"));
        assertEquals(200, post.status());
    }

    private void setupBobAndAliceUsers() {
        HTTP.Response createBobRequest = HTTP.withBasicAuth("neo4j", "secretPassword")
                .POST(
                        txCommitURL("system"),
                        query("CREATE USER bob SET PASSWORD 'secretPassword' " + "SET PASSWORD CHANGE NOT REQUIRED"));
        assertEquals(200, createBobRequest.status());
        HTTP.Response createBobPermissions = HTTP.withBasicAuth("neo4j", "secretPassword")
                .POST(txCommitURL("system"), query("GRANT ROLE admin to bob"));
        assertEquals(200, createBobPermissions.status());
        HTTP.Response createAliceRequest = HTTP.withBasicAuth("neo4j", "secretPassword")
                .POST(
                        txCommitURL("system"),
                        query("CREATE USER alice SET PASSWORD 'secretPassword' " + "SET PASSWORD CHANGE NOT REQUIRED"));
        assertEquals(200, createAliceRequest.status());
    }

    private static JsonNode getResultRow(HTTP.Response response) throws JsonParseException {
        return response.get("results").get(0).get("data").get(0).get("row");
    }
}
