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
package org.neo4j.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.kernel.api.security.StaticAccessMode.FULL;
import static org.neo4j.internal.kernel.api.security.StaticAccessMode.READ;
import static org.neo4j.messages.MessageUtil.authDisabled;
import static org.neo4j.messages.MessageUtil.createNodeWithLabelsDenied;
import static org.neo4j.messages.MessageUtil.overriddenMode;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.UserFunction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class JavaFunctionsTestIT {
    @Inject
    private TestDirectory testDir;

    public static class MyFunctions {

        @UserFunction
        public long myFunc() {
            return 1337L;
        }

        @UserFunction
        public long funcThatThrows() {
            throw new RuntimeException("This is an exception");
        }
    }

    public static class MyFunctionsUsingMyService {

        @Context
        public SomeService service;

        @UserFunction("my.hello")
        public String hello() {
            return service.hello();
        }
    }

    public static class MyFunctionsUsingMyCoreAPI {
        @Context
        public MyCoreAPI myCoreAPI;

        @Context
        public Transaction transaction;

        @UserFunction(value = "my.willFail")
        public long willFail() throws ProcedureException {
            return myCoreAPI.makeNode(transaction, "Test");
        }

        @UserFunction("my.countNodes")
        public long countNodes() {
            return MyCoreAPI.countNodes(transaction);
        }
    }

    private Neo4jBuilder createServer(Class<?> functionClass) {
        return Neo4jBuilders.newInProcessBuilder().withFunction(functionClass);
    }

    @Test
    void shouldLaunchWithDeclaredFunctions() throws Exception {
        // When
        Class<MyFunctions> functionClass = MyFunctions.class;
        try (Neo4j server = createServer(functionClass).build()) {
            // Then
            HTTP.Response response = HTTP.POST(
                    server.httpURI().resolve("db/neo4j/tx/commit").toString(),
                    quotedJson("{ 'statements': [ { 'statement': 'RETURN org.neo4j.harness.myFunc() AS someNumber' } ] "
                            + "}"));

            JsonNode result = response.get("results").get(0);
            assertThat(result.get("columns").get(0).asText()).isEqualTo("someNumber");
            assertThat(result.get("data").get(0).get("row").get(0).asInt()).isEqualTo(1337);
            assertThat(response.get("errors").toString()).isEqualTo("[]");
        }
    }

    @Test
    void shouldGetHelpfulErrorOnProcedureThrowsException() throws Exception {
        // When
        try (Neo4j server = createServer(MyFunctions.class).build()) {
            // Then
            HTTP.Response response = HTTP.POST(
                    server.httpURI().resolve("db/neo4j/tx/commit").toString(),
                    quotedJson("{ 'statements': [ { 'statement': 'RETURN org.neo4j.harness.funcThatThrows()' } ] }"));

            String error = response.get("errors").get(0).get("message").asText();
            assertThat(error)
                    .isEqualTo(
                            "Failed to invoke function `org.neo4j.harness.funcThatThrows`: Caused by: java.lang.RuntimeException: This is an exception");
        }
    }

    @Test
    void shouldWorkWithInjectableFromExtension() throws Throwable {
        // When
        try (Neo4j server = createServer(MyFunctionsUsingMyService.class).build()) {
            // Then
            HTTP.Response response = HTTP.POST(
                    server.httpURI().resolve("db/neo4j/tx/commit").toString(),
                    quotedJson("{ 'statements': [ { 'statement': 'RETURN my.hello() AS result' } ] }"));

            assertThat(response.get("errors").toString()).isEqualTo("[]");
            JsonNode result = response.get("results").get(0);
            assertThat(result.get("columns").get(0).asText()).isEqualTo("result");
            assertThat(result.get("data").get(0).get("row").get(0).asText()).isEqualTo("world");
        }
    }

    @Test
    void shouldWorkWithInjectableFromExtensionWithMorePower() throws Throwable {
        // When
        try (Neo4j server = createServer(MyFunctionsUsingMyCoreAPI.class).build()) {
            HTTP.POST(
                    server.httpURI().resolve("db/neo4j/tx/commit").toString(),
                    quotedJson("{ 'statements': [ { 'statement': 'CREATE (), (), ()' } ] }"));

            // Then
            assertQueryGetsValue(server, "RETURN my.countNodes() AS value", 3L);
            assertQueryGetsError(
                    server,
                    "RETURN my.willFail() AS value",
                    createNodeWithLabelsDenied("", "neo4j", overriddenMode(authDisabled(FULL.name()), READ.name())));
        }
    }

    private static void assertQueryGetsValue(Neo4j server, String query, long value) throws Throwable {
        HTTP.Response response = HTTP.POST(
                server.httpURI().resolve("db/neo4j/tx/commit").toString(),
                quotedJson("{ 'statements': [ { 'statement': '" + query + "' } ] }"));

        assertThat(response.get("errors").toString()).isEqualTo("[]");
        JsonNode result = response.get("results").get(0);
        assertThat(result.get("columns").get(0).asText()).isEqualTo("value");
        assertThat(result.get("data").get(0).get("row").get(0).asLong()).isEqualTo(value);
    }

    private static void assertQueryGetsError(Neo4j server, String query, String error) throws Throwable {
        HTTP.Response response = HTTP.POST(
                server.httpURI().resolve("db/neo4j/tx/commit").toString(),
                quotedJson("{ 'statements': [ { 'statement': '" + query + "' } ] }"));

        assertThat(response.get("errors").toString()).contains(error);
    }
}
