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
package org.neo4j.server.rest;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.TestData;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.SharedWebContainerTestBase;

public class AbstractRestFunctionalTestBase extends SharedWebContainerTestBase implements GraphHolder {
    protected final HTTP.Builder http = HTTP.withBaseUri(container().getBaseUri());
    protected static final String TX_ENDPOINT = "db/neo4j/tx";

    @RegisterExtension
    TestData<Map<String, Node>> data = TestData.producedThrough(GraphDescription.createGraphFor(this));

    @RegisterExtension
    protected TestData<RESTRequestGenerator> gen = TestData.producedThrough(RESTRequestGenerator.PRODUCER);

    @Override
    public GraphDatabaseService graphdb() {
        return container().getDefaultDatabase();
    }

    public <T> T resolveDependency(Class<T> cls) {
        return ((GraphDatabaseAPI) graphdb()).getDependencyResolver().resolveDependency(cls);
    }

    private static String defaultDatabaseUri() {
        return databaseUri("neo4j");
    }

    private static String databaseUri(String databaseName) {
        return databaseUri(getLocalHttpPort(), databaseName);
    }

    private static String databaseUri(int port, String databaseName) {
        return String.format("http://localhost:%s/db/%s/", port, databaseName);
    }

    protected static String dbUri() {
        return "http://localhost:" + getLocalHttpPort() + "/db/";
    }

    protected static String txUri() {
        return defaultDatabaseUri() + "tx";
    }

    protected static String txUri(String databaseName) {
        return databaseUri(databaseName) + "tx";
    }

    protected static String txCommitUri() {
        return defaultDatabaseUri() + "tx/commit";
    }

    public static String txCommitUri(String databaseName) {
        return databaseUri(databaseName) + "tx/commit";
    }

    public static String txCommitUri(String databaseName, int port) {
        return databaseUri(port, databaseName) + "tx/commit";
    }

    protected static String txUri(long txId) {
        return defaultDatabaseUri() + "tx/" + txId;
    }

    protected static long extractTxId(HTTP.Response response) {
        int lastSlash = response.location().lastIndexOf('/');
        String txIdString = response.location().substring(lastSlash + 1);
        return Long.parseLong(txIdString);
    }

    protected static int getLocalHttpPort() {
        GraphDatabaseAPI database = container().getDefaultDatabase();
        ConnectorPortRegister connectorPortRegister =
                database.getDependencyResolver().resolveDependency(ConnectorPortRegister.class);
        return connectorPortRegister.getLocalAddress(ConnectorType.HTTP).getPort();
    }

    protected static HTTP.Response runQuery(String query, String... contentTypes) {
        String resultDataContents = "";
        if (contentTypes.length > 0) {
            resultDataContents = ", 'resultDataContents': ["
                    + Arrays.stream(contentTypes)
                            .map(unquoted -> format("'%s'", unquoted))
                            .collect(joining(",")) + "]";
        }
        return HTTP.POST(
                txCommitUri(),
                quotedJson(format("{'statements': [{'statement': '%s'%s}]}", query, resultDataContents)));
    }

    protected static void assertNoErrors(HTTP.Response response) throws JsonParseException {
        assertEquals("[]", response.get("errors").toString());
        assertEquals(0, response.get("errors").size());
    }

    protected static void assertHasTxLocation(HTTP.Response begin) {
        assertThat(begin.location()).matches(txUri() + "/\\d+");
    }

    protected static void assertHasTxLocation(HTTP.Response begin, String txUri) {
        assertThat(begin.location()).matches(format("http://localhost:\\d+/%s/\\d+", txUri));
    }

    public HTTP.Response POST(String uri) {
        return http.request("POST", uri);
    }

    public HTTP.Response POST(String uri, HTTP.RawPayload payload) {
        return http.request("POST", uri, payload);
    }

    public HTTP.Response POST(String uri, HTTP.RawPayload payload, Map<String, String> headers) {
        return http.request("POST", uri, payload, headers);
    }

    public HTTP.Response DELETE(String uri) {
        return http.request("DELETE", uri);
    }
}
