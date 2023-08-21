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
package org.neo4j.server.http.cypher.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.api.exceptions.Status.Request.InvalidFormat;
import static org.neo4j.kernel.api.exceptions.Status.Statement.SyntaxError;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.containsNoStackTraces;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.hasErrors;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

import org.junit.jupiter.api.Test;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.test.server.HTTP;

/**
 * Tests for error messages and graceful handling of problems with the transactional endpoint.
 */
public class TransactionErrorIT extends AbstractRestFunctionalTestBase {
    @Test
    public void begin__commit_with_invalid_cypher() throws Exception {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        HTTP.Response response = POST(TX_ENDPOINT, quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));
        String commitResource = response.stringFromContent("commit");

        // commit with invalid cypher
        response = POST(commitResource, quotedJson("{ 'statements': [ { 'statement': 'CREATE ;;' } ] }"));

        assertThat(response.status()).isEqualTo(200);
        assertThat(response).satisfies(hasErrors(SyntaxError));
        assertThat(response).satisfies(containsNoStackTraces());

        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction);
    }

    @Test
    public void begin__commit_with_malformed_json() throws Exception {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        HTTP.Response begin = POST(TX_ENDPOINT, quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));
        String commitResource = begin.stringFromContent("commit");

        // commit with malformed json
        HTTP.Response response = POST(commitResource, rawPayload("[{asd,::}]"));

        assertThat(response.status()).isEqualTo(200);
        assertThat(response).satisfies(hasErrors(InvalidFormat));

        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction);
    }

    private long countNodes() {
        return TransactionConditions.countNodes(graphdb());
    }
}
