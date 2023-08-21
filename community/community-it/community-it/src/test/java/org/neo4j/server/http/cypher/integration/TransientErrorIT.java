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
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.containsNoErrors;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.hasErrors;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.server.HTTP;

@ExtendWith(OtherThreadExtension.class)
public class TransientErrorIT extends AbstractRestFunctionalTestBase {
    @Inject
    public OtherThread otherThread;

    @Test
    @Timeout(60)
    public void deadlockShouldRollbackTransaction() throws Exception {
        // Given
        HTTP.Response initial = POST(
                txCommitUri(),
                quotedJson("{'statements': [{'statement': 'CREATE (n1 {prop : 1}), (n2 {prop : 2})'}]}"));
        assertThat(initial.status()).isEqualTo(200);
        assertThat(initial).satisfies(containsNoErrors());

        // When

        // tx1 takes a write lock on node1
        HTTP.Response firstInTx1 =
                POST(txUri(), quotedJson("{'statements': [{'statement': 'MATCH (n {prop : 1}) SET n.prop = 3'}]}"));
        final long tx1 = extractTxId(firstInTx1);

        // tx2 takes a write lock on node2
        HTTP.Response firstInTx2 =
                POST(txUri(), quotedJson("{'statements': [{'statement': 'MATCH (n {prop : 2}) SET n.prop = 4'}]}"));
        long tx2 = extractTxId(firstInTx2);

        // tx1 attempts to take a write lock on node2
        Future<HTTP.Response> future = otherThread.execute(() ->
                POST(txUri(tx1), quotedJson("{'statements': [{'statement': 'MATCH (n {prop : 2}) SET n.prop = 5'}]}")));

        // tx2 attempts to take a write lock on node1
        HTTP.Response secondInTx2 =
                POST(txUri(tx2), quotedJson("{'statements': [{'statement': 'MATCH (n {prop : 1}) SET n.prop = 6'}]}"));

        HTTP.Response secondInTx1 = future.get();

        // Then
        assertThat(secondInTx1.status()).isEqualTo(200);
        assertThat(secondInTx2.status()).isEqualTo(200);

        // either tx1 or tx2 should fail because of the deadlock
        HTTP.Response failed;
        if (containsError(secondInTx1)) {
            failed = secondInTx1;
        } else if (containsError(secondInTx2)) {
            failed = secondInTx2;
        } else {
            failed = null;
            fail("Either tx1 or tx2 is expected to fail");
        }

        assertThat(failed).satisfies(hasErrors(Status.Transaction.DeadlockDetected));

        // transaction was rolled back on the previous step and we can't commit it
        HTTP.Response commit = POST(failed.stringFromContent("commit"));
        assertThat(commit.status()).isEqualTo(404);
    }

    @Test
    public void unavailableCsvResourceShouldRollbackTransaction() throws JsonParseException {
        // Given
        HTTP.Response first = POST(txUri(), quotedJson("{'statements': [{'statement': 'CREATE ()'}]}"));
        assertThat(first.status()).isEqualTo(201);
        assertThat(first).satisfies(containsNoErrors());

        long txId = extractTxId(first);

        // When
        HTTP.Response second = POST(
                txUri(txId),
                quotedJson("{'statements': [{'statement': 'LOAD CSV FROM \\\"http://127.0.0.1/null/\\\" AS line "
                        + "CREATE (a {name:line[0]})'}]}"));

        // Then

        // request fails because specified CSV resource is invalid
        assertThat(second.status()).isEqualTo(200);
        assertThat(second).satisfies(hasErrors(Status.Statement.ExternalResourceFailed));

        // transaction was rolled back on the previous step and we can't commit it
        HTTP.Response commit = POST(second.stringFromContent("commit"));
        assertThat(commit.status()).isEqualTo(404);
    }

    private static boolean containsError(HTTP.Response response) throws JsonParseException {
        return response.get("errors").iterator().hasNext();
    }
}
