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
package org.neo4j.bolt.tx;

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.testing.assertions.DiagnosticRecordAssertions;
import org.neo4j.bolt.testing.assertions.FailureMetadataAssertions;
import org.neo4j.bolt.testing.assertions.GqlMessageParameters;
import org.neo4j.bolt.testing.assertions.RetryConfiguration;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

/**
 * Ensures that Bolt terminates transactions when {@code RESET} is received.
 */
@TestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class TransactionTerminationIT {

    @Inject
    private Neo4jWithSocket server;

    private void awaitTransactionStart() throws InterruptedException {
        Awaitility.await()
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .pollInSameThread()
                .untilAsserted(() -> {
                    try (var tx = server.graphDatabaseService().beginTx()) {
                        var result = tx.execute("SHOW TRANSACTIONS");
                        var txCount = result.stream().toList().size();

                        Assertions.assertThat(txCount)
                                .as("transaction count to exceed 1")
                                .isGreaterThan(1);
                    }
                });
    }

    @ProtocolTest
    void killTxViaReset(BoltWire wire, @Authenticated BoltTestConnection connection) throws Exception {
        connection.send(wire.begin()).send(wire.run("UNWIND range(1, 2000000) AS i CREATE (n)"));

        awaitTransactionStart();

        connection.send(wire.reset());

        assertThat(connection)
                .receivesSuccess()
                .receivesFailure(
                        FailureMetadataAssertions.create()
                                .hasLegacyStatus(Status.Transaction.Terminated)
                                .hasLegacyMessageFuzzy(
                                        "The transaction has been terminated. Retry your operation in a new transaction")
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_25N14,
                                        GqlMessageParameters.create().withString("Explicitly terminated by the user."))
                                .hasDescriptionFuzzy("error: invalid transaction state")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR)),
                        FailureMetadataAssertions.create()
                                .hasLegacyStatus(Status.Transaction.LockClientStopped)
                                .hasLegacyMessageFuzzy(
                                        "The transaction has been terminated. Retry your operation in a new transaction")
                                .hasStatus(GqlStatusInfoCodes.STATUS_25N14)
                                .hasDescriptionFuzzy("error: invalid transaction state")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR)))
                .receivesSuccess();
    }

    @ProtocolTest
    void killTxThenTryToUseItTest(BoltWire wire, @Authenticated BoltTestConnection connection) throws Exception {
        connection
                .send(wire.begin())
                .send(wire.run("UNWIND range(1, 200) AS i RETURN i"))
                .send(wire.pull());

        assertThat(connection).receivesSuccess(2);

        assertThat(connection).receivesRecords();

        awaitTransactionStart(); // Start but should go to sleep

        // Find and cancel the transaction we started above.
        try (var tx = server.graphDatabaseService().beginTx()) {
            var result = tx.execute("SHOW TRANSACTIONS");
            var unwindTransaction = result.stream().toList().stream()
                    .filter(x -> !x.get("connectionId").equals("")
                            && !x.get("clientAddress").equals(""));

            var transactionId = (String) unwindTransaction.toList().get(0).get("transactionId");

            var terminationResult = tx.execute(String.format("TERMINATE TRANSACTION \"%s\"", transactionId));

            var termination = terminationResult.stream().toList().get(0); // should only ever be one.

            Assertions.assertThat(termination.get("message")).isEqualTo("Transaction terminated.");
        }

        connection.send(wire.run("UNWIND range(1, 200) AS i RETURN i")); // send a run to a canceled transaction

        assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Transaction.Terminated)
                        .hasLegacyMessage(
                                "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. ")
                        .hasStatus(
                                GqlStatusInfoCodes.STATUS_25N14,
                                GqlMessageParameters.create().withString("Explicitly terminated by the user."))
                        .hasDescription(
                                "error: invalid transaction state - transaction termination client error. The transaction has been terminated. "
                                        + "Retry your operation in a new transaction, and you should see a successful result. Reason: Explicitly terminated by the user.")
                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                .hasClassification(ErrorClassification.CLIENT_ERROR)));
    }

    @ProtocolTest
    void killedTxShouldNotDestroyConnection(BoltWire wire, @Authenticated BoltTestConnection connection)
            throws Exception {
        connection
                .send(wire.begin())
                .send(wire.run("UNWIND range(1, 200) AS i RETURN i"))
                .send(wire.pull());

        assertThat(connection).receivesSuccess(2);
        assertThat(connection).receivesRecords();

        awaitTransactionStart(); // Start but should go to sleep

        // Find and cancel the transaction we started above.
        try (var tx = server.graphDatabaseService().beginTx()) {
            var result = tx.execute("SHOW TRANSACTIONS");
            var unwindTransaction = result.stream().toList().stream()
                    .filter(x -> !x.get("connectionId").equals("")
                            && !x.get("clientAddress").equals(""));

            var transactionId = (String) unwindTransaction.toList().get(0).get("transactionId");

            var terminationResult = tx.execute(String.format("TERMINATE TRANSACTION \"%s\"", transactionId));

            var termination = terminationResult.stream().toList().get(0); // should only ever be one.

            Assertions.assertThat(termination.get("message")).isEqualTo("Transaction terminated.");
        }

        assertThat(connection)
                .receivesFailureEventually(
                        RetryConfiguration.create()
                                .onPrepare(() -> connection.send(wire.run("UNWIND range(1, 200) AS i RETURN i"))),
                        FailureMetadataAssertions.create()
                                .hasLegacyStatus(Status.Transaction.Terminated)
                                .hasLegacyMessageFuzzy(
                                        "The transaction has been terminated. Retry your operation in a new transaction")
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_25N14,
                                        GqlMessageParameters.create().withString("Explicitly terminated by the user."))
                                .hasDescription(
                                        "error: invalid transaction state - transaction termination client error. The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Reason: Explicitly terminated by the user.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR)));

        connection
                .send(wire.reset())
                .send(wire.begin())
                .send(wire.run("RETURN 1 as n"))
                .send(wire.pull(1))
                .send(wire.commit());

        assertThat(connection).receivesSuccess(3).receivesRecord().receivesSuccess(2);
    }
}
