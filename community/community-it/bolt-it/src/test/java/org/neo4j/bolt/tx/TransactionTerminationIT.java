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

import static org.neo4j.bolt.test.util.ErrorUtil.useNewMessage;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.helpers.collection.Pair;
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
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void killTxViaResetV40(BoltWire wire, @Authenticated BoltTestConnection connection) throws Exception {
        connection.send(wire.begin()).send(wire.run("UNWIND range(1, 2000000) AS i CREATE (n)"));

        awaitTransactionStart();

        connection.send(wire.reset());

        assertThat(connection)
                .receivesSuccess()
                .receivesFailureV40(Status.Transaction.Terminated, Status.Transaction.LockClientStopped)
                .receivesSuccess();
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 7))
    void killTxViaReset(BoltWire wire, @Authenticated BoltTestConnection connection) throws Exception {
        connection.send(wire.begin()).send(wire.run("UNWIND range(1, 2000000) AS i CREATE (n)"));

        awaitTransactionStart();

        connection.send(wire.reset());

        assertThat(connection)
                .receivesSuccess()
                .receivesFailure(
                        Pair.of(Status.Transaction.Terminated, GqlStatusInfoCodes.STATUS_25N14.getGqlStatus()),
                        Pair.of(Status.Transaction.LockClientStopped, GqlStatusInfoCodes.STATUS_25N14.getGqlStatus()))
                .receivesSuccess();
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void killTxThenTryToUseItTestV40(BoltWire wire, @Authenticated BoltTestConnection connection) throws Exception {
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
                .receivesFailureV40(
                        Status.Transaction.Terminated,
                        "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. ");
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void killTxThenTryToUseItTestV5x7(BoltWire wire, @Authenticated BoltTestConnection connection) throws Exception {
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
                .receivesFailure(
                        Status.Transaction.Terminated,
                        "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. ",
                        GqlStatusInfoCodes.STATUS_25N14.getGqlStatus(),
                        "error: invalid transaction state - transaction termination client error. The transaction has been terminated. "
                                + "Retry your operation in a new transaction, and you should see a successful result. Reason: Explicitly terminated by the user.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
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
                .receivesFailure(
                        Status.Transaction.Terminated,
                        useNewMessage(
                                        "25N14: The transaction has been terminated. "
                                                + "Retry your operation in a new transaction, and you should see a successful result. Reason: Explicitly terminated by the user.")
                                .whenLegacyFallbackTo(
                                        "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. "),
                        GqlStatusInfoCodes.STATUS_25N14.getGqlStatus(),
                        "error: invalid transaction state - transaction termination client error. The transaction has been terminated. "
                                + "Retry your operation in a new transaction, and you should see a successful result. Reason: Explicitly terminated by the user.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"));
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void killedTxShouldNotDestroyConnectionV40(BoltWire wire, @Authenticated BoltTestConnection connection)
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

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .pollDelay(11, TimeUnit.SECONDS)
                .pollInSameThread()
                .untilAsserted(() -> {
                    connection.send(
                            wire.run("UNWIND range(1, 200) AS i RETURN i")); // send a run to a canceled transaction

                    assertThat(connection)
                            .receivesFailureV40(
                                    Status.Transaction.Terminated,
                                    "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. ");
                });

        connection
                .send(wire.reset())
                .send(wire.begin())
                .send(wire.run("RETURN 1 as n"))
                .send(wire.pull(1))
                .send(wire.commit());

        assertThat(connection).receivesSuccess(3).receivesRecord().receivesSuccess(2);
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void killedTxShouldNotDestroyConnectionV5x7(BoltWire wire, @Authenticated BoltTestConnection connection)
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

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .pollDelay(11, TimeUnit.SECONDS)
                .pollInSameThread()
                .untilAsserted(() -> {
                    connection.send(
                            wire.run("UNWIND range(1, 200) AS i RETURN i")); // send a run to a canceled transaction

                    assertThat(connection)
                            .receivesFailure(
                                    Status.Transaction.Terminated,
                                    "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. ",
                                    GqlStatusInfoCodes.STATUS_25N14.getGqlStatus(),
                                    "error: invalid transaction state - transaction termination client error. The transaction has been terminated. "
                                            + "Retry your operation in a new transaction, and you should see a successful result. Reason: Explicitly terminated by the user.",
                                    BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord(
                                            "CLIENT_ERROR"));
                });

        connection
                .send(wire.reset())
                .send(wire.begin())
                .send(wire.run("RETURN 1 as n"))
                .send(wire.pull(1))
                .send(wire.commit());

        assertThat(connection).receivesSuccess(3).receivesRecord().receivesSuccess(2);
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
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

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .pollDelay(11, TimeUnit.SECONDS)
                .pollInSameThread()
                .untilAsserted(() -> {
                    connection.send(
                            wire.run("UNWIND range(1, 200) AS i RETURN i")); // send a run to a canceled transaction

                    assertThat(connection)
                            .receivesFailure(
                                    Status.Transaction.Terminated,
                                    useNewMessage(
                                                    "25N14: The transaction has been terminated. "
                                                            + "Retry your operation in a new transaction, and you should see a successful result. Reason: Explicitly terminated by the user.")
                                            .whenLegacyFallbackTo(
                                                    "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. "),
                                    GqlStatusInfoCodes.STATUS_25N14.getGqlStatus(),
                                    "error: invalid transaction state - transaction termination client error. The transaction has been terminated. "
                                            + "Retry your operation in a new transaction, and you should see a successful result. Reason: Explicitly terminated by the user.",
                                    BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord(
                                            "CLIENT_ERROR"));
                });

        connection
                .send(wire.reset())
                .send(wire.begin())
                .send(wire.run("RETURN 1 as n"))
                .send(wire.pull(1))
                .send(wire.commit());

        assertThat(connection).receivesSuccess(3).receivesRecord().receivesSuccess(2);
    }
}
