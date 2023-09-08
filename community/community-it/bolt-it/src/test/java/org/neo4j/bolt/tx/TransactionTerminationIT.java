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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import org.junit.jupiter.api.Timeout;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
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
        long txCount = 1;
        while (txCount <= 1) {
            try (var tx = server.graphDatabaseService().beginTx()) {
                var result = tx.execute("SHOW TRANSACTIONS");
                txCount = result.stream().toList().size();
            }

            Thread.sleep(100);
        }
    }

    @Timeout(15)
    @ProtocolTest
    void killTxViaReset(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        connection.send(wire.begin()).send(wire.run("UNWIND range(1, 2000000) AS i CREATE (n)"));

        awaitTransactionStart();

        connection.send(wire.reset());

        assertThat(connection)
                .receivesSuccess()
                .receivesFailure(Status.Transaction.Terminated, Status.Transaction.LockClientStopped)
                .receivesSuccess();
    }

    @Timeout(15)
    @ProtocolTest
    void killTxThenTryToUseItTest(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
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

            assertEquals(termination.get("message"), "Transaction terminated.");
        }

        connection.send(wire.run("UNWIND range(1, 200) AS i RETURN i")); // send a run to a canceled transaction

        assertThat(connection)
                .receivesFailure(
                        Status.Transaction.Terminated,
                        "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. ");
    }

    @Timeout(20)
    @ProtocolTest
    void killedTxShouldNotDestroyConnection(BoltWire wire, @Authenticated TransportConnection connection)
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

            assertEquals(termination.get("message"), "Transaction terminated.");
        }
        // Due to there being an explicit 10s timeout before validation the calling code should pause.
        Thread.sleep(11000);

        connection.send(wire.run("UNWIND range(1, 200) AS i RETURN i")); // send a run to a canceled transaction

        assertThat(connection)
                .receivesFailure(
                        Status.Transaction.Terminated,
                        "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. ");

        connection
                .send(wire.reset())
                .send(wire.begin())
                .send(wire.run("RETURN 1 as n"))
                .send(wire.pull(1))
                .send(wire.commit());

        assertThat(connection).receivesSuccess(3).receivesRecord().receivesSuccess(2);
    }
}
