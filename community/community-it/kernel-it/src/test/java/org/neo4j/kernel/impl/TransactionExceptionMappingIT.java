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
package org.neo4j.kernel.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.DeadlockDetected;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionStartFailed;

import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.util.ReadAndDeleteTransactionConflictException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class TransactionExceptionMappingIT {
    @Inject
    private GraphDatabaseAPI database;

    @Inject
    private DatabaseManagementService managementService;

    @Test
    void transactionFailureOnTransactionClose() {
        var rootCause = new TransactionFailureException("Test failure", TransactionStartFailed);
        var e = assertThrows(Exception.class, () -> {
            try (var tx = database.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
                tx.registerCloseableResource(() -> {
                    throw rootCause;
                });
                tx.commit();
            }
        });
        assertThat(e).isInstanceOf(TransactionFailureException.class).hasRootCause(rootCause);
    }

    @Test
    void nonTransientTransientFailureOnTransactionClose() {
        var rootCause = new ReadAndDeleteTransactionConflictException(true);
        managementService.registerTransactionEventListener(
                database.databaseName(), new TransactionEventListenerAdapter<>() {
                    @Override
                    public Object beforeCommit(
                            TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
                            throws Exception {
                        throw rootCause;
                    }
                });
        var e = assertThrows(Exception.class, () -> {
            try (var tx = database.beginTx()) {
                tx.createNode();
                tx.commit();
            }
        });
        assertThat(e).isInstanceOf(TransactionFailureException.class);
    }

    @Test
    void transientTransientFailureOnTransactionClose() {
        var rootCause = new TransientTransactionFailureException(DeadlockDetected, "Deadlock detected.");
        managementService.registerTransactionEventListener(
                database.databaseName(), new TransactionEventListenerAdapter<>() {
                    @Override
                    public Object beforeCommit(
                            TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
                            throws Exception {
                        throw rootCause;
                    }
                });
        var e = assertThrows(Exception.class, () -> {
            try (var tx = database.beginTx()) {
                tx.createNode();
                tx.commit();
            }
        });
        assertThat(e).isInstanceOf(TransientTransactionFailureException.class);
    }

    @Test
    void transientExceptionOnDeadlockDetectedError() {
        var rootCause = new DeadlockDetectedException("No panic!");
        managementService.registerTransactionEventListener(
                database.databaseName(), new TransactionEventListenerAdapter<>() {
                    @Override
                    public Object beforeCommit(
                            TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
                            throws Exception {
                        throw rootCause;
                    }
                });
        var e = assertThrows(Exception.class, () -> {
            try (var tx = database.beginTx()) {
                tx.createNode();
                tx.commit();
            }
        });
        assertThat(e).isInstanceOf(TransientTransactionFailureException.class);
    }

    @Test
    void transientExceptionOnAnyTransientException() {
        var rootCause = new ReadAndDeleteTransactionConflictException(false);
        managementService.registerTransactionEventListener(
                database.databaseName(), new TransactionEventListenerAdapter<>() {
                    @Override
                    public Object beforeCommit(
                            TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
                            throws Exception {
                        throw rootCause;
                    }
                });
        var e = assertThrows(Exception.class, () -> {
            try (var tx = database.beginTx()) {
                tx.createNode();
                tx.commit();
            }
        });
        assertThat(e).isInstanceOf(TransientTransactionFailureException.class);
    }
}
