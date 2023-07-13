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
package org.neo4j.kernel.impl.coreapi;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.extension.ExtensionType.DATABASE;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
class QueryFailureTransactionIT {
    @Inject
    private GraphDatabaseAPI databaseAPI;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.addExtension(new CustomProcedureExtension(ProcedureWithException.class));
    }

    @Test
    void failedQueryExecutionMarksTransactionAsTerminated() {
        try (var transaction = databaseAPI.beginTx()) {
            assertThrows(Exception.class, () -> transaction.execute("CREATE (n:evilNode {v:0}) WITH (n) RETURN 1/n.v"));

            checkFailToCommit(transaction);
        }
    }

    @Test
    void failedQueryExecutionRollbackTransactionAndAllowUserRollback() {
        try (var transaction = databaseAPI.beginTx()) {
            assertThrows(Exception.class, () -> transaction.execute("CREATE (n:evilNode {v:0}) WITH (n) RETURN 1/n.v"));
            transaction.rollback();

            checkFailToCommitAfterRollback(transaction);
        }
    }

    @Test
    void incorrectQueryMarksTransactionAsTerminated() {
        try (var transaction = databaseAPI.beginTx()) {
            assertThrows(Exception.class, () -> transaction.execute("rollback everything!"));

            checkFailToCommit(transaction);
        }
    }

    @Test
    void markTransactionForTerminationOnResultException() {
        try (var transaction = databaseAPI.beginTx()) {
            var result = transaction.execute("CALL exception.stream.generate()");
            assertThrows(QueryExecutionException.class, () -> {
                while (result.hasNext()) {
                    result.next();
                }
            });
            checkFailToCommit(transaction);
        }
    }

    private static void checkFailToCommit(Transaction transaction) {
        assertThatThrownBy(transaction::commit)
                .isInstanceOf(TransactionFailureException.class)
                .rootCause()
                .isInstanceOf(TransactionTerminatedException.class);
    }

    private static void checkFailToCommitAfterRollback(Transaction transaction) {
        assertThatThrownBy(transaction::commit)
                .isInstanceOf(TransactionFailureException.class)
                .rootCause()
                .isInstanceOf(NotInTransactionException.class);
    }

    private static class CustomProcedureExtension extends ExtensionFactory<CustomProcedureExtension.Dependencies> {
        private final Class<?> procedureClass;

        interface Dependencies {
            GlobalProcedures procedures();

            Database database();
        }

        CustomProcedureExtension(Class<?> procedureClass) {
            super(DATABASE, "customProcedureRegistration");
            this.procedureClass = procedureClass;
        }

        @Override
        public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
            if (DEFAULT_DATABASE_NAME.equals(
                    dependencies.database().getNamedDatabaseId().name())) {
                return new LifecycleAdapter() {
                    @Override
                    public void start() throws Exception {
                        dependencies.procedures().registerProcedure(procedureClass);
                    }
                };
            } else {
                return new LifecycleAdapter();
            }
        }
    }
}
