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
package org.neo4j.graphdb;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
public class FailingTransactionEventListenerIT {
    public static final TransactionEventListenerAdapter<Object> FAILING_BEFORE_COMMIT =
            new TransactionEventListenerAdapter<>() {
                @Override
                public Object beforeCommit(
                        TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
                    var result = transaction.execute("return 1 / 0");
                    return result.stream().count();
                }
            };

    @Inject
    private GraphDatabaseService db;

    @Inject
    private GlobalTransactionEventListeners txEventListeners;

    @Inject
    private KernelTransactions kernelTransactions;

    @Test
    void shouldNotReturnTxToPoolTwiceWhenFailingInBeforeCommitEventListener() {
        txEventListeners.registerTransactionEventListener(DEFAULT_DATABASE_NAME, FAILING_BEFORE_COMMIT);

        assertThatThrownBy(() -> {
            try (Transaction tx = db.beginTx()) {
                tx.execute("CREATE (n:Something)");
                tx.commit();
            }
        });

        var numberOfActiveTransactions = kernelTransactions.getNumberOfActiveTransactions();
        assertThat(numberOfActiveTransactions).isEqualTo(0);
    }
}
