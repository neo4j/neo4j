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
package org.neo4j.cypher.internal.javacompat;

import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.query.QuerySubscriber.DO_NOTHING_SUBSCRIBER;
import static org.neo4j.kernel.impl.query.TransactionalContext.DatabaseMode.SINGLE;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.virtual.MapValue;

@ImpermanentDbmsExtension
class ServerExecutionEngineTest {
    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private QueryExecutionEngine executionEngine;

    @Inject
    private KernelTransactionFactory transactionFactory;

    @Inject
    private GraphDatabaseQueryService queryService;

    @Test
    void shouldCloseResourcesInCancel() throws Exception {
        // GIVEN
        TransactionalContextFactory contextFactory =
                Neo4jTransactionalContextFactory.create(() -> queryService, transactionFactory, SINGLE);
        // We need two node vars to have one non-pooled cursor
        String query = "MATCH (n), (m) WHERE true RETURN n, m, n.name, m.name";

        try (InternalTransaction tx = db.beginTransaction(KernelTransaction.Type.EXPLICIT, AUTH_DISABLED)) {
            tx.createNode();
            tx.createNode();

            TransactionalContext context =
                    contextFactory.newContext(tx, query, MapValue.EMPTY, QueryExecutionConfiguration.DEFAULT_CONFIG);
            QueryExecution execution =
                    executionEngine.executeQuery(query, MapValue.EMPTY, context, false, DO_NOTHING_SUBSCRIBER);
            execution.request(1);
            execution.cancel(); // This should close all cursors
            tx.commit();
        }
    }
}
