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
package org.neo4j.cypher.util;

import org.neo4j.common.SystemLastTransactionIdProvider;
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * A version of GraphDatabaseCypherService that tracks the last system transaction id and awaits the system database.
 * Should only be user for tests.
 */
public class GraphDatabaseCypherTestService extends GraphDatabaseCypherService {

    private final boolean awaitSystem;
    private long systemTransactionId = -1;
    private SystemLastTransactionIdProvider systemLastTransactionIdProvider;
    private final GraphDatabaseAPI graph;

    public GraphDatabaseCypherTestService(GraphDatabaseService graph, boolean awaitSystem) {
        super(graph);
        this.graph = (GraphDatabaseAPI) graph;
        this.awaitSystem = awaitSystem;
    }

    @Override
    public InternalTransaction beginTransaction(KernelTransaction.Type type, LoginContext loginContext) {
        awaitSystemUpToDate();
        return new SystemLastTransactionIdTrackingWrapper(
                graph.beginTransaction(type, loginContext), id -> systemTransactionId = id, graph);
    }

    /*
     * If SPD is enabled we create a single-server cluster. Since we do not use a driver, we need to manually wait
     * for the system database to be up-to-date
     */
    private void awaitSystemUpToDate() {
        if (!awaitSystem) {
            return;
        }
        var i = 0;
        while (systemTransactionId > 0 && getLastTransactionId() < systemTransactionId) {
            i = i + 1;
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (i > 100) {
                throw new RuntimeException("System transaction did not catch up");
            }
        }
    }

    private long getLastTransactionId() {
        if (systemLastTransactionIdProvider == null) {
            systemLastTransactionIdProvider =
                    getDependencyResolver().resolveDependency(SystemLastTransactionIdProvider.class);
        }
        return systemLastTransactionIdProvider.lastTransactionId();
    }
}
