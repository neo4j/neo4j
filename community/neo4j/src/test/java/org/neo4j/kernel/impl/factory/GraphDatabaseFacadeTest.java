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
package org.neo4j.kernel.impl.factory;

import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.query.TransactionalContext;

class GraphDatabaseFacadeTest {
    private GraphDatabaseFacade graphDatabaseFacade;
    private GraphDatabaseQueryService queryService;
    private Kernel kernel;

    @BeforeEach
    void setUp() {
        queryService = mock(GraphDatabaseQueryService.class);
        Database database = mock(Database.class, RETURNS_MOCKS);
        Dependencies resolver = mock(Dependencies.class);
        kernel = mock(Kernel.class, RETURNS_MOCKS);
        when(database.getKernel()).thenReturn(kernel);
        when(database.getDependencyResolver()).thenReturn(resolver);

        when(resolver.resolveDependency(GraphDatabaseQueryService.class)).thenReturn(queryService);
        Config config = Config.defaults();
        when(resolver.resolveDependency(Config.class)).thenReturn(config);

        graphDatabaseFacade = new GraphDatabaseFacade(
                database,
                config,
                DbmsInfo.COMMUNITY,
                HostedOnMode.SINGLE,
                TransactionalContext.DatabaseMode.SINGLE,
                mock(DatabaseAvailabilityGuard.class));
    }

    @Test
    void beginTransactionWithCustomTimeout() throws TransactionFailureException {
        graphDatabaseFacade.beginTx(10, TimeUnit.MILLISECONDS);

        verify(kernel).beginTransaction(KernelTransaction.Type.EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, 10L);
    }

    @Test
    void beginTransaction() throws TransactionFailureException {
        graphDatabaseFacade.beginTx();

        long timeout =
                Config.defaults().get(GraphDatabaseSettings.transaction_timeout).toMillis();
        verify(kernel).beginTransaction(KernelTransaction.Type.EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout);
    }
}
