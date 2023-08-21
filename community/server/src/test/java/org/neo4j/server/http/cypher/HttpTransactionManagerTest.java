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
package org.neo4j.server.http.cypher;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;

class HttpTransactionManagerTest {
    @Test
    void shouldSetupJobScheduler() {
        var managementService = mock(DatabaseManagementService.class);
        JobScheduler jobScheduler = mock(JobScheduler.class);
        AssertableLogProvider logProvider = new AssertableLogProvider(true);
        var transactionManager = mock(TransactionManager.class);
        var authManager = mock(AuthManager.class);

        new HttpTransactionManager(
                managementService,
                mock(MemoryPool.class),
                jobScheduler,
                Clocks.systemClock(),
                Duration.ofMinutes(1),
                logProvider,
                transactionManager,
                authManager,
                false);

        long runEvery = Math.round(Duration.ofMinutes(1).toMillis() / 2.0);
        verify(jobScheduler)
                .scheduleRecurring(
                        eq(Group.SERVER_TRANSACTION_TIMEOUT),
                        any(JobMonitoringParams.class),
                        any(),
                        eq(runEvery),
                        eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldCreateTransactionHandleRegistry() {
        var managementService = mock(DatabaseManagementService.class);
        JobScheduler jobScheduler = mock(JobScheduler.class);
        AssertableLogProvider logProvider = new AssertableLogProvider(true);
        var transactionManager = mock(TransactionManager.class);
        var authManager = mock(AuthManager.class);

        var manager = new HttpTransactionManager(
                managementService,
                mock(MemoryPool.class),
                jobScheduler,
                Clocks.systemClock(),
                Duration.ofMinutes(1),
                logProvider,
                transactionManager,
                authManager,
                false);

        assertNotNull(manager.getTransactionRegistry());
    }

    @Test
    void shouldGetEmptyTransactionFacadeOfDatabaseData() {
        DatabaseManagementService managementService = mock(DatabaseManagementService.class);
        var memoryPool = mock(MemoryPool.class);
        var manager = newTransactionManager(managementService, memoryPool);
        var graphDatabaseFacade = manager.getGraphDatabaseAPI("data");

        assertFalse(graphDatabaseFacade.isPresent());

        verify(managementService).database("data");
    }

    @Test
    void shouldGetTransactionFacadeOfDatabaseWithSpecifiedName() {
        DatabaseManagementService managementService = mock(DatabaseManagementService.class);
        var memoryPool = mock(MemoryPool.class);
        var manager = newTransactionManager(managementService, memoryPool);
        var transactionFacade = manager.getGraphDatabaseAPI("neo4j");

        assertTrue(transactionFacade.isPresent());

        verify(managementService).database("neo4j");
    }

    @Test
    void shouldGetEmptyTransactionFacadeForUnknownDatabase() {
        DatabaseManagementService managementService = mock(DatabaseManagementService.class);
        var memoryPool = mock(MemoryPool.class);
        var manager = newTransactionManager(managementService, memoryPool);
        var transactionFacade = manager.getGraphDatabaseAPI("foo");

        assertFalse(transactionFacade.isPresent());

        verify(managementService).database("foo");
    }

    @Test
    void shouldCreateTransactionFacade() {
        var managementService = mock(DatabaseManagementService.class);
        var graphDatabase = mock(GraphDatabaseAPI.class);

        var dependencyResolver = mock(DependencyResolver.class);
        var queryExecutionEngine = mock(QueryExecutionEngine.class);

        var memoryPool = mock(MemoryPool.class);
        var memoryTracker = mock(MemoryTracker.class);
        var manager = newTransactionManager(managementService, memoryPool);

        when(graphDatabase.getDependencyResolver()).thenReturn(dependencyResolver);
        when(graphDatabase.dbmsInfo()).thenReturn(DbmsInfo.ENTERPRISE);
        when(dependencyResolver.resolveDependency(QueryExecutionEngine.class)).thenReturn(queryExecutionEngine);

        var facade = manager.createTransactionFacade(graphDatabase, memoryTracker, "neo4j");

        verify(memoryTracker).allocateHeap(TransactionFacade.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryTracker);

        assertNotNull(facade);
    }

    private static HttpTransactionManager newTransactionManager(
            DatabaseManagementService managementService, MemoryPool memoryPool) {
        JobScheduler jobScheduler = mock(JobScheduler.class);
        var transactionManager = mock(TransactionManager.class);
        var authManager = mock(AuthManager.class);
        AssertableLogProvider logProvider = new AssertableLogProvider(true);
        var defaultDatabase = "neo4j";
        when(managementService.database(any(String.class))).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String db = (String) args[0];

            if (db.equals(defaultDatabase) || db.equals("system")) {
                return graphWithName(db);
            } else {
                throw new DatabaseNotFoundException("Not found db named " + db);
            }
        });
        return new HttpTransactionManager(
                managementService,
                memoryPool,
                jobScheduler,
                Clocks.systemClock(),
                Duration.ofMinutes(1),
                logProvider,
                transactionManager,
                authManager,
                false);
    }

    private static GraphDatabaseFacade graphWithName(String name) {
        GraphDatabaseFacade graph = mock(GraphDatabaseFacade.class);
        when(graph.databaseName()).thenReturn(name);
        when(graph.getDependencyResolver()).thenReturn(mock(DependencyResolver.class, Answers.RETURNS_SMART_NULLS));
        return graph;
    }
}
