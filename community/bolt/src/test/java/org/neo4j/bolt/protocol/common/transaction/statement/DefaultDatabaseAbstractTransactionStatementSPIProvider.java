/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.transaction.statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40.ABSENT_DB_NAME;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.bolt.protocol.common.transaction.TransactionStateMachineSPI;
import org.neo4j.bolt.protocol.common.transaction.TransactionStateMachineSPIProvider;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.SystemNanoClock;

class DefaultDatabaseAbstractTransactionStatementSPIProvider {
    private final BoltChannel mockBoltChannel = mock(BoltChannel.class, RETURNS_MOCKS);

    @Test
    void shouldReturnDefaultTransactionStateMachineSPIWithEmptyDatabaseName() throws Throwable {
        DatabaseManagementService managementService = managementServiceWithDatabase("neo4j");
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider(managementService);

        TransactionStateMachineSPI spi = spiProvider.getTransactionStateMachineSPI(
                ABSENT_DB_NAME, mock(StatementProcessorReleaseManager.class), "123");
        assertThat(spi).isInstanceOf(TransactionStateMachineSPI.class);
    }

    @Test
    void shouldErrorIfDatabaseNotFound() {
        DatabaseManagementService managementService = managementServiceWithDatabase("database");
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider(managementService);

        BoltProtocolBreachFatality error = assertThrows(
                BoltProtocolBreachFatality.class,
                () -> spiProvider.getTransactionStateMachineSPI(
                        "database", mock(StatementProcessorReleaseManager.class), "123"));
        assertThat(error.getMessage())
                .contains("Database selection by name not supported by Bolt protocol version lower than BoltV4.");
    }

    private DatabaseManagementService managementServiceWithDatabase(String databaseName) {
        DatabaseManagementService managementService = mock(DatabaseManagementService.class);
        GraphDatabaseFacade databaseFacade = mock(GraphDatabaseFacade.class);
        DependencyResolver dependencyResolver = mock(DependencyResolver.class);
        GraphDatabaseQueryService queryService = mock(GraphDatabaseQueryService.class);

        doReturn(true).when(databaseFacade).isAvailable();
        doReturn(databaseFacade).when(managementService).database(databaseName);
        doReturn(mock(Database.class)).when(dependencyResolver).resolveDependency(Database.class);

        doReturn(dependencyResolver).when(queryService).getDependencyResolver();
        doReturn(dependencyResolver).when(databaseFacade).getDependencyResolver();

        doReturn(queryService).when(dependencyResolver).resolveDependency(GraphDatabaseQueryService.class);

        doReturn("neo4j").when(mockBoltChannel).defaultDatabase();

        return managementService;
    }

    private TransactionStateMachineSPIProvider newSpiProvider(DatabaseManagementService managementService) {
        var clock = mock(SystemNanoClock.class);
        var dbProvider = new BoltKernelDatabaseManagementServiceProvider(
                managementService, new Monitors(), clock, Duration.ZERO);
        return new AbstractTransactionStatementSPIProvider(dbProvider, mockBoltChannel, clock) {
            @Override
            protected TransactionStateMachineSPI newTransactionStateMachineSPI(
                    BoltGraphDatabaseServiceSPI activeBoltGraphDatabaseServiceSPI,
                    StatementProcessorReleaseManager resourceReleaseManager,
                    String transactionId) {
                return mock(TransactionStateMachineSPI.class);
            }

            @Override
            public void releaseTransactionStateMachineSPI() {}
        };
    }
}
