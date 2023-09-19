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
package org.neo4j.bolt.testing.extension.dependency;

import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mockito;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.protocol.common.connector.connection.ConnectionHandle;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.bolt.testing.mock.TransactionManagerMockFactory;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.time.FakeClock;

public class MockStateMachineDependencyProvider implements StateMachineDependencyProvider {
    private final BoltGraphDatabaseManagementServiceSPI spi = Mockito.mock(BoltGraphDatabaseManagementServiceSPI.class);
    ;
    private final FakeClock clock = new FakeClock();
    private TransactionManager transactionManager = TransactionManagerMockFactory.newInstance();
    private ConnectionHandle connection = ConnectionMockFactory.newFactory()
            .withTransactionManager(transactionManager)
            .build();

    @Override
    public BoltGraphDatabaseManagementServiceSPI spi(ExtensionContext context) {
        return this.spi;
    }

    @Override
    public FakeClock clock(ExtensionContext context) {
        return this.clock;
    }

    @Override
    public ConnectionHandle connection(ExtensionContext context) {
        return this.connection;
    }

    @Override
    public Optional<TransactionManager> transactionManager() {
        return Optional.ofNullable(this.transactionManager);
    }

    @Override
    public void close(ExtensionContext context) {
        this.connection = null;
        this.transactionManager = null;
    }
}
