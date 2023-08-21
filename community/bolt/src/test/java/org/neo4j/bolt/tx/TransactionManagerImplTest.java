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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.protocol.common.connector.tx.TransactionOwner;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.time.FakeClock;

class TransactionManagerImplTest {

    private BoltGraphDatabaseManagementServiceSPI graphDatabaseManagementService;
    private BoltGraphDatabaseServiceSPI graphDatabaseService;
    private Clock clock;

    private TransactionOwner transactionOwner;

    @BeforeEach
    void prepare() throws UnavailableException {
        this.graphDatabaseManagementService =
                Mockito.mock(BoltGraphDatabaseManagementServiceSPI.class, Mockito.RETURNS_MOCKS);
        this.graphDatabaseService = Mockito.mock(BoltGraphDatabaseServiceSPI.class, Mockito.RETURNS_MOCKS);
        this.clock = FakeClock.fixed(Instant.EPOCH, ZoneOffset.UTC);

        this.transactionOwner = Mockito.mock(TransactionOwner.class);

        Mockito.doReturn(this.graphDatabaseService)
                .when(this.graphDatabaseManagementService)
                .database(Mockito.any(), Mockito.any());
    }

    protected void shouldManageTransaction(TransactionType type, Type kernelType)
            throws UnavailableException, TransactionException {
        var transactionManager = new TransactionManagerImpl(this.graphDatabaseManagementService, this.clock);

        var tx = transactionManager.create(
                type,
                transactionOwner,
                "",
                AccessMode.WRITE,
                Collections.emptyList(),
                Duration.ofSeconds(42),
                Collections.emptyMap(),
                null);

        var inOrder =
                Mockito.inOrder(this.transactionOwner, this.graphDatabaseManagementService, this.graphDatabaseService);

        inOrder.verify(this.transactionOwner).selectedDefaultDatabase();
        inOrder.verify(this.transactionOwner).memoryTracker();
        inOrder.verify(this.graphDatabaseManagementService).database(Mockito.any(), Mockito.any());

        inOrder.verify(this.transactionOwner).loginContext();
        inOrder.verify(this.transactionOwner).info();
        inOrder.verify(this.transactionOwner).routingContext();
        inOrder.verify(this.graphDatabaseService)
                .beginTransaction(
                        Mockito.eq(kernelType),
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.eq(Collections.emptyList()),
                        Mockito.eq(Duration.ofSeconds(42)),
                        Mockito.eq(AccessMode.WRITE),
                        Mockito.eq(Collections.emptyMap()),
                        Mockito.any(),
                        Mockito.eq(QueryExecutionConfiguration.DEFAULT_CONFIG));

        // via TransactionImpl
        inOrder.verify(this.graphDatabaseService).getDatabaseReference();

        inOrder.verifyNoMoreInteractions();

        Assertions.assertThat(tx).isNotNull().extracting(Transaction::id).isEqualTo("bolt-1");
        Assertions.assertThat(transactionManager.get("bolt-1")).isPresent().containsSame(tx);

        tx.close();

        Assertions.assertThat(transactionManager.get("bolt-1")).isNotPresent();
    }

    @Test
    void shouldManageExplicitTransactions() throws TransactionException, UnavailableException {
        this.shouldManageTransaction(TransactionType.EXPLICIT, Type.EXPLICIT);
    }

    @Test
    void shouldManageImplicitTransactions() throws TransactionException, UnavailableException {
        this.shouldManageTransaction(TransactionType.IMPLICIT, Type.IMPLICIT);
    }
}
