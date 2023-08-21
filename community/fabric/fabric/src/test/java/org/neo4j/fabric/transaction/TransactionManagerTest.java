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
package org.neo4j.fabric.transaction;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.shutdown_transaction_end_timeout;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.configuration.Config;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.time.Clocks;

class TransactionManagerTest {

    @Test
    void terminateNonLocalTransactionsOnStop() {
        var remoteTransactionContext = mock(FabricRemoteExecutor.RemoteTransactionContext.class);
        var localTransactionContext = mock(FabricRemoteExecutor.RemoteTransactionContext.class);
        when(localTransactionContext.isEmptyContext()).thenReturn(true);

        var fabricRemoteExecutor = mock(FabricRemoteExecutor.class);
        when(fabricRemoteExecutor.startTransactionContext(any(), any(), any()))
                .thenReturn(localTransactionContext, remoteTransactionContext, localTransactionContext);

        var localExecutor = mock(FabricLocalExecutor.class, RETURNS_MOCKS);
        var errorReporter = mock(ErrorReporter.class);
        var transactionMonitor = mock(FabricTransactionMonitor.class);
        var bookmarkManager = mock(TransactionBookmarkManager.class);
        var guard = mock(AvailabilityGuard.class);
        var securityLog = mock(AbstractSecurityLog.class);
        var catalogManager = mock(CatalogManager.class);
        var config = Config.defaults(shutdown_transaction_end_timeout, Duration.ZERO);
        var globalProcedures = mock(GlobalProcedures.class);
        var transactionManager = new TransactionManager(
                fabricRemoteExecutor,
                localExecutor,
                catalogManager,
                transactionMonitor,
                securityLog,
                Clocks.nanoClock(),
                config,
                guard,
                errorReporter,
                globalProcedures);

        // local tx
        var tx1 = transactionManager.begin(createTransactionInfo(), bookmarkManager);
        // remote tx
        var tx2 = transactionManager.begin(createTransactionInfo(), bookmarkManager);
        // will be terminated before stop
        var tx3 = transactionManager.begin(createTransactionInfo(), bookmarkManager);
        // local tx
        var tx4 = transactionManager.begin(createTransactionInfo(), bookmarkManager);

        tx3.markForTermination(Status.Transaction.Outdated);
        transactionManager.stop();

        assertTrue(tx1.isOpen());
        assertFalse(tx2.isOpen());
        assertFalse(tx3.isOpen());
        assertTrue(tx4.isOpen());
    }

    private static FabricTransactionInfo createTransactionInfo() {
        var databaseName = new NormalizedDatabaseName("a");
        var databaseId = DatabaseIdFactory.from(databaseName.name(), UUID.randomUUID());
        var databaseRef = new DatabaseReferenceImpl.Internal(databaseName, databaseId, true);
        return new FabricTransactionInfo(
                AccessMode.READ,
                LoginContext.AUTH_DISABLED,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                databaseRef,
                false,
                Duration.ZERO,
                emptyMap(),
                new RoutingContext(true, emptyMap()),
                QueryExecutionConfiguration.DEFAULT_CONFIG);
    }
}
