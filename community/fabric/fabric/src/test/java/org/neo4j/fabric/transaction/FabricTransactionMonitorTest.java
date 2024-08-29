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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.configuration.Config;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitor;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.FakeClock;

class FabricTransactionMonitorTest {

    private static final Duration DEFAULT_TX_TIMEOUT = Duration.ofSeconds(10);

    private final FakeClock clock = new FakeClock();
    private final TransactionBookmarkManager bookmarkManager = mock(TransactionBookmarkManager.class);
    private final InternalLog log = mock(InternalLog.class);
    private TransactionManager transactionManager;
    private FabricTransactionMonitor transactionMonitor;

    @BeforeEach
    void beforeEach() {
        var remoteTransactionContext = mock(FabricRemoteExecutor.RemoteTransactionContext.class);
        var localTransactionContext = mock(FabricRemoteExecutor.RemoteTransactionContext.class);
        when(localTransactionContext.isEmptyContext()).thenReturn(true);

        var fabricRemoteExecutor = mock(FabricRemoteExecutor.class);
        when(fabricRemoteExecutor.startTransactionContext(any(), any(), any()))
                .thenReturn(localTransactionContext, remoteTransactionContext, localTransactionContext);

        var localExecutor = mock(FabricLocalExecutor.class, RETURNS_MOCKS);
        var errorReporter = mock(ErrorReporter.class);
        var guard = mock(AvailabilityGuard.class);
        var securityLog = mock(AbstractSecurityLog.class);
        var catalogManager = mock(CatalogManager.class);
        var config = Config.defaults(transaction_timeout, DEFAULT_TX_TIMEOUT);
        var globalProcedures = mock(GlobalProcedures.class);
        var fabricConfig = new FabricConfig(() -> DEFAULT_TX_TIMEOUT, null, false);

        var logService = mock(LogService.class);
        when(logService.getInternalLog(TransactionMonitor.class)).thenReturn(log);
        transactionMonitor = new FabricTransactionMonitor(config, clock, logService, fabricConfig);
        transactionManager = new TransactionManager(
                fabricRemoteExecutor,
                localExecutor,
                catalogManager,
                transactionMonitor,
                securityLog,
                clock,
                config,
                guard,
                errorReporter,
                globalProcedures);
    }

    @Test
    void testTransactionMonitorInteraction() {
        var tx1 = transactionManager.begin(createTransactionInfo(Duration.ofSeconds(5)), bookmarkManager);
        // tx with the default timeout 10s
        var tx2 = transactionManager.begin(createTransactionInfo(null), bookmarkManager);

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(2);

        transactionMonitor.run();

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(2);
        assertThat(tx1.getTerminationMark()).isEmpty();
        assertThat(tx2.getTerminationMark()).isEmpty();

        clock.forward(Duration.ofSeconds(2));
        transactionMonitor.run();
        verify(log, never()).warn(any(String.class), ArgumentMatchers.<Object>any());

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(2);
        assertThat(tx1.getTerminationMark()).isEmpty();
        assertThat(tx2.getTerminationMark()).isEmpty();

        clock.forward(Duration.ofSeconds(4));
        transactionMonitor.run();

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(2);
        assertThat(tx1.getTerminationMark()).isPresent();
        assertThat(tx2.getTerminationMark()).isEmpty();

        // Termination of a transaction is logged only once ...
        verify(log, times(1)).warn(any(String.class), ArgumentMatchers.<Object>any());
        transactionMonitor.run();
        // ... regardless how many times the monitor runs
        verify(log, times(1)).warn(any(String.class), ArgumentMatchers.<Object>any());

        tx1.rollback();
        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(1);

        clock.forward(Duration.ofSeconds(5));
        transactionMonitor.run();

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(1);
        assertThat(tx1.getTerminationMark()).isPresent();
        assertThat(tx2.getTerminationMark()).isPresent();

        // Termination of a transaction is logged only once ...
        verify(log, times(2)).warn(any(String.class), ArgumentMatchers.<Object>any());
        transactionMonitor.run();
        // ... regardless how many times the monitor runs
        verify(log, times(2)).warn(any(String.class), ArgumentMatchers.<Object>any());

        tx2.rollback();
        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(0);
    }

    private static FabricTransactionInfo createTransactionInfo(Duration timeout) {
        var databaseName = new NormalizedDatabaseName("a");
        var databaseId = DatabaseIdFactory.from(databaseName.name(), UUID.randomUUID());
        var databaseRef = new DatabaseReferenceImpl.Internal(databaseName, databaseId, true);
        return new FabricTransactionInfo(
                AccessMode.READ,
                LoginContext.AUTH_DISABLED,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                databaseRef,
                false,
                timeout,
                emptyMap(),
                new RoutingContext(true, emptyMap()),
                QueryExecutionConfiguration.DEFAULT_CONFIG);
    }
}
