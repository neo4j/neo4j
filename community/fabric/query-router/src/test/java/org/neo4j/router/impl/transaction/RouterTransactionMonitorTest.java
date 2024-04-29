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
package org.neo4j.router.impl.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.configuration.Config;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.QueryStatementLifecycles;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitor;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.kernel.impl.query.QueryRoutingMonitor;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.router.QueryRouter;
import org.neo4j.router.impl.QueryRouterImpl;
import org.neo4j.router.impl.transaction.database.LocalDatabaseTransaction;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.DatabaseReferenceResolver;
import org.neo4j.router.query.QueryProcessor;
import org.neo4j.router.transaction.DatabaseTransactionFactory;
import org.neo4j.router.transaction.TransactionInfo;
import org.neo4j.time.FakeClock;

class RouterTransactionMonitorTest {

    private static final Duration DEFAULT_TX_TIMEOUT = Duration.ofSeconds(10);

    private final NormalizedDatabaseName databaseName = new NormalizedDatabaseName("a");
    private final FakeClock clock = new FakeClock();
    private final InternalLog log = mock(InternalLog.class);
    private Config config;
    private QueryRouterTransactionMonitor transactionMonitor;
    private QueryRouter queryRouter;

    @BeforeEach
    void beforeEach() {
        config = Config.defaults(transaction_timeout, DEFAULT_TX_TIMEOUT);
        var logService = mock(LogService.class);
        when(logService.getInternalLog(TransactionMonitor.class)).thenReturn(log);
        transactionMonitor = new QueryRouterTransactionMonitor(config, clock, logService);
        var databaseId = DatabaseIdFactory.from(databaseName.name(), UUID.randomUUID());
        var databaseRef = new DatabaseReferenceImpl.Internal(databaseName, databaseId, true);
        var databaseReferenceResolver = mock(DatabaseReferenceResolver.class);
        when(databaseReferenceResolver.resolve(databaseName)).thenReturn(databaseRef);
        var locationService = mock(LocationService.class);
        var location = mock(Location.Local.class);
        when(location.databaseReference()).thenReturn(databaseRef);
        when(locationService.locationOf(databaseRef)).thenReturn(location);

        var databaseTransactionFactory = mock(DatabaseTransactionFactory.class);
        when(databaseTransactionFactory.beginTransaction(any(), any(), any(), any(), any()))
                .thenReturn(mock(LocalDatabaseTransaction.class));

        queryRouter = new QueryRouterImpl(
                config,
                databaseReferenceResolver,
                ignored -> locationService,
                mock(QueryProcessor.class),
                databaseTransactionFactory,
                mock(DatabaseTransactionFactory.class),
                mock(ErrorReporter.class),
                clock,
                mock(LocalGraphTransactionIdTracker.class),
                mock(QueryStatementLifecycles.class),
                mock(QueryRoutingMonitor.class),
                new RouterTransactionManager(transactionMonitor),
                mock(AbstractSecurityLog.class),
                mock(InternalLog.class));
    }

    @Test
    void testTransactionMonitorInteraction() {
        var tx1 = queryRouter.beginTransaction(createTransactionInfo(Duration.ofSeconds(5)));
        // tx with the default timeout 10s
        var tx2 = queryRouter.beginTransaction(createTransactionInfo(null));

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(2);

        transactionMonitor.run();

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(2);
        assertThat(tx1.routerTransaction().getReasonIfTerminated()).isEmpty();
        assertThat(tx2.routerTransaction().getReasonIfTerminated()).isEmpty();

        clock.forward(Duration.ofSeconds(2));
        transactionMonitor.run();
        verify(log, never()).warn(any(String.class), ArgumentMatchers.<Object>any());

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(2);
        assertThat(tx1.routerTransaction().getReasonIfTerminated()).isEmpty();
        assertThat(tx2.routerTransaction().getReasonIfTerminated()).isEmpty();

        clock.forward(Duration.ofSeconds(4));
        transactionMonitor.run();

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(2);
        assertThat(tx1.routerTransaction().getReasonIfTerminated()).isPresent();
        assertThat(tx2.routerTransaction().getReasonIfTerminated()).isEmpty();

        // Termination of a transaction is logged only once ...
        verify(log, times(1)).warn(any(String.class), ArgumentMatchers.<Object>any());
        transactionMonitor.run();
        // ... regardless how many times the monitor runs
        verify(log, times(1)).warn(any(String.class), ArgumentMatchers.<Object>any());

        tx1.routerTransaction().rollback();
        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(1);

        clock.forward(Duration.ofSeconds(5));
        transactionMonitor.run();

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(1);
        assertThat(tx1.routerTransaction().getReasonIfTerminated()).isPresent();
        assertThat(tx2.routerTransaction().getReasonIfTerminated()).isPresent();

        // Termination of a transaction is logged only once ...
        verify(log, times(2)).warn(any(String.class), ArgumentMatchers.<Object>any());
        transactionMonitor.run();
        // ... regardless how many times the monitor runs
        verify(log, times(2)).warn(any(String.class), ArgumentMatchers.<Object>any());

        tx2.routerTransaction().rollback();
        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(0);
    }

    @Test
    void testChangeTimeoutAtRuntime() {
        config.setDynamic(transaction_timeout, Duration.ofSeconds(20), "test");
        var tx1 = queryRouter.beginTransaction(createTransactionInfo(Duration.ofSeconds(5)));
        var tx2 = queryRouter.beginTransaction(createTransactionInfo(null));

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(2);

        clock.forward(Duration.ofSeconds(11));
        transactionMonitor.run();

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(2);
        assertThat(tx1.routerTransaction().getReasonIfTerminated()).isPresent();
        assertThat(tx2.routerTransaction().getReasonIfTerminated()).isEmpty();

        clock.forward(Duration.ofSeconds(10));
        transactionMonitor.run();

        assertThat(transactionMonitor.getActiveTransactions()).size().isEqualTo(2);
        assertThat(tx1.routerTransaction().getReasonIfTerminated()).isPresent();
        assertThat(tx2.routerTransaction().getReasonIfTerminated()).isPresent();
    }

    private TransactionInfo createTransactionInfo(Duration timeout) {
        return new TransactionInfo(
                databaseName,
                KernelTransaction.Type.EXPLICIT,
                AUTH_DISABLED,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                List.of(),
                timeout,
                AccessMode.READ,
                Map.of(),
                new RoutingContext(true, Map.of()),
                QueryExecutionConfiguration.DEFAULT_CONFIG,
                false); // Currently we only test query router for non-composite databases.
    }
}
