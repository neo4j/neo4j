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
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.fabric.transaction.TransactionMode.DEFINITELY_READ;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTimedOut;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.configuration.Config;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.fabric.executor.Location;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.time.Clocks;

public class FabricTransactionImplTest {

    @Test
    void testChildrenAreTerminated() {
        var config = Config.defaults();
        var bookmarkManager = mock(TransactionBookmarkManager.class);

        var loc1 = new Location.Local(1, internalDatabase("one"));
        var loc2 = new Location.Local(2, internalDatabase("two"));
        var loc3 = new Location.Local(3, internalDatabase("three"));

        var itx1 = internalTransaction(true, null);
        var itx2 = internalTransaction(true, null);
        var itx3 = internalTransaction(true, null);

        var transactionManager =
                transactionManager(config, localExecutor(config, Map.of(loc1, itx1, loc2, itx2, loc3, itx3)));
        var tx = transactionManager.begin(createTransactionInfo(), bookmarkManager);

        tx.execute(ctx -> {
            var local = ctx.getLocal();
            local.getOrCreateTx(loc1, DEFINITELY_READ, false);
            local.getOrCreateTx(loc2, DEFINITELY_READ, false);
            local.getOrCreateTx(loc3, DEFINITELY_READ, false);
            tx.markForTermination(Terminated);

            verify(itx1).terminate(Terminated);
            verify(itx2).terminate(Terminated);
            verify(itx3).terminate(Terminated);

            return null;
        });
    }

    @Test
    void testClosedChildrenAreNotTerminated() {
        var config = Config.defaults();
        var bookmarkManager = mock(TransactionBookmarkManager.class);

        var loc1 = new Location.Local(1, internalDatabase("one"));
        var loc2 = new Location.Local(2, internalDatabase("two"));
        var loc3 = new Location.Local(3, internalDatabase("three"));

        var itx1 = internalTransaction(true, null);
        var itx2 = internalTransaction(false, null);
        var itx3 = internalTransaction(true, null);

        var transactionManager =
                transactionManager(config, localExecutor(config, Map.of(loc1, itx1, loc2, itx2, loc3, itx3)));
        var tx = transactionManager.begin(createTransactionInfo(), bookmarkManager);

        tx.execute(ctx -> {
            var local = ctx.getLocal();
            local.getOrCreateTx(loc1, DEFINITELY_READ, false);
            local.getOrCreateTx(loc2, DEFINITELY_READ, false);
            local.getOrCreateTx(loc3, DEFINITELY_READ, false);
            tx.markForTermination(Terminated);

            verify(itx1).terminate(Terminated);
            verify(itx2, never()).terminate(Terminated);
            verify(itx3).terminate(Terminated);

            return null;
        });
    }

    @Test
    void testTerminatedChildrenAreNotTerminated() {
        var config = Config.defaults();
        var bookmarkManager = mock(TransactionBookmarkManager.class);

        var loc1 = new Location.Local(1, internalDatabase("one"));
        var loc2 = new Location.Local(2, internalDatabase("two"));
        var loc3 = new Location.Local(3, internalDatabase("three"));

        var itx1 = internalTransaction(true, null);
        var itx2 = internalTransaction(true, TransactionTimedOut);
        var itx3 = internalTransaction(true, null);

        var transactionManager =
                transactionManager(config, localExecutor(config, Map.of(loc1, itx1, loc2, itx2, loc3, itx3)));
        var tx = transactionManager.begin(createTransactionInfo(), bookmarkManager);

        tx.execute(ctx -> {
            var local = ctx.getLocal();
            local.getOrCreateTx(loc1, DEFINITELY_READ, false);
            local.getOrCreateTx(loc2, DEFINITELY_READ, false);
            local.getOrCreateTx(loc3, DEFINITELY_READ, false);
            tx.markForTermination(Terminated);

            verify(itx1).terminate(Terminated);
            verify(itx2, never()).terminate(Terminated);
            verify(itx3).terminate(Terminated);

            return null;
        });
    }

    private static DatabaseReferenceImpl.Internal internalDatabase(String name) {
        return new DatabaseReferenceImpl.Internal(
                new NormalizedDatabaseName(name), DatabaseIdFactory.from(name, UUID.randomUUID()), true);
    }

    private static InternalTransaction internalTransaction(boolean open, Status terminated) {
        var itx = mock(InternalTransaction.class);
        when(itx.isOpen()).thenReturn(open);
        when(itx.terminationReason()).thenReturn(Optional.ofNullable(terminated));
        return itx;
    }

    private static FabricLocalExecutor localExecutor(
            Config config, Map<Location.Local, InternalTransaction> transactionMap) {
        var fabricDatabaseManager = mock(FabricDatabaseManager.class);
        transactionMap.forEach((loc, itx) -> {
            try {
                var graphDatabaseApi = mock(GraphDatabaseAPI.class, RETURNS_MOCKS);
                when(graphDatabaseApi.databaseId())
                        .thenReturn(DatabaseIdFactory.from(loc.getDatabaseName(), loc.getUuid()));
                when(graphDatabaseApi.beginTransaction(any(), any(), any(), any(), anyLong(), any(), any(), any()))
                        .thenReturn(itx);
                when(fabricDatabaseManager.getDatabaseFacade(eq(loc.getDatabaseName())))
                        .thenReturn(graphDatabaseApi);
            } catch (UnavailableException e) {
                throw new RuntimeException(e);
            }
        });

        return new FabricLocalExecutor(
                FabricConfig.from(config), fabricDatabaseManager, mock(LocalGraphTransactionIdTracker.class));
    }

    private static TransactionManager transactionManager(Config config, FabricLocalExecutor localExecutor) {
        var remoteExecutor = mock(FabricRemoteExecutor.class, RETURNS_MOCKS);
        var errorReporter = mock(ErrorReporter.class);
        var transactionMonitor = mock(FabricTransactionMonitor.class);
        var guard = mock(AvailabilityGuard.class);
        var securityLog = mock(AbstractSecurityLog.class);
        var catalogManager = mock(CatalogManager.class);
        var globalProcedures = mock(GlobalProcedures.class);
        return new TransactionManager(
                remoteExecutor,
                localExecutor,
                catalogManager,
                transactionMonitor,
                securityLog,
                Clocks.nanoClock(),
                config,
                guard,
                errorReporter,
                globalProcedures);
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
