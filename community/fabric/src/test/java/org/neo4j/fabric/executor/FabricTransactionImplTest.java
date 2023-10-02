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
package org.neo4j.fabric.executor;

import static java.util.Collections.emptyMap;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.ArgumentMatchers.any;
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

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.fabric.transaction.FabricTransactionMonitor;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitor;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.FakeClock;

public class FabricTransactionImplTest {
    private static final FakeClock clock = new FakeClock();
    private static final Log log = mock( Log.class );

    @Test
    void testChildrenAreTerminated() {
        var config = Config.defaults();
        var bookmarkManager = mock(TransactionBookmarkManager.class);

        var loc1 = new Location.Local(1, UUID.randomUUID(), "one");
        var loc2 = new Location.Local(2, UUID.randomUUID(), "two");
        var loc3 = new Location.Local(3, UUID.randomUUID(), "three");

        var itx1 = internalTransaction(true, null);
        var itx2 = internalTransaction(true, null);
        var itx3 = internalTransaction(true, null);

        var transactionManager =
                transactionManager(config, localExecutor(config, Map.of(loc1, itx1, loc2, itx2, loc3, itx3)));
        var tx = transactionManager.begin(createTransactionInfo(), bookmarkManager);

        tx.execute(ctx -> {
            var local = ctx.getLocal();
            local.getOrCreateTx(loc1, DEFINITELY_READ);
            local.getOrCreateTx(loc2, DEFINITELY_READ);
            local.getOrCreateTx(loc3, DEFINITELY_READ);
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

        var loc1 = new Location.Local(1, UUID.randomUUID(), "one");
        var loc2 = new Location.Local(2, UUID.randomUUID(), "two");
        var loc3 = new Location.Local(3, UUID.randomUUID(), "three");

        var itx1 = internalTransaction(true, null);
        var itx2 = internalTransaction(false, null);
        var itx3 = internalTransaction(true, null);

        var transactionManager =
                transactionManager(config, localExecutor(config, Map.of(loc1, itx1, loc2, itx2, loc3, itx3)));
        var tx = transactionManager.begin(createTransactionInfo(), bookmarkManager);

        tx.execute(ctx -> {
            var local = ctx.getLocal();
            local.getOrCreateTx(loc1, DEFINITELY_READ);
            local.getOrCreateTx(loc2, DEFINITELY_READ);
            local.getOrCreateTx(loc3, DEFINITELY_READ);
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

        var loc1 = new Location.Local(1, UUID.randomUUID(), "one");
        var loc2 = new Location.Local(2, UUID.randomUUID(), "two");
        var loc3 = new Location.Local(3, UUID.randomUUID(), "three");

        var itx1 = internalTransaction(true, null);
        var itx2 = internalTransaction(true, TransactionTimedOut);
        var itx3 = internalTransaction(true, null);

        var transactionManager =
                transactionManager(config, localExecutor(config, Map.of(loc1, itx1, loc2, itx2, loc3, itx3)));
        var tx = transactionManager.begin(createTransactionInfo(), bookmarkManager);

        tx.execute(ctx -> {
            var local = ctx.getLocal();
            local.getOrCreateTx(loc1, DEFINITELY_READ);
            local.getOrCreateTx(loc2, DEFINITELY_READ);
            local.getOrCreateTx(loc3, DEFINITELY_READ);
            tx.markForTermination(Terminated);

            verify(itx1).terminate(Terminated);
            verify(itx2, never()).terminate(Terminated);
            verify(itx3).terminate(Terminated);

            return null;
        });
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
                var graphDatabaseFacade = mock( GraphDatabaseFacade.class, RETURNS_MOCKS);
                when(graphDatabaseFacade.databaseId())
                        .thenReturn(DatabaseIdFactory.from(loc.getDatabaseName(), loc.getUuid()));
                when(graphDatabaseFacade.beginTransaction(any(), any(), any(), any(), any()))
                        .thenReturn(itx);
                when(fabricDatabaseManager.getDatabaseFacade(eq(loc.getDatabaseName())))
                        .thenReturn(graphDatabaseFacade);
                DependencyResolver dependencyResolverMock = mock( DependencyResolver.class );
                when(dependencyResolverMock.resolveDependency( GraphDatabaseQueryService.class )).thenReturn( mock(GraphDatabaseQueryService.class, RETURNS_MOCKS) );
                when(graphDatabaseFacade.getDependencyResolver()).thenReturn( dependencyResolverMock );
            } catch (UnavailableException e) {
                throw new RuntimeException(e);
            }
        });

        return new FabricLocalExecutor( FabricConfig.from(config), fabricDatabaseManager, FabricDatabaseAccess.NO_RESTRICTION);
    }

    private static TransactionManager transactionManager( Config config, FabricLocalExecutor localExecutor) {
        var remoteExecutor = mock(FabricRemoteExecutor.class, RETURNS_MOCKS);
        var errorReporter = mock(ErrorReporter.class);
        var transactionMonitor = mock(FabricTransactionMonitor.class);
        var guard = mock(AvailabilityGuard.class);
        var securityLog = mock(AbstractSecurityLog.class);
        var catalogManager = mock(CatalogManager.class);
        var fabricConfig = new FabricConfig( () -> Duration.ZERO, null, false, true );

        var logService = mock( LogService.class );
        when(logService.getInternalLog( TransactionMonitor.class )).thenReturn( log );
        return new TransactionManager(
                remoteExecutor,
                localExecutor,
                catalogManager,
                fabricConfig,
                transactionMonitor,
                securityLog,
                clock,
                config,
                guard,
                errorReporter );
    }

    private static FabricTransactionInfo createTransactionInfo() {
        var databaseName = new NormalizedDatabaseName("a");
        var databaseId = DatabaseIdFactory.from(databaseName.name(), UUID.randomUUID());
        var databaseRef = new DatabaseReference.Internal(databaseName, databaseId);
        return new FabricTransactionInfo(
                AccessMode.READ,
                LoginContext.AUTH_DISABLED,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                databaseRef,
                false,
                Duration.ZERO,
                emptyMap(),
                new RoutingContext( true, emptyMap()));
    }
}