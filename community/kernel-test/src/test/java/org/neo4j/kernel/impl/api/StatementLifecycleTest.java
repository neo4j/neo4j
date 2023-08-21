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
package org.neo4j.kernel.impl.api;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockTracer;
import org.neo4j.resources.CpuClock;

class StatementLifecycleTest {
    @Test
    void shouldReleaseStoreStatementOnlyWhenReferenceCountDownToZero() {
        // given
        KernelTransactionImplementation transaction = mock(KernelTransactionImplementation.class);
        KernelStatement statement = createStatement(transaction);
        statement.acquire();
        statement.acquire();

        // when
        statement.close();
        verify(transaction, never()).releaseStatementResources();

        // then
        statement.close();
        verify(transaction).releaseStatementResources();
    }

    @Test
    void shouldReleaseStoreStatementWhenForceClosingStatements() {
        // given
        KernelTransactionImplementation transaction = mock(KernelTransactionImplementation.class);
        when(transaction.isCommitted()).thenReturn(true);
        KernelStatement statement = createStatement(transaction);
        statement.acquire();

        // when
        assertThrows(KernelStatement.StatementNotClosedException.class, statement::forceClose);

        // then
        verify(transaction).releaseStatementResources();
    }

    private static KernelStatement createStatement(KernelTransactionImplementation transaction) {
        var statement = new KernelStatement(
                transaction,
                LockTracer.NONE,
                new TransactionClockContext(),
                new AtomicReference<>(CpuClock.NOT_AVAILABLE),
                from(DEFAULT_DATABASE_NAME, UUID.randomUUID()),
                Config.defaults(GraphDatabaseInternalSettings.track_tx_statement_close, true));
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        var cursorContext = contextFactory.create("test");
        statement.initialize(mock(LockManager.Client.class), cursorContext, 1);
        return statement;
    }
}
