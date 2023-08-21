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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockTracer;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.Clocks;
import org.neo4j.values.virtual.MapValue;

class KernelStatementTest {
    private final AtomicReference<CpuClock> cpuClockRef = new AtomicReference<>(CpuClock.NOT_AVAILABLE);

    @Test
    void shouldReleaseResourcesWhenForceClosed() {
        // given
        KernelTransactionImplementation transaction = mock(KernelTransactionImplementation.class);
        when(transaction.isCommitted()).thenReturn(true);
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        KernelStatement statement = createStatement(transaction);
        var cursorContext = contextFactory.create("test");
        statement.initialize(mock(LockManager.Client.class), cursorContext, 1);
        statement.acquire();

        // when
        assertThrows(KernelStatement.StatementNotClosedException.class, statement::forceClose);

        // then
        verify(transaction).releaseStatementResources();
    }

    @Test
    void reportQueryWaitingTimeToTransactionStatisticWhenFinishQueryExecution() {
        KernelTransactionImplementation transaction = mock(KernelTransactionImplementation.class);
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);

        KernelTransactionImplementation.Statistics statistics =
                new KernelTransactionImplementation.Statistics(transaction, cpuClockRef, false);
        when(transaction.getStatistics()).thenReturn(statistics);
        when(transaction.executingQuery()).thenReturn(Optional.empty());

        KernelStatement statement = createStatement(transaction);
        var cursorContext = contextFactory.create("test");
        statement.initialize(mock(LockManager.Client.class), cursorContext, 1);
        statement.acquire();

        ExecutingQuery query = getQueryWithWaitingTime();
        ExecutingQuery query2 = getQueryWithWaitingTime();
        ExecutingQuery query3 = getQueryWithWaitingTime();

        statement.stopQueryExecution(query);
        statement.stopQueryExecution(query2);
        statement.stopQueryExecution(query3);

        assertEquals(3, statistics.getWaitingTimeNanos(1));
    }

    @Test
    void emptyPageCacheStatisticOnClosedStatement() {
        var transaction = mock(KernelTransactionImplementation.class, RETURNS_DEEP_STUBS);
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        try (var statement = createStatement(transaction)) {
            var cursorContext = contextFactory.create("test");
            statement.initialize(mock(LockManager.Client.class), cursorContext, 100);
            statement.acquire();

            PageSwapper swapper = mock(PageSwapper.class, RETURNS_MOCKS);
            try (var pinEvent = cursorContext.getCursorTracer().beginPin(false, 1, swapper)) {
                pinEvent.hit();
            }
            try (var pinEvent = cursorContext.getCursorTracer().beginPin(false, 1, swapper)) {
                pinEvent.hit();
            }
            try (var pinEvent = cursorContext.getCursorTracer().beginPin(false, 1, swapper)) {
                pinEvent.beginPageFault(1, swapper).close();
            }
            assertEquals(2, statement.getHits());
            assertEquals(1, statement.getFaults());

            statement.close();

            assertEquals(0, statement.getHits());
            assertEquals(0, statement.getFaults());
        }
    }

    @Test
    void trackSequentialQueriesInStatement() {
        var queryFactory = new ExecutingQueryFactory(Clocks.nanoClock(), cpuClockRef, LockTracer.NONE);
        var transaction = mock(KernelTransactionImplementation.class, RETURNS_DEEP_STUBS);
        var statement = createStatement(transaction);
        statement.initialize(mock(LockManager.Client.class), CursorContext.NULL_CONTEXT, 100);

        var query1 = queryFactory.createForStatement(statement, "test1", MapValue.EMPTY);
        var query2 = queryFactory.createForStatement(statement, "test2", MapValue.EMPTY);
        var query3 = queryFactory.createForStatement(statement, "test3", MapValue.EMPTY);

        statement.startQueryExecution(query1);
        statement.startQueryExecution(query2);
        assertSame(query2, statement.executingQuery().orElseThrow());

        statement.startQueryExecution(query3);
        assertSame(query3, statement.executingQuery().orElseThrow());

        statement.stopQueryExecution(query3);
        assertSame(query2, statement.executingQuery().orElseThrow());

        statement.stopQueryExecution(query2);
        assertSame(query1, statement.executingQuery().orElseThrow());

        statement.stopQueryExecution(query1);
        assertFalse(statement.executingQuery().isPresent());
    }

    @Test
    void shouldKeepExecutingQueryUntilForceClosedOrReused() {
        // Given
        var queryFactory = new ExecutingQueryFactory(Clocks.nanoClock(), cpuClockRef, LockTracer.NONE);
        var transaction = mock(KernelTransactionImplementation.class, RETURNS_DEEP_STUBS);
        var statement = createStatement(transaction);
        statement.initialize(mock(LockManager.Client.class), CursorContext.NULL_CONTEXT, 100);
        var query = queryFactory.createForStatement(statement, "test1", MapValue.EMPTY);
        statement.startQueryExecution(query);

        // When/Then ForceClose
        assertThat(statement.executingQuery()).isPresent();
        statement.close();
        assertThat(statement.executingQuery()).isPresent();
        statement.forceClose();
        assertThat(statement.executingQuery()).isEmpty();

        // When/Then Init
        statement.initialize(mock(LockManager.Client.class), CursorContext.NULL_CONTEXT, 100);
        statement.startQueryExecution(query);

        assertThat(statement.executingQuery()).isPresent();
        statement.close();
        assertThat(statement.executingQuery()).isPresent();
        statement.initialize(mock(LockManager.Client.class), CursorContext.NULL_CONTEXT, 100);
        assertThat(statement.executingQuery()).isEmpty();
    }

    private KernelStatement createStatement(KernelTransactionImplementation transaction) {
        return new KernelStatement(
                transaction,
                LockTracer.NONE,
                new TransactionClockContext(),
                cpuClockRef,
                from(DEFAULT_DATABASE_NAME, UUID.randomUUID()),
                Config.defaults(GraphDatabaseInternalSettings.track_tx_statement_close, true));
    }

    private static ExecutingQuery getQueryWithWaitingTime() {
        ExecutingQuery executingQuery = mock(ExecutingQuery.class);
        when(executingQuery.reportedWaitingTimeNanos()).thenReturn(1L);
        return executingQuery;
    }
}
