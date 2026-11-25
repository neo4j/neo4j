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
package org.neo4j.bolt.tx.statement;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

class StatementImplTest {

    private DatabaseReference databaseReference;
    private SystemNanoClock clock;
    private BoltQueryExecution execution;
    private QueryExecution queryExecution;
    private QueryExecutionType queryExecutionType;
    private StatementQuerySubscriber querySubscriber;

    @BeforeEach
    void prepare() {
        this.databaseReference = Mockito.mock(DatabaseReference.class);
        this.clock = Clocks.fakeClock();
        this.execution = Mockito.mock(BoltQueryExecution.class);
        this.queryExecution = Mockito.mock(QueryExecution.class);
        this.querySubscriber = Mockito.mock(StatementQuerySubscriber.class);

        Mockito.doReturn(this.queryExecution).when(this.execution).queryExecution();

        var fieldNames = new String[] {"firstName", "lastName"};
        Mockito.doReturn(fieldNames).when(this.queryExecution).fieldNames();
    }

    @Test
    void shouldReturnPassedId() {
        var statement = new StatementImpl(42, this.databaseReference, this.clock, this.execution, this.querySubscriber);

        Assertions.assertThat(statement.id()).isEqualTo(42);
    }

    @Test
    void shouldCacheFieldNames() {
        var statement = new StatementImpl(42, this.databaseReference, this.clock, this.execution, this.querySubscriber);

        Mockito.verify(this.execution).queryExecution();
        Mockito.verify(this.queryExecution).fieldNames();

        Assertions.assertThat(statement.fieldNames()).containsExactly("firstName", "lastName");

        Mockito.verifyNoMoreInteractions(this.execution);
        Mockito.verifyNoMoreInteractions(this.queryExecution);
    }

    @Test
    void shouldTerminate() {
        var statement = new StatementImpl(42, this.databaseReference, this.clock, this.execution, this.querySubscriber);

        Mockito.verify(this.execution).queryExecution();
        Mockito.verify(this.queryExecution).fieldNames();

        Assertions.assertThat(statement.hasRemaining()).isTrue();

        statement.terminate();

        var inOrder = Mockito.inOrder(this.execution, this.queryExecution);

        inOrder.verify(this.execution).terminate();
        inOrder.verifyNoMoreInteractions();

        Assertions.assertThat(statement.hasRemaining()).isFalse();
    }

    @Test
    void shouldClose() throws Exception {
        var statement = new StatementImpl(42, this.databaseReference, this.clock, this.execution, this.querySubscriber);

        Mockito.verify(this.execution).queryExecution();
        Mockito.verify(this.queryExecution).fieldNames();

        Assertions.assertThat(statement.hasRemaining()).isTrue();

        statement.close();

        var inOrder = Mockito.inOrder(this.execution, this.queryExecution);

        inOrder.verify(this.execution).queryExecution();
        inOrder.verify(this.queryExecution).cancel();
        inOrder.verify(this.queryExecution).awaitCleanup();
        inOrder.verify(this.execution).close();
        inOrder.verifyNoMoreInteractions();

        Assertions.assertThat(statement.hasRemaining()).isFalse();
    }

    /**
     * Ensures that the time spent streaming results (reported as {@code t_last}) is measured using a
     * monotonic time source and is thus unaffected by adjustments to the wall clock
     * while results are being consumed.
     */
    @Test
    void shouldMeasureStreamingTimeUsingMonotonicClock() throws Exception {
        var clock = new SteppableWallClock();
        var responseHandler = Mockito.mock(ResponseHandler.class);

        Mockito.doReturn(QueryExecutionType.query(QueryExecutionType.QueryType.READ_ONLY))
                .when(this.queryExecution)
                .executionType();
        Mockito.doAnswer(invocation -> {
                    clock.forward(42, TimeUnit.MILLISECONDS);
                    clock.stepWallClock(Duration.ofSeconds(-41));
                    return false;
                })
                .when(this.queryExecution)
                .await();

        var statement = new StatementImpl(42, this.databaseReference, clock, this.execution, this.querySubscriber);
        statement.consume(responseHandler, -1);

        Assertions.assertThat(statement.executionTime()).isEqualTo(42);
        Mockito.verify(responseHandler)
                .onStreamingMetadata(
                        Mockito.eq(42L), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    /**
     * Fake clock which permits its wall clock to be stepped independently of its monotonic time
     * source.
     */
    private static final class SteppableWallClock extends FakeClock {
        private long wallClockOffsetMillis;

        void stepWallClock(Duration delta) {
            this.wallClockOffsetMillis += delta.toMillis();
        }

        @Override
        public long millis() {
            return super.millis() + this.wallClockOffsetMillis;
        }
    }
}
