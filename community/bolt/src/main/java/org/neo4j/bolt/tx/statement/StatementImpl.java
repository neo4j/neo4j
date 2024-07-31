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

import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.event.CopyOnWriteEventPublisher;
import org.neo4j.bolt.event.EventPublisher;
import org.neo4j.bolt.protocol.common.fsm.response.NoopRecordHandler;
import org.neo4j.bolt.protocol.common.fsm.response.RecordHandler;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.statement.StatementException;
import org.neo4j.bolt.tx.error.statement.StatementStreamingException;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryExecutionType.QueryType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

public class StatementImpl implements Statement {
    // TODO: Is this really a good idea? Are we sure about that?
    private static final long DEFAULT_BATCH_SIZE = Long.MAX_VALUE;

    private final long id;
    private final DatabaseReference database;
    private final Clock clock;
    private final StatementQuerySubscriber subscriber;
    private final BoltQueryExecution execution;
    private final EventPublisher<Statement.Listener> eventPublisher = new CopyOnWriteEventPublisher<>();

    /**
     * Provides a lock which safeguards consumption of results on this statement.
     * <p />
     * Generally, this implementation permits interactions from multiple threads. However, results
     * can only ever be consumed once thus requiring synchronization to safeguard the consumption
     * state of this statement.
     */
    private final Lock executionLock = new ReentrantLock();

    /**
     * Stores the current canonical state of the statement.
     * <p />
     * This reference is used as a lock in order to facilitate safe termination and freeing of
     * resources associated with this statement.
     */
    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

    private long timeSpentStreaming;
    private final List<String> fieldNames;
    private QueryStatistics statistics;

    public StatementImpl(
            long id,
            DatabaseReference database,
            Clock clock,
            BoltQueryExecution execution,
            StatementQuerySubscriber subscriber) {
        this.id = id;
        this.database = database;
        this.clock = clock;
        this.execution = execution;
        this.subscriber = subscriber;

        this.fieldNames = Arrays.asList(execution.queryExecution().fieldNames());
    }

    @Override
    public long id() {
        return this.id;
    }

    @Override
    public List<String> fieldNames() {
        return this.fieldNames;
    }

    @Override
    public long executionTime() {
        return this.timeSpentStreaming;
    }

    @Override
    public Optional<QueryStatistics> statistics() {
        return Optional.ofNullable(this.statistics);
    }

    @Override
    public boolean hasRemaining() {
        // statements are considered to have remaining results so long as they have not been
        // terminated or closed and while an end has not been explicitly encountered while consuming
        // results
        return this.state.get() == State.RUNNING;
    }

    @Override
    public void consume(ResponseHandler responseHandler, long n) throws StatementException {
        // ensure that the statement has remaining records and has not been terminated
        var state = this.state.get();
        if (state != State.RUNNING) {
            return;
        }

        this.executionLock.lock();
        try {
            var recordHandler = responseHandler.onBeginStreaming(this.fieldNames);
            this.subscriber.setHandler(recordHandler);

            long start = this.clock.millis();
            var query = this.execution.queryExecution();

            // if the caller requested for all possible results to be streamed within a single operation,
            // we'll just loop until the query indicates that no more data is available
            // TODO: Is this also -1 in protocol? Why?!?
            if (n == -1) {
                try {
                    boolean remaining;
                    do {
                        query.request(DEFAULT_BATCH_SIZE);
                        remaining = query.await();

                        this.subscriber.assertSuccess();
                    } while (remaining);
                } catch (Exception ex) {
                    throw new StatementStreamingException("Failed to consume all statement results", ex);
                }

                this.timeSpentStreaming += this.clock.millis() - start;

                // if possible, update the statement state to indicate that there is no more results
                // waiting for consumption
                this.complete(responseHandler, this.subscriber.getStatistics());
                responseHandler.onCompleteStreaming(false);
            } else {
                // otherwise we'll request the specific amount of results requested
                boolean remaining;
                try {
                    query.request(n);
                    remaining = query.await();

                    this.subscriber.assertSuccess();
                } catch (Exception ex) {
                    throw new StatementStreamingException("Failed to consume statement results", ex);
                }

                this.timeSpentStreaming += this.clock.millis() - start;

                // if there are no remaining results to be consumed, attempt to update the state to
                // reflect the new state
                if (!remaining) {
                    this.complete(responseHandler, this.subscriber.getStatistics());
                }

                responseHandler.onCompleteStreaming(remaining);
            }

            this.subscriber.setHandler(null);

            var pendingException = this.subscriber.getPendingException();
            if (pendingException != null) {
                throw new StatementStreamingException(pendingException);
            }
        } finally {
            this.executionLock.unlock();
        }
    }

    @Override
    public void discard(ResponseHandler responseHandler, long n) throws StatementException {
        // ensure that the statement has remaining records and has not been terminated
        var state = this.state.get();
        if (state != State.RUNNING) {
            return;
        }

        this.executionLock.lock();
        try {
            long start = this.clock.millis();
            var query = this.execution.queryExecution();

            // if the query has no side effects, and we wish to discard all remaining results, we'll
            // simply terminate it
            if (n == -1
                    && query.executionMetadataAvailable()
                    && query.executionType().queryType() == QueryType.READ_ONLY) {
                responseHandler.onBeginStreaming(this.fieldNames);

                try {
                    query.cancel();
                    query.await();
                } catch (Exception ex) {
                    throw new StatementStreamingException("Failed to discard results", ex);
                }

                this.timeSpentStreaming += this.clock.millis() - start;

                // since there's no remaining records within this statement, swap its state from
                // running to completed - if this swap fails, we'll simply ignore it as the owner of
                // this statement will free the associated resources
                this.complete(responseHandler, QueryStatistics.EMPTY);
                responseHandler.onCompleteStreaming(false);
            } else {
                this.consume(new DiscardingRecordConsumer(responseHandler), n);
            }
        } finally {
            this.executionLock.unlock();
        }
    }

    private void complete(ResponseHandler handler, QueryStatistics statistics) {
        this.statistics = statistics;

        var execution = this.execution.queryExecution();

        handler.onStreamingMetadata(
                this.timeSpentStreaming,
                execution.executionType(),
                this.database,
                this.statistics,
                execution.getNotifications(),
                execution.getGqlStatusObjects());

        var executionType = execution.executionType();

        if (executionType.requestedExecutionPlanDescription()) {
            handler.onStreamingExecutionPlan(execution.executionPlanDescription());
        }

        if (this.state.compareAndSet(State.RUNNING, State.COMPLETED)) {
            this.eventPublisher.dispatch(l -> l.onCompleted(this));
        }
    }

    /**
     * Attempts to update the current statement state to the desired target state.
     *
     * @param targetState a target state.
     * @param filter a filter function which indicates whether a transition from the current state
     *               is permitted.
     * @return true if the update succeeded, false otherwise.
     */
    private boolean updateState(State targetState, Predicate<State> filter) {
        State previousState;
        do {
            previousState = this.state.get();

            // if the statement is already in an undesirable state, we'll abort and let the caller
            // figure out how it wants to proceed with this
            if (!filter.test(previousState)) {
                return false;
            }
        } while (!this.state.compareAndSet(previousState, targetState));

        return true;
    }

    @Override
    public void terminate() {
        // swap the state to terminated to ensure that this is the first and only thread to
        // terminate this statement
        if (!this.updateState(State.TERMINATED, state -> state != State.TERMINATED && state != State.CLOSED)) {
            return;
        }

        // request execution termination from the underlying implementation - this should also call
        // any pending operations to be terminated (hence the lack of protection within this
        // context)
        this.execution.terminate();

        // notify all subscribed listeners ignoring any generated exceptions as we want to ensure
        // that cleanup code completes gracefully
        // TODO: Logging
        this.eventPublisher.dispatchSafe(l -> l.onTerminated(this));
    }

    @Override
    public void close() {
        // swap the state to closed to ensure that this is the first and only thread to close this
        // statement
        if (!this.updateState(State.CLOSED, state -> state != State.CLOSED)) {
            return;
        }

        // request termination of this statement prior to attempting to free its resources in order
        // to gracefully terminate in-flight result consumption (if an)
        try {
            this.execution.queryExecution().cancel();
            this.execution.queryExecution().awaitCleanup();
        } catch (Exception ignore) {
            // we'll ignore any errors raised while awaiting graceful termination as we wish to free
            // the remaining resources regardless
        }

        // free the remaining resources within this statement from the protection of the consumption
        // lock - this is necessary as we do not wish to close the statement while another thread
        // is still consuming results
        this.executionLock.lock();
        try {
            this.execution.close();
        } finally {
            this.executionLock.unlock();
        }

        // notify all subscribed listeners ignoring any generated exceptions as we want to ensure
        // that cleanup code completes gracefully
        // TODO: Logging
        this.eventPublisher.dispatchSafe(l -> l.onClosed(this));
    }

    @Override
    public void registerListener(Listener listener) {
        this.eventPublisher.registerListener(listener);
    }

    @Override
    public void removeListener(Listener listener) {
        this.eventPublisher.removeListener(listener);
    }

    static final class DiscardingRecordConsumer implements ResponseHandler {
        private final ResponseHandler delegate;

        public DiscardingRecordConsumer(ResponseHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onStatementPrepared(
                TransactionType transactionType,
                long statementId,
                long timeSpentPreparingResults,
                List<String> fieldNames) {
            // never occurs within this context
        }

        @Override
        public void onMetadata(String key, AnyValue value) {
            // TODO: Technically unused - Hide MetadataConsumer from this context
            this.delegate.onMetadata(key, value);
        }

        @Override
        public RecordHandler onBeginStreaming(List<String> fieldNames) {
            // we'll notify the handler about the ongoing streaming but ignore the return value as
            // any records will be discarded internally through our NOOP handler
            this.delegate.onBeginStreaming(fieldNames);

            return NoopRecordHandler.getInstance();
        }

        @Override
        public void onStreamingMetadata(
                long timeSpentStreaming,
                QueryExecutionType executionType,
                DatabaseReference database,
                QueryStatistics statistics,
                Iterable<Notification> notifications,
                Iterable<GqlStatusObject> statuses) {
            this.delegate.onStreamingMetadata(
                    timeSpentStreaming, executionType, database, statistics, notifications, statuses);
        }

        @Override
        public void onStreamingExecutionPlan(ExecutionPlanDescription plan) {}

        @Override
        public void onCompleteStreaming(boolean hasRemaining) {
            this.delegate.onCompleteStreaming(hasRemaining);
        }

        @Override
        public void onRoutingTable(String databaseName, MapValue routingTable) {
            // never occurs within this context
        }

        @Override
        public void onBookmark(String encodedBookmark) {
            // never occurs within this context
        }

        @Override
        public void onFailure(Error error) {
            this.delegate.onFailure(error);
        }

        @Override
        public void onIgnored() {
            this.delegate.onIgnored();
        }

        @Override
        public void onSuccess() {
            this.delegate.onSuccess();
        }
    }

    enum State {
        RUNNING,
        COMPLETED,
        TERMINATED,
        CLOSED
    }
}
