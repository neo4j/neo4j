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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.event.CopyOnWriteEventPublisher;
import org.neo4j.bolt.event.EventPublisher;
import org.neo4j.bolt.tx.error.TransactionCloseException;
import org.neo4j.bolt.tx.error.TransactionCompletionException;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.error.statement.StatementException;
import org.neo4j.bolt.tx.error.statement.StatementExecutionException;
import org.neo4j.bolt.tx.statement.Statement;
import org.neo4j.bolt.tx.statement.StatementImpl;
import org.neo4j.bolt.tx.statement.StatementQuerySubscriber;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.values.virtual.MapValue;

public class TransactionImpl implements Transaction {
    private final String id;
    private final TransactionType type;
    private final DatabaseReference database;
    private final Clock clock;
    private final BoltTransaction transaction;
    private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);
    private final AtomicBoolean interrupted = new AtomicBoolean();
    private final EventPublisher<Listener> eventPublisher = new CopyOnWriteEventPublisher<>();

    /**
     * Protects the statement maps from memory corruption when accessed concurrently.
     */
    private final Lock statementLock = new ReentrantLock();

    private final AtomicLong nextStatementId = new AtomicLong();
    private volatile long latestStatementId;
    private final StatementCleanupListener statementListener = new StatementCleanupListener();
    private final Map<Long, Statement> statementMap = new HashMap<>();

    public TransactionImpl(
            String id, TransactionType type, DatabaseReference database, Clock clock, BoltTransaction transaction) {
        this.id = id;
        this.type = type;
        this.database = database;
        this.clock = clock;
        this.transaction = transaction;
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public TransactionType type() {
        return this.type;
    }

    @Override
    public boolean isOpen() {
        return this.state.get() == State.OPEN;
    }

    @Override
    public boolean isValid() {
        return this.state.get() != State.CLOSED;
    }

    @Override
    public long latestStatementId() {
        return this.latestStatementId;
    }

    @Override
    public boolean hasOpenStatement() {
        return !this.statementMap.isEmpty();
    }

    @Override
    public Statement run(String statement, MapValue params) throws StatementException {
        System.out.println("Query is : " + statement);
        long t1 = System.nanoTime();
        var statementId = this.nextStatementId.getAndIncrement();
        this.latestStatementId = statementId;

        var subscriber = new StatementQuerySubscriber();
        BoltQueryExecution query;
        try {
            long t2 = System.nanoTime();
            query = this.transaction.executeQuery(statement, params, true, subscriber);
            System.out.println("Total Server Query Execution Time : " + (System.nanoTime() - t2));
            // Note: Some special queries such as pure writes will be eagerly executed by Fabric
            // thus causing errors to be surfaced prior to "actual" execution
            subscriber.assertSuccess();
        } catch (Exception ex) {
            throw new StatementExecutionException(ex);
        }
        var handle = new StatementImpl(statementId, this.database, this.clock, query, subscriber);

        // register a lifecycle listener with the new statement to ensure that we are notified once
        // the statement is closed by its owner
        handle.registerListener(this.statementListener);

        this.statementLock.lock();
        try {
            this.statementMap.put(statementId, handle);
        } finally {
            this.statementLock.unlock();
        }

        System.out.println("Total Server Query Time : " + (System.nanoTime() - t1));
        return handle;
    }

    @Override
    public Optional<Statement> getStatement(long id) {
        this.statementLock.lock();
        try {
            return Optional.ofNullable(this.statementMap.get(id));
        } finally {
            this.statementLock.unlock();
        }
    }

    @Override
    public String commit() throws TransactionException {
        var updatedValue = this.state.compareAndExchange(State.OPEN, State.COMMITTED);
        if (updatedValue != State.OPEN) {
            throw new TransactionCompletionException(
                    "Transaction \"" + this.id + "\" has already terminated with state " + updatedValue);
        }

        try {
            this.transaction.commit();
        } catch (Exception ex) {
            // TODO: Fabric currently surfaces its own FabricException which is not visible to us
            //       within this module. This somewhat violates the API contract thus requiring us
            //       to catch Exception instead.
            throw new TransactionCompletionException("Failed to commit transaction \"" + this.id + "\"", ex);
        }

        var bookmark = this.transaction.getBookmark();
        this.eventPublisher.dispatch(l -> l.onCommit(this, bookmark));
        return bookmark;
    }

    @Override
    public void rollback() throws TransactionException {
        var updatedValue = this.state.compareAndExchange(State.OPEN, State.ROLLED_BACK);
        if (updatedValue != State.OPEN) {
            throw new TransactionCompletionException(
                    "Transaction \"" + this.id + "\" has already terminated with state " + updatedValue);
        }

        this.statementLock.lock();
        try {
            this.statementMap.values().forEach(statement -> {
                try {
                    statement.terminate();
                } catch (Exception ignore) {
                }
            });
        } finally {
            this.statementLock.unlock();
        }

        try {
            this.transaction.rollback();
        } catch (Exception ex) {
            // TODO: Fabric currently surfaces its own FabricException which is not visible to us
            //       within this module. This somewhat violates the API contract thus requiring us
            //       to catch Exception instead.
            throw new TransactionCompletionException("Failed to rollback transaction \"" + this.id + "\"", ex);
        }

        this.eventPublisher.dispatch(l -> l.onRollback(this));
    }

    @Override
    public void interrupt() {
        // ensure that this is the first and only thread to interrupt this transaction, all
        // subsequent calls will simply be ignored as the desired state has already been achieved
        if (!this.interrupted.compareAndSet(false, true)) {
            return;
        }

        // mark the transaction itself for termination at the closest possible time in order to
        // prevent it from progressing any further
        this.transaction.markForTermination(Status.Transaction.Terminated);
    }

    @Override
    public boolean validate() {
        var reason = this.transaction
                .getReasonIfTerminated()
                .filter(status -> status.code().classification().rollbackTransaction());

        return reason.isEmpty();
    }

    @Override
    public void close() throws TransactionCloseException {
        // attempt to swap the transaction state to closed - this effectively acts as a lock for our
        // cleanup procedure as only a single thread will ever succeed in swapping the value to its new
        // state
        State previousState;
        do {
            previousState = this.state.get();

            // if the transaction has already been closed by another thread, we'll abort here as there is
            // no more work for us to do
            if (previousState == State.CLOSED) {
                return;
            }
        } while (!this.state.compareAndSet(previousState, State.CLOSED));

        // if statements remain within this transaction, we'll have to close them as well before we
        // move on to terminating the transaction
        this.statementLock.lock();
        try {
            var statements = Set.copyOf(this.statementMap.values());
            for (var statement : statements) {
                try {
                    statement.close();
                } catch (Exception ignore) {
                }
            }
        } finally {
            this.statementLock.unlock();
        }

        // if the transaction has not been explicitly committed or rolled back prior to being
        // closed, we'll force a rollback to cleanly terminate the transaction rather than just
        // closing it in its undefined state
        if (previousState == State.OPEN) {
            try {
                this.transaction.rollback();
            } catch (TransactionFailureException ignore) {
                // any failures here are simply ignored - we expect the lower components to clean up
                // this mess (or report a failure) once the transaction is finally closed further
                // down
            }
        }

        try {
            this.transaction.close();
        } catch (TransactionFailureException ex) {
            throw new TransactionCloseException("Failed to close transaction \"" + this.id + "\"", ex);
        }

        this.eventPublisher.dispatchSafe(l -> l.onClose(this));
    }

    @Override
    public void registerListener(Listener listener) {
        this.eventPublisher.registerListener(listener);
    }

    @Override
    public void removeListener(Listener listener) {
        this.eventPublisher.removeListener(listener);
    }

    private enum State {
        OPEN,
        COMMITTED,
        ROLLED_BACK,
        CLOSED
    }

    private class StatementCleanupListener implements Statement.Listener {

        @Override
        public void onClosed(Statement statement) {
            statementLock.lock();
            try {
                statementMap.remove(statement.id());
            } finally {
                statementLock.unlock();
            }
        }
    }
}
