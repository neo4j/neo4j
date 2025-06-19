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
package org.neo4j.fabric.transaction.parent;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionRollbackFailed;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTerminationFailed;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.fabric.executor.Exceptions;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.TransactionTerminatedHelper;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.scheduler.CallableExecutor;
import org.neo4j.time.SystemNanoClock;

/**
 * Implements transaction actions for transactions that consist of child transactions
 */
public abstract class AbstractCompoundTransaction<Child extends ChildTransaction>
        implements CompoundTransaction<Child> {

    private final CallableExecutor executor;
    private final ErrorReporter errorReporter;
    private final SystemNanoClock clock;

    private final ReadWriteLock transactionLock = new ReentrantReadWriteLock();
    protected final Lock nonExclusiveLock = transactionLock.readLock();
    protected final Lock exclusiveLock = transactionLock.writeLock();

    protected State state = State.OPEN;
    protected TerminationMark terminationMark;

    private final Set<AutocommitQuery> autocommitQueries = ConcurrentHashMap.newKeySet();
    protected final Set<ReadingChildTransaction<Child>> readingTransactions = ConcurrentHashMap.newKeySet();
    protected Child writingTransaction;

    private record ReadingChildTransaction<Tx>(Tx inner, boolean readingOnly) {}

    protected enum State {
        OPEN,
        CLOSED,
        TERMINATED
    }

    protected static class ErrorRecord {
        private final String message;
        private final Throwable error;
        private final ErrorGqlStatusObject gqlStatusObject;

        private ErrorRecord(ErrorGqlStatusObject gql, String message, Throwable error) {
            this.message = message;
            this.error = error;
            this.gqlStatusObject = gql;
        }

        public String message() {
            return message;
        }

        public Throwable error() {
            return error;
        }

        public ErrorGqlStatusObject gqlStatusObject() {
            return gqlStatusObject;
        }

        public static ErrorRecord commitFailed(String message, Throwable error) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_2DN01)
                    .build();
            return new ErrorRecord(gql, message, error);
        }

        public static ErrorRecord constituentCommitFailed(String message, Throwable error) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_2DN02)
                    .build();
            return new ErrorRecord(gql, message, error);
        }

        public static ErrorRecord rollbackFailed(String message, Throwable error) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_40N01)
                    .build();
            return new ErrorRecord(gql, message, error);
        }

        public static ErrorRecord constituentRollbackFailed(String message, Throwable error) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_40N02)
                    .build();
            return new ErrorRecord(gql, message, error);
        }

        public static ErrorRecord transactionTerminateFailed(String message, Throwable error) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_2DN03)
                    .build();
            return new ErrorRecord(gql, message, error);
        }

        public static ErrorRecord constituentTransactionTerminationFailed(String message, Throwable error) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_2DN04)
                    .build();
            return new ErrorRecord(gql, message, error);
        }
    }

    protected AbstractCompoundTransaction(
            ErrorReporter errorReporter, SystemNanoClock clock, CallableExecutor executor) {
        this.errorReporter = errorReporter;
        this.clock = clock;
        this.executor = executor;
    }

    @Override
    public <Tx extends Child> Tx registerNewChildTransaction(
            Location location, TransactionMode mode, Supplier<Tx> transactionSupplier) throws FabricException {
        return switch (mode) {
            case DEFINITELY_WRITE -> startWritingTransaction(location, transactionSupplier);
            case MAYBE_WRITE -> startReadingTransaction(false, transactionSupplier);
            case DEFINITELY_READ -> startReadingTransaction(true, transactionSupplier);
        };
    }

    private <Tx extends Child> Tx startWritingTransaction(Location location, Supplier<Tx> writeTransactionSupplier)
            throws FabricException {
        exclusiveLock.lock();
        try {
            checkTransactionOpenForStatementExecution();

            if (writingTransaction != null) {
                throw multipleWriteError(location, writingTransaction.location());
            }

            var tx = writeTransactionSupplier.get();
            writingTransaction = tx;
            return tx;
        } finally {
            exclusiveLock.unlock();
        }
    }

    private <TX extends Child> TX startReadingTransaction(boolean readOnly, Supplier<TX> readingTransactionSupplier)
            throws FabricException {
        nonExclusiveLock.lock();
        try {
            checkTransactionOpenForStatementExecution();

            var tx = readingTransactionSupplier.get();
            readingTransactions.add(new ReadingChildTransaction<>(tx, readOnly));
            return tx;
        } finally {
            nonExclusiveLock.unlock();
        }
    }

    @Override
    public <Tx extends Child> void upgradeToWritingTransaction(Tx childTransaction) throws FabricException {
        if (this.writingTransaction == childTransaction) {
            return;
        }

        exclusiveLock.lock();
        try {
            if (this.writingTransaction == childTransaction) {
                return;
            }

            if (this.writingTransaction != null) {
                throw multipleWriteError(childTransaction.location(), this.writingTransaction.location());
            }

            var readingTransaction = readingTransactions.stream()
                    .filter(readingTx -> readingTx.inner == childTransaction)
                    .findAny()
                    .orElseThrow(
                            () -> new IllegalArgumentException("The supplied transaction has not been registered"));

            if (readingTransaction.readingOnly) {
                throw new IllegalStateException("Upgrading reading-only transaction to a writing one is not allowed");
            }

            readingTransactions.remove(readingTransaction);
            this.writingTransaction = readingTransaction.inner;
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public void commit() {
        exclusiveLock.lock();
        try {
            if (state == State.TERMINATED) {
                // Wait for all children to be rolled back. Ignore errors
                doRollbackAndIgnoreErrors(this::childTransactionRollback);
                throw TransactionTerminatedHelper.transactionTerminated(terminationMark.getReason());
            }

            if (state == State.CLOSED) {
                throw FabricException.transactionCommitFailed(
                        TransactionCommitFailed, "Trying to commit closed transaction");
            }

            state = State.CLOSED;

            var allFailures = new ArrayList<ErrorRecord>();

            try {
                doOnChildren(readingTransactions, null, this::childTransactionCommit)
                        .forEach(error -> allFailures.add(ErrorRecord.constituentCommitFailed(
                                "Failed to commit a child read transaction", error)));

                if (!allFailures.isEmpty()) {
                    doOnChildren(List.of(), writingTransaction, this::childTransactionRollback)
                            .forEach(error -> allFailures.add(ErrorRecord.constituentRollbackFailed(
                                    "Failed to rollback a child write transaction", error)));
                } else {
                    doOnChildren(List.of(), writingTransaction, this::childTransactionCommit)
                            .forEach(error -> allFailures.add(ErrorRecord.constituentCommitFailed(
                                    "Failed to commit a child write transaction", error)));
                }
            } catch (Exception e) {
                allFailures.add(
                        ErrorRecord.commitFailed("Failed to commit composite transaction", commitFailedError()));
            } finally {
                closeContextsAndRemoveTransaction();
            }

            throwIfNonEmpty(allFailures, TransactionCommitFailed);
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public void rollback() {
        exclusiveLock.lock();
        try {
            // guard against someone calling rollback after 'begin' failure
            if (isUninitialized()) {
                return;
            }

            if (state == State.TERMINATED) {
                // Wait for all children to be rolled back. Ignore errors
                doRollbackAndIgnoreErrors(this::childTransactionRollback);
                return;
            }

            if (state == State.CLOSED) {
                return;
            }

            state = State.CLOSED;
            doRollback(this::childTransactionRollback);
        } finally {
            exclusiveLock.unlock();
        }
    }

    private void doRollback(Consumer<Child> operation) {
        var allFailures = new ArrayList<ErrorRecord>();

        try {
            doOnChildren(readingTransactions, writingTransaction, operation)
                    .forEach(error -> allFailures.add(
                            ErrorRecord.constituentRollbackFailed("Failed to rollback a child transaction", error)));
        } catch (Exception e) {
            allFailures.add(
                    ErrorRecord.rollbackFailed("Failed to rollback composite transaction", rollbackFailedError()));
        } finally {
            closeContextsAndRemoveTransaction();
        }

        throwIfNonEmpty(allFailures, TransactionRollbackFailed);
    }

    private void doRollbackAndIgnoreErrors(Consumer<Child> operation) {
        try {
            doOnChildren(readingTransactions, writingTransaction, operation);
        } finally {
            closeContextsAndRemoveTransaction();
        }
    }

    @Override
    public boolean markForTermination(Status reason) {
        // While state is open, take the lock by polling.
        // We do this to re-check state, which could be set by another thread committing or rolling back.
        while (true) {
            try {
                if (state != State.OPEN) {
                    return false;
                } else {
                    if (exclusiveLock.tryLock(100, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw terminationFailedError();
            }
        }

        try {
            if (state != State.OPEN) {
                return false;
            }

            terminationMark = new TerminationMark(reason, clock.nanos());
            state = State.TERMINATED;

            terminateChildren(reason);
            autocommitQueries.forEach(q ->
                    // See terminateChildren for explanation
                    executor.execute(() -> q.terminate(reason)));
        } finally {
            exclusiveLock.unlock();
        }

        return true;
    }

    @Override
    public void childTransactionTerminated(Status reason) {
        if (!isOpen()) {
            return;
        }

        markForTermination(reason);
    }

    @Override
    public void registerAutocommitQuery(AutocommitQuery autocommitQuery) {
        autocommitQueries.add(autocommitQuery);
        // Handle a case when we are registering to an already terminated transaction
        if (state == State.TERMINATED) {
            autocommitQuery.terminate(terminationMark.getReason());
        }
    }

    @Override
    public void unRegisterAutocommitQuery(AutocommitQuery autocommitQuery) {
        autocommitQueries.remove(autocommitQuery);
    }

    private void terminateChildren(Status reason) {
        // Historically, transaction termination has been a quick and non-blocking
        // operation and, unfortunately, users count on that (Bolt server invokes it
        // on Netty event loop thread).
        // Because of that we can't invoke the termination of remote transactions
        // and await results. So let's invoke the termination of transactions
        // asynchronously in a fire and forget manner. We don't care about the results anyway.
        // It is important just to try to terminate the transactions using the best effort.
        // Technically, we could terminate the local transactions synchronously as it is
        // a cheap and non-blocking operation, but for simplicity, let's not distinguish
        // between local and remote cases. Also, the remote case is more common in Composite databases.
        readingTransactions.forEach(
                childTransaction -> executor.execute(() -> childTransactionTerminate(childTransaction.inner, reason)));
        if (writingTransaction != null) {
            executor.execute(() -> childTransactionTerminate(writingTransaction, reason));
        }
    }

    public boolean isOpen() {
        return state == State.OPEN;
    }

    public Optional<TerminationMark> getTerminationMark() {
        return Optional.ofNullable(terminationMark);
    }

    protected void checkTransactionOpenForStatementExecution() throws FabricException {
        if (state == State.TERMINATED) {
            throw TransactionTerminatedHelper.transactionTerminated(terminationMark.getReason());
        }

        if (state == State.CLOSED) {
            throw FabricException.executeQueryInClosedTransaction();
        }
    }

    private List<Throwable> doOnChildren(
            Iterable<ReadingChildTransaction<Child>> readingTransactions,
            Child writingTransaction,
            Consumer<Child> operation) {
        List<Future<?>> futures = new ArrayList<>();
        if (writingTransaction != null) {
            futures.add(executor.submit(() -> {
                operation.accept(writingTransaction);
                return null;
            }));
        }

        for (var readingChildTransaction : readingTransactions) {
            futures.add(executor.submit(() -> {
                operation.accept(readingChildTransaction.inner);
                return null;
            }));
        }

        List<Throwable> exceptions = new ArrayList<>();
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                exceptions.add(e.getCause());
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        return exceptions;
    }

    private void throwIfNonEmpty(List<ErrorRecord> failures, Status defaultStatusCode) {
        if (!failures.isEmpty()) {
            // The main exception is not logged, because it will be logged by Bolt
            // and the log would contain two lines reporting the same thing without any additional info.
            var mainException =
                    Exceptions.transform(failures.get(0).gqlStatusObject, defaultStatusCode, failures.get(0).error);
            for (int i = 1; i < failures.size(); i++) {
                var errorRecord = failures.get(i);
                mainException.addSuppressed(errorRecord.error);
                errorReporter.report(errorRecord.message, errorRecord.error, defaultStatusCode);
            }

            throw mainException;
        }
    }

    private FabricException multipleWriteError(Location attempt, Location current) {
        // There are two situations and the error should reflect them in order not to confuse the users:
        // 1. This is actually the same database, but the location has changed, because of leader switch in the cluster.
        if (current.getUuid().equals(attempt.getUuid())) {
            return FabricException.writeDuringLeaderSwitch(attempt, current);
        }

        // 2. The user is really trying to write to two different databases.
        return FabricException.writingToMultipleGraphs(
                attempt.databaseReference().toPrettyString(),
                current.databaseReference().toPrettyString());
    }

    private FabricException commitFailedError() {
        return FabricException.transactionCommitFailed(
                TransactionCommitFailed, "Failed to commit composite transaction");
    }

    private FabricException rollbackFailedError() {
        return FabricException.transactionRollbackFailed(
                TransactionRollbackFailed, "Failed to rollback composite transaction");
    }

    private FabricException terminationFailedError() {
        return FabricException.transactionTerminationFailed(
                TransactionTerminationFailed, "Failed to terminate composite transaction");
    }

    protected abstract boolean isUninitialized();

    protected abstract void closeContextsAndRemoveTransaction();

    protected abstract void childTransactionCommit(Child child);

    protected abstract void childTransactionRollback(Child child);

    protected abstract void childTransactionTerminate(Child child, Status reason);
}
