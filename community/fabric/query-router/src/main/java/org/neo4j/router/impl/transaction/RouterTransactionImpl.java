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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionRollbackFailed;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTerminationFailed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.fabric.transaction.parent.CompoundTransaction;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.transaction.trace.TraceProvider;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory;
import org.neo4j.router.QueryRouterException;
import org.neo4j.router.impl.query.StatementType;
import org.neo4j.router.impl.transaction.database.LocalDatabaseTransaction;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.transaction.DatabaseTransaction;
import org.neo4j.router.transaction.DatabaseTransactionFactory;
import org.neo4j.router.transaction.RouterTransaction;
import org.neo4j.router.transaction.TransactionInfo;
import org.neo4j.time.SystemNanoClock;

public class RouterTransactionImpl implements CompoundTransaction<DatabaseTransaction>, RouterTransaction {
    private final TransactionInfo transactionInfo;
    private final DatabaseTransactionFactory<Location.Local> localDatabaseTransactionFactory;
    private final DatabaseTransactionFactory<Location.Remote> remoteDatabaseTransactionFactory;
    private final TransactionBookmarkManager transactionBookmarkManager;
    private final Map<UUID, DatabaseTransaction> databaseTransactions;
    private final TransactionInitializationTrace initializationTrace;
    private final RouterTransactionManager transactionManager;
    private final SystemNanoClock clock;
    private final ErrorReporter errorReporter;
    private ConstituentTransactionFactory constituentTransactionFactory;

    // Concurrency note:
    // Transaction termination is the only modification operation that can be invoked
    // concurrently with other operations. It iterates through all child transactions
    // and marks them as terminated, which is a thread-safe operation for both kernel
    // and driver transactions.
    // Transactions being non-thread-safe apart from termination has been a feature of Kernel
    // transactions since the dawn of time and all clients of the API are build to respect it.
    // Therefore since a Query Router transaction is just a thin layer between those clients
    // and kernel transactions (and driver transactions), it can rely on this concurrency model.
    // Apart from that status, terminationMark and statementType can be queried by another thread.

    // Why a concurrent collection? Because transaction termination operation can iterate over
    // it while it is being modified by another thread.
    private final Set<ReadingChildTransaction> readingTransactions = new CopyOnWriteArraySet<>();
    private volatile DatabaseTransaction writingTransaction;
    private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);
    private volatile TerminationMark terminationMark;
    private volatile StatementType statementType = null;

    private record ReadingChildTransaction(DatabaseTransaction inner, boolean readingOnly) {}

    private enum State {
        OPEN,
        CLOSED,
        TERMINATED
    }

    private record ErrorRecord(String message, Throwable error) {}

    public RouterTransactionImpl(
            TransactionInfo transactionInfo,
            DatabaseTransactionFactory<Location.Local> localDatabaseTransactionFactory,
            DatabaseTransactionFactory<Location.Remote> remoteDatabaseTransactionFactory,
            ErrorReporter errorReporter,
            SystemNanoClock clock,
            TransactionBookmarkManager transactionBookmarkManager,
            TraceProvider traceProvider,
            RouterTransactionManager transactionManager) {
        this.transactionInfo = transactionInfo;
        this.localDatabaseTransactionFactory = localDatabaseTransactionFactory;
        this.remoteDatabaseTransactionFactory = remoteDatabaseTransactionFactory;
        this.transactionBookmarkManager = transactionBookmarkManager;
        this.initializationTrace = traceProvider.getTraceInfo();
        this.transactionManager = transactionManager;
        this.clock = clock;
        this.errorReporter = errorReporter;
        this.databaseTransactions = new HashMap<>();
    }

    @Override
    public DatabaseTransaction transactionFor(
            Location location, TransactionMode mode, LocationService locationService) {
        var tx = databaseTransactions.computeIfAbsent(
                location.databaseReference().id(),
                ref -> registerNewChildTransaction(
                        location, mode, () -> createTransactionFor(location, locationService)));
        if (mode == TransactionMode.DEFINITELY_WRITE) {
            upgradeToWritingTransaction(tx);
        }
        return tx;
    }

    @Override
    public void setConstituentTransactionFactory(ConstituentTransactionFactory constituentTransactionFactory) {
        this.constituentTransactionFactory = constituentTransactionFactory;
    }

    private DatabaseTransaction createTransactionFor(Location location, LocationService locationService) {
        if (location instanceof Location.Local local) {
            return localDatabaseTransactionFactory.beginTransaction(
                    local,
                    transactionInfo,
                    transactionBookmarkManager,
                    this::childTransactionTerminated,
                    constituentTransactionFactory);
        } else if (location instanceof Location.Remote remote) {
            return remoteDatabaseTransactionFactory.beginTransaction(
                    remote,
                    transactionInfo,
                    transactionBookmarkManager,
                    this::childTransactionTerminated,
                    constituentTransactionFactory);
        } else {
            throw new IllegalArgumentException("Unexpected Location type: " + location);
        }
    }

    public Optional<TerminationMark> getTerminationMark() {
        return Optional.ofNullable(terminationMark);
    }

    @Override
    public Optional<Status> getReasonIfTerminated() {
        return getTerminationMark().map(TerminationMark::getReason);
    }

    @Override
    public void verifyStatementType(StatementType type) {
        if (statementType == null) {
            statementType = type;
        } else {
            var oldType = statementType;
            if (!oldType.equals(type)) {
                var sameStatementType = type.statementType().equals(oldType.statementType());
                var readQueryAfterSchema = type.isReadQuery() && oldType.isSchemaCommand();
                var schemaAfterReadQuery = type.isSchemaCommand() && oldType.isReadQuery();
                var allowedCombination = readQueryAfterSchema || schemaAfterReadQuery || sameStatementType;
                if (allowedCombination) {
                    var queryAfterQuery = type.isQuery() && oldType.isQuery();
                    var writeQueryAfterReadQuery = queryAfterQuery && !type.isReadQuery() && oldType.isReadQuery();
                    var upgrade = writeQueryAfterReadQuery || schemaAfterReadQuery;
                    if (upgrade) {
                        statementType = type;
                    }
                } else {
                    throw new QueryRouterException(
                            Status.Transaction.ForbiddenDueToTransactionType,
                            "Tried to execute %s after executing %s",
                            type,
                            oldType);
                }
            }
        }
    }

    boolean isSchemaTransaction() {
        var type = statementType;
        return type != null && type.isSchemaCommand();
    }

    TransactionInfo transactionInfo() {
        return transactionInfo;
    }

    TransactionInitializationTrace initializationTrace() {
        return initializationTrace;
    }

    @Override
    public void commit() {
        if (!state.compareAndSet(State.OPEN, State.CLOSED)) {
            if (state.get() == State.TERMINATED) {
                // Wait for all children to be rolled back. Ignore errors
                doRollbackAndIgnoreErrors();
                throw new TransactionTerminatedException(terminationMark.getReason());
            }

            if (state.get() == State.CLOSED) {
                throw new QueryRouterException(TransactionCommitFailed, "Trying to commit closed transaction");
            }
        }

        var allFailures = new ArrayList<ErrorRecord>();

        doOnChildren(
                readingTransactions,
                null,
                allFailures,
                DatabaseTransaction::commit,
                () -> "Failed to commit a child read transaction");

        if (!allFailures.isEmpty()) {
            doOnChildren(
                    Set.of(),
                    writingTransaction,
                    allFailures,
                    DatabaseTransaction::rollback,
                    () -> "Failed to rollback a child write transaction");
        } else {
            doOnChildren(
                    Set.of(),
                    writingTransaction,
                    allFailures,
                    DatabaseTransaction::commit,
                    () -> "Failed to commit a child write transaction");
        }
        closeContextsAndRemoveTransaction();

        throwIfNonEmpty(allFailures, TransactionCommitFailed);
    }

    @Override
    public void rollback() {
        if (!state.compareAndSet(State.OPEN, State.CLOSED)) {
            if (state.get() == State.TERMINATED) {
                // Wait for all children to be rolled back. Ignore errors
                doRollbackAndIgnoreErrors();
                return;
            }

            if (state.get() == State.CLOSED) {
                return;
            }
        }
        var allFailures = new ArrayList<ErrorRecord>();

        doOnChildren(
                readingTransactions,
                writingTransaction,
                allFailures,
                DatabaseTransaction::rollback,
                () -> "Failed to rollback a child transaction");
        closeContextsAndRemoveTransaction();

        throwIfNonEmpty(allFailures, TransactionRollbackFailed);
    }

    private void doRollbackAndIgnoreErrors() {
        try {
            doOnChildren(
                    readingTransactions,
                    writingTransaction,
                    new ArrayList<>(),
                    DatabaseTransaction::rollback,
                    () -> "");
        } finally {
            closeContextsAndRemoveTransaction();
        }
    }

    @Override
    public boolean markForTermination(Status reason) {
        if (!state.compareAndSet(State.OPEN, State.TERMINATED)) {
            return false;
        }

        terminationMark = new TerminationMark(reason, clock.nanos());

        var allFailures = new ArrayList<ErrorRecord>();

        doOnChildren(
                readingTransactions,
                writingTransaction,
                allFailures,
                tx -> tx.terminate(reason),
                () -> "Failed to terminate a child transaction");
        throwIfNonEmpty(allFailures, TransactionTerminationFailed);
        return true;
    }

    private void doOnChildren(
            Set<ReadingChildTransaction> readingTransactions,
            DatabaseTransaction writingTransaction,
            List<ErrorRecord> errors,
            Consumer<DatabaseTransaction> operation,
            Supplier<String> errorMessage) {
        for (var readingTransaction : readingTransactions) {
            try {
                operation.accept(readingTransaction.inner);
            } catch (RuntimeException e) {
                errors.add(new ErrorRecord(errorMessage.get(), e));
            }
        }

        try {
            if (writingTransaction != null) {
                operation.accept(writingTransaction);
            }
        } catch (RuntimeException e) {
            errors.add(new ErrorRecord(errorMessage.get(), e));
        }
    }

    @Override
    public <Tx extends DatabaseTransaction> Tx registerNewChildTransaction(
            Location location, TransactionMode mode, Supplier<Tx> transactionSupplier) {
        return switch (mode) {
            case DEFINITELY_WRITE -> startWritingTransaction(location, transactionSupplier);
            case MAYBE_WRITE -> startReadingTransaction(false, transactionSupplier);
            case DEFINITELY_READ -> startReadingTransaction(true, transactionSupplier);
        };
    }

    private <Tx extends DatabaseTransaction> Tx startWritingTransaction(
            Location location, Supplier<Tx> writeTransactionSupplier) {
        checkTransactionOpenForStatementExecution();

        if (writingTransaction != null) {
            throw multipleWriteError(location, writingTransaction.location());
        }

        var tx = writeTransactionSupplier.get();
        writingTransaction = tx;
        // The Query Router transaction might have been terminated as this tx was being created
        if (terminationMark != null) {
            tx.terminate(terminationMark.getReason());
        }
        return tx;
    }

    private <TX extends DatabaseTransaction> TX startReadingTransaction(
            boolean readOnly, Supplier<TX> readingTransactionSupplier) {
        checkTransactionOpenForStatementExecution();

        var tx = readingTransactionSupplier.get();
        readingTransactions.add(new ReadingChildTransaction(tx, readOnly));
        // The Query Router transaction might have been terminated as this tx was being created
        if (terminationMark != null) {
            tx.terminate(terminationMark.getReason());
        }
        return tx;
    }

    @Override
    public <Tx extends DatabaseTransaction> void upgradeToWritingTransaction(Tx childTransaction) {
        if (this.writingTransaction == childTransaction) {
            return;
        }

        if (this.writingTransaction != null) {
            throw multipleWriteError(childTransaction.location(), this.writingTransaction.location());
        }

        var readingTransaction = readingTransactions.stream()
                .filter(readingTx -> readingTx.inner == childTransaction)
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("The supplied transaction has not been registered"));

        if (readingTransaction.readingOnly) {
            throw new IllegalStateException("Upgrading reading-only transaction to a writing one is not allowed");
        }

        readingTransactions.remove(readingTransaction);
        this.writingTransaction = readingTransaction.inner;
        // The Query Router transaction might have been terminated as this tx was being created
        // If that happens between the transaction being removed from the list of reading transactions
        // and being assigned as the writing transaction, it will not be terminated.
        if (terminationMark != null) {
            writingTransaction.terminate(terminationMark.getReason());
        }
    }

    @Override
    public void registerAutocommitQuery(AutocommitQuery autocommitQuery) {
        throw new IllegalStateException("Autocommit queries are not supported by Query Router transaction");
    }

    @Override
    public void unRegisterAutocommitQuery(AutocommitQuery autocommitQuery) {
        throw new IllegalStateException("Autocommit queries are not supported by Query Router transaction");
    }

    @Override
    public void childTransactionTerminated(Status reason) {
        markForTermination(reason);
    }

    @Override
    public void closeTransaction(DatabaseTransaction databaseTransaction) {
        if (readingTransactions.removeIf(readingTx -> readingTx.inner == databaseTransaction)) {
            databaseTransaction.close();
        }
    }

    private void throwIfNonEmpty(List<ErrorRecord> failures, Status defaultStatusCode) {
        if (!failures.isEmpty()) {
            // The main exception is not logged, because it will be logged by Bolt
            // and the log would contain two lines reporting the same thing without any additional info.
            var mainException = transform(defaultStatusCode, failures.get(0).error);
            for (int i = 1; i < failures.size(); i++) {
                var errorRecord = failures.get(i);
                mainException.addSuppressed(errorRecord.error);
                errorReporter.report(errorRecord.message, errorRecord.error, defaultStatusCode);
            }

            throw mainException;
        }
    }

    private void closeContextsAndRemoveTransaction() {
        databaseTransactions.values().forEach(DatabaseTransaction::close);
        transactionManager.unregisterTransaction(this);
    }

    private QueryRouterException multipleWriteError(Location attempt, Location current) {
        // There are two situations and the error should reflect them in order not to confuse the users:
        // 1. This is actually the same database, but the location has changed, because of leader switch in the cluster.
        if (current.getUuid().equals(attempt.getUuid())) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N34)
                    .withClassification(ErrorClassification.TRANSIENT_ERROR)
                    .build();
            return new QueryRouterException(
                    gql,
                    Status.Transaction.LeaderSwitch,
                    "Could not write to a database due to a cluster leader switch that occurred during the transaction. "
                            + "Previous leader: %s, Current leader: %s.",
                    current,
                    attempt);
        }

        // 2. The user is really trying to write to two different databases.
        return new QueryRouterException(
                Status.Statement.AccessMode,
                "Writing to more than one database per transaction is not allowed. Attempted write to %s, currently writing to %s",
                attempt.databaseReference().toPrettyString(),
                current.databaseReference().toPrettyString());
    }

    private void checkTransactionOpenForStatementExecution() {
        throwIfTerminatedOrClosed(() -> "Trying to execute query in a closed transaction");
    }

    @Override
    public void throwIfTerminatedOrClosed(Supplier<String> closedExceptionMessage) {
        if (terminationMark != null) {
            throw new TransactionTerminatedException(terminationMark.getReason());
        }

        if (state.get() == State.CLOSED) {
            throw new QueryRouterException(Status.Statement.ExecutionFailed, closedExceptionMessage.get());
        }
    }

    private RuntimeException transform(Status defaultStatus, Throwable t) {
        String message = t.getMessage();

        // preserve the original exception if possible
        // or try to preserve  at least the original status
        if (t instanceof Status.HasStatus) {
            if (t instanceof RuntimeException) {
                return (RuntimeException) t;
            }

            return new QueryRouterException(((Status.HasStatus) t).status(), message, t);
        }

        return new QueryRouterException(defaultStatus, message, t);
    }

    Set<InternalTransaction> getInternalTransactions() {
        Set<InternalTransaction> internalTransactions = new HashSet<>();

        readingTransactions.stream()
                .map(ReadingChildTransaction::inner)
                .filter(tx -> tx instanceof LocalDatabaseTransaction)
                .map(LocalDatabaseTransaction.class::cast)
                .map(LocalDatabaseTransaction::internalTransaction)
                .forEach(internalTransactions::add);

        if (writingTransaction != null
                && writingTransaction instanceof LocalDatabaseTransaction localDatabaseTransaction) {
            internalTransactions.add(localDatabaseTransaction.internalTransaction());
        }

        return internalTransactions;
    }

    void stopRemoteDbsAfterTimeout(long timeoutMillis) {
        var nonLocalTransaction = readingTransactions.stream()
                .map(ReadingChildTransaction::inner)
                .filter(tx -> !(tx instanceof LocalDatabaseTransaction))
                .toList();

        if (nonLocalTransaction.isEmpty()) {
            return;
        }

        awaitTransactionsClosedWithinTimeout(nonLocalTransaction, timeoutMillis);
        nonLocalTransaction.forEach(tx -> tx.terminate(Status.Transaction.Terminated));
    }

    private void awaitTransactionsClosedWithinTimeout(
            Collection<DatabaseTransaction> nonLocalTransaction, long timeoutMillis) {
        long deadline = clock.millis() + timeoutMillis;
        while (hasOpenTransactions(nonLocalTransaction) && clock.millis() < deadline) {
            parkNanos(MILLISECONDS.toNanos(10));
        }
    }

    private static boolean hasOpenTransactions(Collection<DatabaseTransaction> nonLocalTransaction) {
        for (DatabaseTransaction dbTransaction : nonLocalTransaction) {
            if (dbTransaction.isOpen()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setMetaData(Map<String, Object> txMeta) {
        transactionInfo.setTxMetadata(txMeta);
        getInternalTransactions().forEach(tx -> tx.setMetaData(txMeta));
    }
}
