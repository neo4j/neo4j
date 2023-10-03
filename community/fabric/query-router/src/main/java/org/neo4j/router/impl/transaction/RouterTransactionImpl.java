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

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.fabric.transaction.parent.AbstractCompoundTransaction;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.router.QueryRouterException;
import org.neo4j.router.impl.query.StatementType;
import org.neo4j.router.transaction.DatabaseTransaction;
import org.neo4j.router.transaction.DatabaseTransactionFactory;
import org.neo4j.router.transaction.RouterTransaction;
import org.neo4j.router.transaction.TransactionInfo;
import org.neo4j.time.SystemNanoClock;
import reactor.core.publisher.Mono;

public class RouterTransactionImpl extends AbstractCompoundTransaction<DatabaseTransaction>
        implements RouterTransaction {
    private final TransactionInfo transactionInfo;
    private final DatabaseTransactionFactory<Location.Local> localDatabaseTransactionFactory;
    private final DatabaseTransactionFactory<Location.Remote> remoteDatabaseTransactionFactory;
    private final TransactionBookmarkManager transactionBookmarkManager;
    private final ConcurrentHashMap<UUID, DatabaseTransaction> databaseTransactions;
    private StatementType statementType = null;

    public RouterTransactionImpl(
            TransactionInfo transactionInfo,
            DatabaseTransactionFactory<Location.Local> localDatabaseTransactionFactory,
            DatabaseTransactionFactory<Location.Remote> remoteDatabaseTransactionFactory,
            ErrorReporter errorReporter,
            SystemNanoClock clock,
            TransactionBookmarkManager transactionBookmarkManager) {
        super(errorReporter, clock);
        this.transactionInfo = transactionInfo;
        this.localDatabaseTransactionFactory = localDatabaseTransactionFactory;
        this.remoteDatabaseTransactionFactory = remoteDatabaseTransactionFactory;
        this.transactionBookmarkManager = transactionBookmarkManager;
        this.databaseTransactions = new ConcurrentHashMap<>();
    }

    @Override
    public DatabaseTransaction transactionFor(Location location) {
        var mode =
                switch (transactionInfo.accessMode()) {
                    case WRITE -> TransactionMode.MAYBE_WRITE;
                    case READ -> TransactionMode.DEFINITELY_READ;
                };
        return databaseTransactions.computeIfAbsent(
                location.databaseReference().id(),
                ref -> registerNewChildTransaction(location, mode, () -> createTransactionFor(location)));
    }

    private DatabaseTransaction createTransactionFor(Location location) {
        if (location instanceof Location.Local local) {
            return localDatabaseTransactionFactory.beginTransaction(local, transactionInfo, transactionBookmarkManager);
        } else if (location instanceof Location.Remote remote) {
            return remoteDatabaseTransactionFactory.beginTransaction(
                    remote, transactionInfo, transactionBookmarkManager);
        } else {
            throw new IllegalArgumentException("Unexpected Location type: " + location);
        }
    }

    @Override
    protected boolean isUninitialized() {
        return false;
    }

    @Override
    protected void closeContextsAndRemoveTransaction() {
        databaseTransactions.values().forEach(DatabaseTransaction::close);
    }

    @Override
    protected Mono<Void> childTransactionCommit(DatabaseTransaction databaseTransaction) {
        return Mono.fromRunnable(databaseTransaction::commit);
    }

    @Override
    protected Mono<Void> childTransactionRollback(DatabaseTransaction databaseTransaction) {
        return Mono.fromRunnable(databaseTransaction::rollback);
    }

    @Override
    protected Mono<Void> childTransactionTerminate(DatabaseTransaction databaseTransaction, Status reason) {
        return Mono.fromRunnable(() -> databaseTransaction.terminate(reason));
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
            if (oldType != type) {
                var queryAfterQuery = type.isQuery() && oldType.isQuery();
                var readQueryAfterSchema = type.isReadQuery() && oldType.isSchemaCommand();
                var schemaAfterReadQuery = type.isSchemaCommand() && oldType.isReadQuery();
                var allowedCombination = queryAfterQuery || readQueryAfterSchema || schemaAfterReadQuery;
                if (allowedCombination) {
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
}
