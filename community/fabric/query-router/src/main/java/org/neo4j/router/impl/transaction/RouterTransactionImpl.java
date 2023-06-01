/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.router.impl.transaction;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.fabric.transaction.parent.AbstractCompoundTransaction;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseReference;
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

    private final ConcurrentHashMap<DatabaseReference, DatabaseTransaction> databaseTransactions;

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
                location.databaseReference(),
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
}
