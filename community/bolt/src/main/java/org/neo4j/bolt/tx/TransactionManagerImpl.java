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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.protocol.common.connector.tx.TransactionOwner;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.tx.error.DatabaseUnavailableTransactionCreationException;
import org.neo4j.bolt.tx.error.NoSuchDatabaseTransactionCreationException;
import org.neo4j.bolt.tx.error.TransactionCreationException;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.query.NotificationConfiguration;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;

public class TransactionManagerImpl implements TransactionManager {
    private final BoltGraphDatabaseManagementServiceSPI graphDatabaseManagementService;
    private final Clock clock;
    private final AtomicLong nextTransactionId = new AtomicLong(1);

    private final Map<String, Transaction> transactionMap = new ConcurrentHashMap<>();
    private final CleanupListener cleanupListener = new CleanupListener();

    public TransactionManagerImpl(BoltGraphDatabaseManagementServiceSPI graphDatabaseManagementService, Clock clock) {
        this.graphDatabaseManagementService = graphDatabaseManagementService;
        this.clock = clock;
    }

    @Override
    public int getTransactionCount() {
        return this.transactionMap.size();
    }

    @Override
    public Optional<Transaction> get(String id) {
        return Optional.ofNullable(this.transactionMap.get(id));
    }

    @Override
    public Transaction create(
            TransactionType type,
            TransactionOwner owner,
            String databaseName,
            AccessMode mode,
            List<String> bookmarks,
            Duration timeout,
            Map<String, Object> metadata,
            NotificationConfiguration notificationsConfig)
            throws TransactionException {
        // TODO: Currently we consider both null and empty string to be a magic value relating to
        //       the default database. This is a left-over from older protocol revisions and should
        //       be corrected in a future version.
        var db = databaseName;
        if (db == null || databaseName.isEmpty()) {
            db = owner.selectedDefaultDatabase();
        }

        var kernelType =
                switch (type) {
                    case EXPLICIT -> Type.EXPLICIT;
                    case IMPLICIT -> Type.IMPLICIT;
                };

        BoltGraphDatabaseServiceSPI databaseService;
        try {
            databaseService = this.graphDatabaseManagementService.database(db, owner.memoryTracker());
        } catch (DatabaseNotFoundException ex) {
            throw new NoSuchDatabaseTransactionCreationException(db, ex);
        } catch (UnavailableException ex) {
            throw new DatabaseUnavailableTransactionCreationException(databaseName, ex);
        }

        var executionConfig = notificationsConfig != null
                ? new QueryExecutionConfiguration(notificationsConfig)
                : QueryExecutionConfiguration.DEFAULT_CONFIG;

        var id = "bolt-" + this.nextTransactionId.getAndIncrement();

        BoltTransaction tx;
        try {
            tx = databaseService.beginTransaction(
                    kernelType,
                    owner.loginContext(),
                    owner.info(),
                    bookmarks,
                    timeout,
                    mode,
                    metadata,
                    owner.routingContext(),
                    executionConfig);
        } catch (Exception ex) {
            throw new TransactionCreationException(ex);
        }

        var handle = new TransactionImpl(id, type, databaseService.getDatabaseReference(), this.clock, tx);
        handle.registerListener(this.cleanupListener);

        this.transactionMap.put(id, handle);
        return handle;
    }

    private class CleanupListener implements Transaction.Listener {

        @Override
        public void onClose(Transaction transaction) {
            transactionMap.remove(transaction.id());
        }
    }
}
