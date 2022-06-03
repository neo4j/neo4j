/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.dbms.database;

import static org.neo4j.dbms.database.ExtendedDatabaseInfo.COMMITTED_TX_ID_NOT_AVAILABLE;

import java.util.Optional;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.StoreFileClosedException;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;

public class DetailedDbInfoProvider {
    private final DatabaseContextProvider<?> databaseContextProvider;

    public DetailedDbInfoProvider(DatabaseContextProvider<?> databaseContextProvider) {
        this.databaseContextProvider = databaseContextProvider;
    }

    public long lastCommittedTxId(DatabaseId databaseId) {
        return databaseContextProvider
                .getDatabaseContext(databaseId)
                .filter(databaseContext -> databaseContext.database().isStarted())
                .map(DatabaseContext::dependencies)
                .map(dependencies -> dependencies.resolveDependency(TransactionIdStore.class))
                .flatMap(transactionIdStore -> {
                    try {
                        return Optional.of(transactionIdStore.getLastCommittedTransactionId());
                    } catch (StoreFileClosedException e) {
                        return Optional.empty();
                    }
                })
                .orElse(COMMITTED_TX_ID_NOT_AVAILABLE);
    }

    public StoreId storeId(DatabaseId databaseId) {
        return databaseContextProvider
                .getDatabaseContext(databaseId)
                .filter(databaseContext -> databaseContext.database().isStarted())
                .flatMap(databaseContext -> {
                    try {
                        return Optional.of(databaseContext.database().getStoreId());
                    } catch (Exception e) {
                        return Optional.empty();
                    }
                })
                .orElse(StoreId.UNKNOWN);
    }
}
