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
package org.neo4j.dbms.database;

import java.util.Optional;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.StoreFileClosedException;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdProvider;
import org.neo4j.storageengine.api.TransactionIdStore;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class DefaultDatabaseDetailsExtrasProvider {
    public static final long COMMITTED_TX_ID_NOT_AVAILABLE = -1;

    private final DatabaseContextProvider<?> databaseContextProvider;

    public DefaultDatabaseDetailsExtrasProvider(DatabaseContextProvider<?> databaseContextProvider) {
        this.databaseContextProvider = databaseContextProvider;
    }

    public DatabaseDetailsExtras extraDetails(DatabaseId databaseId, TopologyInfoService.RequestedExtras detailsLevel) {
        if (detailsLevel.txInfo() || detailsLevel.storeInfo()) {
            var lastCommittedTxId = Optional.<Long>empty();
            var lastAppendIndex = Optional.<Long>empty();
            var storeId = Optional.<StoreId>empty();
            var externalStoreId = Optional.<ExternalStoreId>empty();
            var context = databaseContextProvider
                    .getDatabaseContext(databaseId)
                    .filter(databaseContext -> databaseContext.database().isStarted());
            if (detailsLevel.txInfo()) {
                lastCommittedTxId = fetchLastCommittedTxId(context);
                lastAppendIndex = fetchLastAppendIndex(context);
            }
            if (detailsLevel.storeInfo()) {
                storeId = fetchStoreId(context);
                externalStoreId = fetchExternalStoreId(context);
            }
            return new DatabaseDetailsExtras(lastCommittedTxId, lastAppendIndex, storeId, externalStoreId);
        }
        return DatabaseDetailsExtras.EMPTY;
    }

    private static Optional<Long> fetchLastAppendIndex(Optional<? extends DatabaseContext> context) {
        return context.map(DatabaseContext::dependencies)
                .filter(dependencies -> dependencies.containsDependency(AppendIndexProvider.class))
                .map(dependencies -> dependencies.resolveDependency(AppendIndexProvider.class))
                .flatMap(applyIndexProvider -> Optional.of(applyIndexProvider.getLastAppendIndex()));
    }

    private static Optional<Long> fetchLastCommittedTxId(Optional<? extends DatabaseContext> context) {
        return context.map(DatabaseContext::dependencies)
                .filter(dependencies -> dependencies.containsDependency(TransactionIdStore.class))
                .map(dependencies -> dependencies.resolveDependency(TransactionIdStore.class))
                .flatMap(transactionIdStore -> {
                    try {
                        return Optional.of(transactionIdStore.getLastCommittedTransactionId());
                    } catch (StoreFileClosedException e) {
                        return Optional.empty();
                    }
                });
    }

    private static Optional<StoreId> fetchStoreId(Optional<? extends DatabaseContext> context) {
        return context.flatMap(c -> {
            try {
                return Optional.of(c.database().getStoreId());
            } catch (Exception e) {
                return Optional.empty();
            }
        });
    }

    private static Optional<ExternalStoreId> fetchExternalStoreId(Optional<? extends DatabaseContext> context) {
        return context.map(DatabaseContext::dependencies)
                .filter(dependencies -> dependencies.containsDependency(StoreIdProvider.class))
                .map(dependencies -> dependencies.resolveDependency(StoreIdProvider.class))
                .flatMap(storeIdProvider -> {
                    try {
                        return Optional.of(storeIdProvider.getExternalStoreId());
                    } catch (StoreFileClosedException e) {
                        return Optional.empty();
                    }
                });
    }
}
