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

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.StoreId;

public record DatabaseDetailsExtras(
        Optional<Long> lastCommittedTxId,
        Optional<Long> lastAppendIndex,
        Optional<StoreId> storeId,
        Optional<ExternalStoreId> externalStoreId) {
    public static final DatabaseDetailsExtras EMPTY =
            new DatabaseDetailsExtras(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    public static <T> Map<DatabaseId, Long> maxCommittedTxIds(
            Map<T, DatabaseDetailsExtras> extraDetails, Function<T, DatabaseId> databaseIdResolver) {
        return extraDetails.entrySet().stream()
                .filter(e -> e.getValue().lastCommittedTxId().isPresent())
                .collect(Collectors.toMap(
                        e -> databaseIdResolver.apply(e.getKey()),
                        e -> e.getValue().lastCommittedTxId().orElse(0L),
                        Math::max));
    }

    public Optional<Long> txCommitLag(long maxLastCommittedTxId) {
        if (maxLastCommittedTxId == DefaultDatabaseDetailsExtrasProvider.COMMITTED_TX_ID_NOT_AVAILABLE) {
            return lastCommittedTxId.map(c -> replace(c, () -> 0L));
        }
        return lastCommittedTxId.map(c -> replace(c, () -> c - maxLastCommittedTxId));
    }

    private Long replace(Long c, LongSupplier calculator) {
        return c == DefaultDatabaseDetailsExtrasProvider.COMMITTED_TX_ID_NOT_AVAILABLE
                ? DefaultDatabaseDetailsExtrasProvider.COMMITTED_TX_ID_NOT_AVAILABLE
                : calculator.getAsLong();
    }

    public boolean isEmpty() {
        return EMPTY.equals(this);
    }

    public void ifPresent(Consumer<DatabaseDetailsExtras> consumer) {
        if (!isEmpty()) {
            consumer.accept(this);
        }
    }
}
