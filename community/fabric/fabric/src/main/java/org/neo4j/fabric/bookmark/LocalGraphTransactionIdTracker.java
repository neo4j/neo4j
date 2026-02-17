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
package org.neo4j.fabric.bookmark;

import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.time.Duration;
import java.util.Optional;
import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

public class LocalGraphTransactionIdTracker {
    private final TransactionIdTracker transactionIdTracker;
    private final DatabaseIdRepository databaseIdRepository;
    private volatile Duration bookmarkTimeout;

    public LocalGraphTransactionIdTracker(
            TransactionIdTracker transactionIdTracker, DatabaseIdRepository databaseIdRepository, Config config) {
        this.transactionIdTracker = transactionIdTracker;
        this.databaseIdRepository = databaseIdRepository;

        bookmarkTimeout = config.get(GraphDatabaseSettings.bookmark_ready_timeout);
        config.addListener(GraphDatabaseSettings.bookmark_ready_timeout, (before, after) -> bookmarkTimeout = after);
    }

    public void awaitSystemGraphUpToDate(long transactionId) {
        awaitGraphUpToDate(NAMED_SYSTEM_DATABASE_ID, transactionId);
    }

    public void awaitGraphUpToDate(Location.Local location, long transactionId) {
        getNamedDatabaseId(location).ifPresent(databaseId -> awaitGraphUpToDate(databaseId, transactionId));
    }

    private void awaitGraphUpToDate(NamedDatabaseId namedDatabaseId, long transactionId) {
        transactionIdTracker.awaitUpToDate(namedDatabaseId, transactionId, bookmarkTimeout);
    }

    public Optional<Long> getTransactionId(Location.Local location) {
        return getNamedDatabaseId(location).map(transactionIdTracker::newestTransactionId);
    }

    private Optional<NamedDatabaseId> getNamedDatabaseId(Location.Local location) {
        DatabaseId databaseId = DatabaseIdFactory.from(location.getUuid());
        return databaseIdRepository.getById(databaseId);
    }
}
