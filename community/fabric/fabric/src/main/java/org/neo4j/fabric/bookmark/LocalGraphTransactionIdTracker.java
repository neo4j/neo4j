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
        var namedDatabaseId = getNamedDatabaseId(location);
        awaitGraphUpToDate(namedDatabaseId, transactionId);
    }

    private void awaitGraphUpToDate(NamedDatabaseId namedDatabaseId, long transactionId) {
        transactionIdTracker.awaitUpToDate(namedDatabaseId, transactionId, bookmarkTimeout);
    }

    public long getTransactionId(Location.Local location) {
        var namedDatabaseId = getNamedDatabaseId(location);
        return transactionIdTracker.newestTransactionId(namedDatabaseId);
    }

    private NamedDatabaseId getNamedDatabaseId(Location.Local location) {
        DatabaseId databaseId = DatabaseIdFactory.from(location.getUuid());
        var namedDatabaseId = databaseIdRepository.getById(databaseId);
        if (namedDatabaseId.isEmpty()) {
            // this can only happen when the database has just been deleted or someone tempered with a bookmark
            throw new IllegalArgumentException("A local graph could not be mapped to a database");
        }

        return namedDatabaseId.get();
    }
}
