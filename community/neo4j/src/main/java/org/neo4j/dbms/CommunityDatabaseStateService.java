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
package org.neo4j.dbms;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Database State Service for the community edition of the dbms
 */
public final class CommunityDatabaseStateService implements DatabaseStateService {
    private final DatabaseContextProvider<StandaloneDatabaseContext> databaseContextProvider;

    public CommunityDatabaseStateService(DatabaseContextProvider<StandaloneDatabaseContext> databaseContextProvider) {
        this.databaseContextProvider = databaseContextProvider;
    }

    @Override
    public Map<NamedDatabaseId, DatabaseState> stateOfAllDatabases() {
        return databaseContextProvider.registeredDatabases().entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> getState(entry.getValue())));
    }

    @Override
    public DatabaseState stateOfDatabase(NamedDatabaseId namedDatabaseId) {
        return databaseContextProvider
                .getDatabaseContext(namedDatabaseId)
                .map(CommunityDatabaseStateService::getState)
                .orElse(CommunityDatabaseState.unknown(namedDatabaseId));
    }

    @Override
    public Optional<Throwable> causeOfFailure(NamedDatabaseId namedDatabaseId) {
        return databaseContextProvider.getDatabaseContext(namedDatabaseId).map(StandaloneDatabaseContext::failureCause);
    }

    private static DatabaseState getState(StandaloneDatabaseContext ctx) {
        return new CommunityDatabaseState(
                ctx.database().getNamedDatabaseId(), ctx.database().isStarted(), ctx.isFailed(), ctx.failureCause());
    }
}
