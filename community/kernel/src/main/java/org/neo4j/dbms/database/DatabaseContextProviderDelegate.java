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

import java.util.NavigableMap;
import java.util.Optional;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

public class DatabaseContextProviderDelegate<DB extends DatabaseContext> implements DatabaseContextProvider<DB> {
    private final DatabaseContextProvider<DB> delegate;

    private DatabaseContextProviderDelegate(DatabaseContextProvider<DB> delegate) {
        this.delegate = delegate;
    }

    public static <DB extends DatabaseContext> DatabaseContextProviderDelegate<DB> delegate(
            DatabaseContextProvider<DB> delegate) {
        return new DatabaseContextProviderDelegate<>(delegate);
    }

    @Override
    public Optional<DB> getDatabaseContext(NamedDatabaseId namedDatabaseId) {
        return delegate.getDatabaseContext(namedDatabaseId);
    }

    @Override
    public NavigableMap<NamedDatabaseId, DB> registeredDatabases() {
        return delegate.registeredDatabases();
    }

    @Override
    public DatabaseIdRepository databaseIdRepository() {
        return delegate.databaseIdRepository();
    }

    @Override
    public Optional<DB> getDatabaseContext(DatabaseId databaseId) {
        return delegate.getDatabaseContext(databaseId);
    }

    @Override
    public Optional<DB> getDatabaseContext(String databaseName) {
        return delegate.getDatabaseContext(databaseName);
    }
}
