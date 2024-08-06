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
package org.neo4j.dbms.database.readonly;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

public class DefaultReadOnlyDatabases implements ReadOnlyDatabases {
    private final ReadOnlyChangeListener listener;
    private volatile Set<Lookup> readOnlyDatabases;
    private volatile long updateId;
    private final Set<LookupFactory> readOnlyDatabasesLookupFactories;

    public DefaultReadOnlyDatabases(LookupFactory... readOnlyDatabasesLookupFactories) {
        this(ReadOnlyChangeListener.NO_OP, readOnlyDatabasesLookupFactories);
    }

    public DefaultReadOnlyDatabases(
            ReadOnlyChangeListener listener, LookupFactory... readOnlyDatabasesLookupFactories) {
        this.readOnlyDatabasesLookupFactories = Set.of(readOnlyDatabasesLookupFactories);
        this.readOnlyDatabases = Set.of();
        this.updateId = -1;
        this.listener = listener;
    }

    @Override
    public boolean isReadOnly(DatabaseId databaseId) {
        Objects.requireNonNull(databaseId);

        // System database can't be read only
        if (databaseId.isSystemDatabase()) {
            return false;
        }

        return readOnlyDatabases.stream().anyMatch(l -> l.databaseIsReadOnly(databaseId));
    }

    @Override
    public boolean isReadOnlyLocally(DatabaseId databaseId) {
        Objects.requireNonNull(databaseId);

        // System database can't be read only
        if (databaseId.isSystemDatabase()) {
            return false;
        }

        return readOnlyDatabases.stream()
                .anyMatch(l -> l.databaseIsReadOnly(databaseId) && l.source() == Lookup.Source.CONFIG);
    }

    /**
     * @return a numeric value which increases monotonically with each call to {@link #refresh()}. Used by {@link DatabaseReadOnlyChecker} for caching.
     */
    @Override
    public long updateId() {
        return updateId;
    }

    @Override
    public DatabaseReadOnlyChecker forDatabase(NamedDatabaseId namedDatabaseId) {
        Objects.requireNonNull(namedDatabaseId);

        // System database can't be read only
        if (namedDatabaseId.isSystemDatabase()) {
            return DatabaseReadOnlyChecker.writable();
        }

        refresh();
        return new DatabaseReadOnlyChecker.Default(this, namedDatabaseId);
    }

    @Override
    public synchronized void refresh() {
        this.readOnlyDatabases = readOnlyDatabasesLookupFactories.stream()
                .map(LookupFactory::lookupReadOnlyDatabases)
                .collect(Collectors.toUnmodifiableSet());
        this.updateId++;
        listener.onRefresh(this);
    }
}
