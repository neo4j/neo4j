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
package org.neo4j.kernel.database;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MapCachingDatabaseReferenceRepository implements DatabaseReferenceRepository.Caching {
    private DatabaseReferenceRepository delegate;
    private volatile Map<NormalizedDatabaseName, DatabaseReference> databaseRefsByName;

    public MapCachingDatabaseReferenceRepository(DatabaseReferenceRepository delegate) {
        this.databaseRefsByName = new ConcurrentHashMap<>();
        this.delegate = delegate;
    }

    public MapCachingDatabaseReferenceRepository() {
        this(null);
    }

    public void setDelegate(DatabaseReferenceRepository delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<DatabaseReference> getByAlias(NormalizedDatabaseName databaseAlias) {
        return Optional.ofNullable(databaseRefsByName.computeIfAbsent(databaseAlias, this::lookupReferenceOnDelegate));
    }

    /**
     * May return null, as {@link ConcurrentHashMap#computeIfAbsent} uses null as a signal not to add an entry to for the given key.
     */
    private DatabaseReference lookupReferenceOnDelegate(NormalizedDatabaseName databaseName) {
        return delegate.getByAlias(databaseName).orElse(null);
    }

    @Override
    public Set<DatabaseReference> getAllDatabaseReferences() {
        // Can't cache getAll call
        return delegate.getAllDatabaseReferences();
    }

    @Override
    public Set<DatabaseReferenceImpl.Internal> getInternalDatabaseReferences() {
        // Can't cache getAll call
        return delegate.getInternalDatabaseReferences();
    }

    @Override
    public Set<DatabaseReferenceImpl.External> getExternalDatabaseReferences() {
        // Can't cache getAll call
        return delegate.getExternalDatabaseReferences();
    }

    @Override
    public Set<DatabaseReferenceImpl.Composite> getCompositeDatabaseReferences() {
        // Can't cache getAll call
        return delegate.getCompositeDatabaseReferences();
    }

    @Override
    public void invalidateAll() {
        this.databaseRefsByName = new ConcurrentHashMap<>();
    }
}
