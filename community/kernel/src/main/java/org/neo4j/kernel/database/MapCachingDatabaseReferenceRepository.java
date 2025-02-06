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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MapCachingDatabaseReferenceRepository implements DatabaseReferenceRepository.Caching {
    private DatabaseReferenceRepository delegate;
    private volatile Map<NormalizedDatabaseName, DatabaseReference> databaseRefsByName;
    private volatile Map<NormalizedCatalogEntry, DatabaseReference> databaseRefsByCatalogEntry;
    private volatile Map<UUID, DatabaseReference> databaseRefsByUUID;

    public MapCachingDatabaseReferenceRepository(DatabaseReferenceRepository delegate) {
        this.databaseRefsByName = new ConcurrentHashMap<>();
        this.databaseRefsByCatalogEntry = new ConcurrentHashMap<>();
        this.databaseRefsByUUID = new ConcurrentHashMap<>();
        this.delegate = delegate;
    }

    public MapCachingDatabaseReferenceRepository() {
        this(null);
    }

    public void setDelegate(DatabaseReferenceRepository delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<DatabaseReference> getByAlias(NormalizedCatalogEntry catalogEntry) {
        var databaseRef = Optional.ofNullable(
                databaseRefsByCatalogEntry.computeIfAbsent(catalogEntry, this::lookupReferenceOnDelegate));

        databaseRef.ifPresent(databaseReference -> {
            databaseRefsByName.putIfAbsent(databaseReference.fullName(), databaseReference);
            databaseRefsByUUID.putIfAbsent(databaseReference.id(), databaseReference);
        });
        return databaseRef;
    }

    @Override
    public Optional<DatabaseReference> getByAlias(NormalizedDatabaseName databaseAlias) {
        var databaseRef =
                Optional.ofNullable(databaseRefsByName.computeIfAbsent(databaseAlias, this::lookupReferenceOnDelegate));
        databaseRef.ifPresent(databaseReference -> {
            databaseRefsByCatalogEntry.putIfAbsent(databaseReference.catalogEntry(), databaseReference);
            databaseRefsByUUID.putIfAbsent(databaseReference.id(), databaseReference);
        });
        return databaseRef;
    }

    @Override
    public Optional<DatabaseReference> getByUuid(UUID uuid) {
        var databaseRef =
                Optional.ofNullable(databaseRefsByUUID.computeIfAbsent(uuid, this::lookupReferenceByUuidOnDelegate));

        databaseRef.ifPresent(databaseReference -> {
            databaseRefsByName.putIfAbsent(databaseReference.fullName(), databaseReference);
            databaseRefsByCatalogEntry.putIfAbsent(databaseReference.catalogEntry(), databaseReference);
        });
        return databaseRef;
    }

    /**
     * May return null, as {@link ConcurrentHashMap#computeIfAbsent} uses null as a signal not to add an entry to for the given key.
     */
    private DatabaseReference lookupReferenceOnDelegate(NormalizedDatabaseName databaseName) {
        return delegate.getByAlias(databaseName).orElse(null);
    }

    private DatabaseReference lookupReferenceOnDelegate(NormalizedCatalogEntry catalogEntry) {
        return delegate.getByAlias(catalogEntry).orElse(null);
    }

    /**
     * May return null, as {@link ConcurrentHashMap#computeIfAbsent} uses null as a signal not to add an entry to for the given key.
     */
    private DatabaseReference lookupReferenceByUuidOnDelegate(UUID databaseId) {
        return delegate.getByUuid(databaseId).orElse(null);
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
        this.databaseRefsByCatalogEntry = new ConcurrentHashMap<>();
        this.databaseRefsByUUID = new ConcurrentHashMap<>();
    }
}
