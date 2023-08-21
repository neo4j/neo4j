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

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.util.Preconditions;

public class TestDatabaseIdRepository implements DatabaseIdRepository {
    private final String defaultDatabaseName;
    private final Set<String> filterSet;
    private final ConcurrentHashMap<NormalizedDatabaseName, NamedDatabaseId> cache = new ConcurrentHashMap<>(
            Map.of(new NormalizedDatabaseName(SYSTEM_DATABASE_NAME), NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID));

    public TestDatabaseIdRepository() {
        this(DEFAULT_DATABASE_NAME);
    }

    public TestDatabaseIdRepository(Config config) {
        this(config.get(GraphDatabaseSettings.initial_default_database));
    }

    public TestDatabaseIdRepository(String defaultDbName) {
        filterSet = new CopyOnWriteArraySet<>();
        this.defaultDatabaseName = defaultDbName;
    }

    public NamedDatabaseId defaultDatabase() {
        return getRaw(defaultDatabaseName);
    }

    public NamedDatabaseId getRaw(String databaseName) {
        var databaseIdOpt = getByName(databaseName);
        Preconditions.checkState(
                databaseIdOpt.isPresent(),
                getClass().getSimpleName() + " should always produce a " + NamedDatabaseId.class.getSimpleName()
                        + " for any database name");
        return databaseIdOpt.get();
    }

    /**
     * Add a database to appear "not found" by the id repository
     */
    public void filter(String databaseName) {
        filterSet.add(databaseName);
    }

    @Override
    public Optional<NamedDatabaseId> getByName(NormalizedDatabaseName databaseName) {
        cache.putIfAbsent(databaseName, new NamedDatabaseId(databaseName.name(), UUID.randomUUID()));
        var id = cache.get(databaseName);
        return filterSet.contains(id.name()) ? Optional.empty() : Optional.of(id);
    }

    @Override
    public Optional<NamedDatabaseId> getByName(String databaseName) {
        return getByName(new NormalizedDatabaseName(databaseName));
    }

    @Override
    public Optional<NamedDatabaseId> getById(DatabaseId databaseId) {
        var id = cache.values().stream()
                .filter(v -> v.databaseId().equals(databaseId))
                .findFirst();
        var uuidIsFiltered = id.map(i -> filterSet.contains(i.name())).orElse(false);
        return uuidIsFiltered ? Optional.empty() : id;
    }

    public Set<NamedDatabaseId> getAllDatabaseIds() {
        return Set.copyOf(cache.values());
    }
}
