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
package org.neo4j.configuration.database.readonly;

import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.configuration.GraphDatabaseSettings.writable_databases;

import java.util.Set;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DefaultReadOnlyDatabases;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Default implementation of {@link DefaultReadOnlyDatabases.LookupFactory} which resolves read only database names from  config.
 */
public final class ConfigBasedLookupFactory implements ReadOnlyDatabases.LookupFactory {
    private final Config config;
    private final DatabaseIdRepository databaseIdRepository;

    public ConfigBasedLookupFactory(Config config, DatabaseIdRepository databaseIdRepository) {
        this.config = config;
        this.databaseIdRepository = databaseIdRepository;
    }

    @Override
    public ReadOnlyDatabases.Lookup lookupReadOnlyDatabases() {
        return new ConfigLookup(
                databaseIdRepository,
                config.get(read_only_database_default),
                config.get(read_only_databases),
                config.get(writable_databases));
    }

    private static class ConfigLookup implements ReadOnlyDatabases.Lookup {
        private final DatabaseIdRepository databaseIdRepository;
        private final boolean readOnlyDefault;
        private final Set<String> readOnlyDatabaseNames;
        private final Set<String> writableDatabaseNames;

        ConfigLookup(
                DatabaseIdRepository databaseIdRepository,
                boolean readOnlyDefault,
                Set<String> readOnlyDatabaseNames,
                Set<String> writableDatabaseNames) {
            this.databaseIdRepository = databaseIdRepository;
            this.readOnlyDefault = readOnlyDefault;
            this.readOnlyDatabaseNames = readOnlyDatabaseNames;
            this.writableDatabaseNames = writableDatabaseNames;
        }

        @Override
        public boolean databaseIsReadOnly(DatabaseId databaseId) {
            return explicitlyReadOnly(databaseId) || implicitlyReadOnly(databaseId);
        }

        @Override
        public Source source() {
            return Source.CONFIG;
        }

        private boolean explicitlyReadOnly(DatabaseId databaseId) {
            return containsDatabaseId(readOnlyDatabaseNames, databaseId);
        }

        private boolean implicitlyReadOnly(DatabaseId databaseId) {
            return readOnlyDefault && !containsDatabaseId(writableDatabaseNames, databaseId);
        }

        private boolean containsDatabaseId(Set<String> names, DatabaseId databaseId) {
            return names.stream()
                    .flatMap(name ->
                            databaseIdRepository.getByName(name).stream().map(NamedDatabaseId::databaseId))
                    .anyMatch(databaseId::equals);
        }
    }
}
