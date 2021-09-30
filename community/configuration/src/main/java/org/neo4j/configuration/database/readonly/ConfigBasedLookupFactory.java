/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.configuration.database.readonly;

import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.configuration.GraphDatabaseSettings.writable_databases;

/**
 * Default implementation of {@link ReadOnlyDatabases.LookupFactory} which resolves read only database names from  config.
 */
public final class ConfigBasedLookupFactory implements ReadOnlyDatabases.LookupFactory
{
    private final Config config;

    public ConfigBasedLookupFactory( Config config )
    {
        this.config = config;
    }

    @Override
    public ReadOnlyDatabases.Lookup lookupReadOnlyDatabases()
    {
        return new ConfigLookup( config.get( read_only_database_default ), config.get( read_only_databases ), config.get( writable_databases ) );
    }

    private static class ConfigLookup implements ReadOnlyDatabases.Lookup
    {
        private final boolean readOnlyDefault;
        private final Set<String> readOnlyDatabaseNames;
        private final Set<String> writableDatabaseNames;

        ConfigLookup( boolean readOnlyDefault, Set<String> readOnlyDatabaseNames, Set<String> writableDatabaseNames )
        {
            this.readOnlyDefault = readOnlyDefault;
            this.readOnlyDatabaseNames = readOnlyDatabaseNames;
            this.writableDatabaseNames = writableDatabaseNames;
        }

        @Override
        public boolean databaseIsReadOnly( NamedDatabaseId databaseId )
        {
            var name = databaseId.name();
            var explicitlyReadOnly = readOnlyDatabaseNames.contains( name );
            var implicitlyReadOnly = readOnlyDefault && !writableDatabaseNames.contains( name );
            return explicitlyReadOnly || implicitlyReadOnly;
        }
    }
}
