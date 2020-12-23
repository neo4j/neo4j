/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.configuration.helpers;

import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingConstraint;
import org.neo4j.graphdb.config.Configuration;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.configuration.GraphDatabaseSettings.writable_databases;

/**
 * Injectable component for checking whether a database is configured to be read_only (by name).
 */
@FunctionalInterface
public interface ReadOnlyDatabaseChecker extends Predicate<String>
{
    /**
     * There are two configurations for declaring a database read_only.
     * The global config `dbms.read_only` and the per database config `dbms.databases.read_only`.
     * The former is fixed and the latter is dynamic.
     *
     * This difference can be safely ignored in most cases, but the current implementation of `CommunityCommitProcessFactory`
     * executes `ReadOnlyDatabaseChecker#test( databaseName )` and provides a `ReadOnlyTransactionCommitProcess` if true.
     *
     * CommitProcesses cannot be replaced at runtime, so in such cases we need to be able to distinguish between a database
     * being read_only due to dynamic or fixed config.
     *
     * Most implementations / uses of `ReadOnlyDatabaseChecker` can safely ignore this method.
     */
    default boolean readOnlyFixed()
    {
        return false;
    }

    static ReadOnlyDatabaseChecker neverReadOnly()
    {
        return databaseName -> false;
    }

    private static boolean check( Config config, String databaseName )
    {
        //System database can't be read only
        if ( Objects.equals( SYSTEM_DATABASE_NAME, databaseName ) )
        {
            return false;
        }

        return config.get( read_only ) ||
               config.get( read_only_databases ).contains( databaseName ) ||
               defaultsToReadOnlyAndNotWritable( config, databaseName );
    }

    private static boolean defaultsToReadOnlyAndNotWritable( Config config, String databaseName )
    {
        return config.get( read_only_database_default ) && !config.get( writable_databases ).contains( databaseName );
    }

    class Default implements ReadOnlyDatabaseChecker
    {
        private final Config config;

        public Default( Config config )
        {
            this.config = config;
        }

        @Override
        public boolean readOnlyFixed()
        {
            return config.get( read_only );
        }

        @Override
        public boolean test( String databaseName )
        {
            return check( config, databaseName );
        }
    }
}
