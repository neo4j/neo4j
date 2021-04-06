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
package org.neo4j.configuration.helpers;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;
import org.neo4j.util.VisibleForTesting;

import static java.util.Objects.requireNonNull;

public interface DatabaseReadOnlyChecker
{
    static DatabaseReadOnlyChecker writable()
    {
        return WritableDatabaseReadOnlyChecker.INSTANCE;
    }

    static DatabaseReadOnlyChecker readOnly()
    {
        return ReadOnlyDatabaseReadOnlyChecker.INSTANCE;
    }

    /**
     * @return true if database is readonly, false otherwise.
     */
    boolean isReadOnly();

    /**
     * Check if database is a read only and throw exception if its not.
     */
    void check();

    class Default implements DatabaseReadOnlyChecker
    {
        private final DbmsReadOnlyChecker dbmsChecker;
        private final String databaseName;

        @VisibleForTesting
        public Default( Config config, String databaseName )
        {
            this( new DbmsReadOnlyChecker.Default( config ), databaseName );
        }

        public Default( DbmsReadOnlyChecker dbmsChecker, String databaseName )
        {
            this.dbmsChecker = requireNonNull( dbmsChecker );
            this.databaseName = requireNonNull( databaseName );
        }

        @Override
        public boolean isReadOnly()
        {
            return dbmsChecker.isReadOnly( databaseName );
        }

        @Override
        public void check()
        {
            if ( isReadOnly() )
            {
                throw new RuntimeException( new ReadOnlyDbException( databaseName ) );
            }
        }
    }

    class WritableDatabaseReadOnlyChecker implements DatabaseReadOnlyChecker
    {
        static final WritableDatabaseReadOnlyChecker INSTANCE = new WritableDatabaseReadOnlyChecker();

        private WritableDatabaseReadOnlyChecker()
        {
        }

        @Override
        public boolean isReadOnly()
        {
            return false;
        }

        @Override
        public void check()
        {
        }
    }

    class ReadOnlyDatabaseReadOnlyChecker implements DatabaseReadOnlyChecker
    {
        static final ReadOnlyDatabaseReadOnlyChecker INSTANCE = new ReadOnlyDatabaseReadOnlyChecker();

        private ReadOnlyDatabaseReadOnlyChecker()
        {
        }

        @Override
        public boolean isReadOnly()
        {
            return true;
        }

        @Override
        public void check()
        {
            throw new RuntimeException( new ReadOnlyDbException() );
        }
    }
}
