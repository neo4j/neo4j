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
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

public interface DatabaseContextProvider<DB extends DatabaseContext> {
    /**
     * Returns a given {@link DatabaseContext} object by identifier, or `Optional.empty()` if the database does not exist
     *
     * @param namedDatabaseId the ID of the database to be returned
     * @return optionally, the database context instance with ID databaseId
     */
    Optional<DB> getDatabaseContext(NamedDatabaseId namedDatabaseId);

    /**
     * Returns a given {@link DatabaseContext} object by name, or `Optional.empty()` if the database does not exist
     *
     * @param databaseName the name of the database to be returned
     * @return optionally, the database context instance with name databaseName
     */
    Optional<DB> getDatabaseContext(String databaseName);

    /**
     * Returns a given {@link DatabaseContext} object by name, or `Optional.empty()` if the database does not exist
     *
     * @param databaseId the identifier of the database to be returned
     * @return optionally, the database context instance with identifier databaseId
     */
    Optional<DB> getDatabaseContext(DatabaseId databaseId);

    /**
     * Returns the {@link DatabaseContext} for the system database, or throws if said context does not exist.
     *
     * @return the database context for the system database
     */
    default DB getSystemDatabaseContext() {
        return getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
                .orElseThrow(() -> new DatabaseShutdownException(
                        (Throwable) new DatabaseManagementException("Unable to retrieve the system database!")));
    }

    /**
     * Return all {@link DatabaseContext} instances created by this service, associated with their database names.
     *
     * The collection returned from this method must be an immutable view over the registered database and sorted by database name.
     *
     * @return a Map from database names to database objects.
     */
    NavigableMap<NamedDatabaseId, DB> registeredDatabases();

    /**
     * Use this to retrieve a {@link NamedDatabaseId} for a given database name.
     *
     * @return database ID repository for use with this database manager
     */
    DatabaseIdRepository databaseIdRepository();
}
