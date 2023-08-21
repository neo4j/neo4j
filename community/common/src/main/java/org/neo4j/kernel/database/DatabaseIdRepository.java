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

import java.util.Optional;

/**
 * Implementations of this interface allow for the retrieval of {@link NamedDatabaseId}s for databases which have
 * not yet been dropped.
 *
 * Note: for editions which support multiple instances of Neo4j in one DBMS (i.e. enterprise editions), returning
 * database Ids from methods of this class does *not* necessarily mean that said databases exist on the local instance.
 */
public interface DatabaseIdRepository {

    /**
     * Given a database name, return the corresponding {@link NamedDatabaseId} from the system database, if one exists.
     *
     * Note: the same id may be returned for multiple different input names. This is due to the fact that database may
     * have multiple aliases. **Despite this**, {@link NamedDatabaseId#name()} will always return the "true" name, also
     * known as the primary alias.
     */
    Optional<NamedDatabaseId> getByName(NormalizedDatabaseName databaseName);

    /**
     * Given a database Id, return the corresponding {@link NamedDatabaseId} from the system database, if one exists.
     *
     * This is useful as many network protocols only send {@link DatabaseId} objects "over-the-wire", and their human
     * readable names need to be resolved at either end.
     */
    Optional<NamedDatabaseId> getById(DatabaseId databaseId);

    /**
     * Given a string representation of a database name, validate it and return the corresponding
     * {@link NamedDatabaseId} from the system database, if one exists.
     */
    default Optional<NamedDatabaseId> getByName(String databaseName) {
        return getByName(new NormalizedDatabaseName(databaseName));
    }

    interface Caching extends DatabaseIdRepository {
        void invalidateAll();
    }
}
