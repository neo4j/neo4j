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
import java.util.Set;
import org.neo4j.kernel.database.DatabaseReferenceImpl.Composite;
import org.neo4j.kernel.database.DatabaseReferenceImpl.External;
import org.neo4j.kernel.database.DatabaseReferenceImpl.Internal;
import org.neo4j.kernel.database.DatabaseReferenceImpl.SPD;

/**
 * Implementations of this interface allow for the retrieval of {@link DatabaseReference}s for databases which have not yet been dropped.
 */
public interface DatabaseReferenceRepository {
    /**
     * Given a database alias, return the corresponding {@link DatabaseReference} from the system database, if one exists.
     */
    Optional<DatabaseReference> getByAlias(NormalizedDatabaseName databaseAlias);

    /**
     * Given a database alias, return the corresponding {@link DatabaseReferenceImpl.Internal} from the system database, if one exists.
     *
     * Note that this reference must point to a database hosted on this DBMS.
     */
    default Optional<DatabaseReferenceImpl.Internal> getInternalByAlias(NormalizedDatabaseName databaseAlias) {
        return getByAlias(databaseAlias)
                .filter(DatabaseReferenceImpl.Internal.class::isInstance)
                .map(DatabaseReferenceImpl.Internal.class::cast);
    }

    /**
     * Given a database alias, return the corresponding {@link DatabaseReferenceImpl.External} from the system database, if one exists.
     *
     * Note that this reference must not point to a database hosted on this DBMS.
     */
    default Optional<DatabaseReferenceImpl.External> getExternalByAlias(NormalizedDatabaseName databaseAlias) {
        return getByAlias(databaseAlias)
                .filter(DatabaseReferenceImpl.External.class::isInstance)
                .map(DatabaseReferenceImpl.External.class::cast);
    }

    /**
     * Given a string representation of a database name, return the corresponding {@link DatabaseReference} from the system database, if one exists.
     */
    default Optional<DatabaseReference> getByAlias(String databaseName) {
        return getByAlias(new NormalizedDatabaseName(databaseName));
    }

    /**
     * Given a database name, return the corresponding {@link DatabaseReferenceImpl.Internal} from the system database, if one exists.
     *
     * Note that this reference must point to a database hosted on this DBMS.
     */
    default Optional<DatabaseReferenceImpl.Internal> getInternalByAlias(String databaseName) {
        return getInternalByAlias(new NormalizedDatabaseName(databaseName));
    }

    /**
     * Given a database name, return the corresponding {@link DatabaseReferenceImpl.External} from the system database, if one exists.
     *
     * Note that this reference must not point to a database hosted on this DBMS.
     */
    default Optional<DatabaseReferenceImpl.External> getExternalByAlias(String databaseName) {
        return getExternalByAlias(new NormalizedDatabaseName(databaseName));
    }

    /**
     *  Fetch all known {@link DatabaseReference}es.
     */
    Set<DatabaseReference> getAllDatabaseReferences();

    /**
     * Fetch all known {@link Internal} references
     */
    Set<Internal> getInternalDatabaseReferences();

    /**
     * Fetch all known {@link  External} references
     */
    Set<External> getExternalDatabaseReferences();

    /**
     * Fetch all known {@link  Composite} references
     */
    Set<Composite> getCompositeDatabaseReferences();

    /**
     * Get an SPD database reference if there is one.
     * Only one SPD database can be present in a DBMS.
     */
    Optional<SPD> getSpdDatabaseReference();

    interface Caching extends DatabaseReferenceRepository {
        void invalidateAll();
    }
}
