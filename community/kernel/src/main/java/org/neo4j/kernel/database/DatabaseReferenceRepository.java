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
package org.neo4j.kernel.database;

import java.util.Optional;
import java.util.Set;

import org.neo4j.kernel.database.DatabaseReference.External;
import org.neo4j.kernel.database.DatabaseReference.Internal;

/**
 * Implementations of this interface allow for the retrieval of {@link DatabaseReference}s for databases which have not yet been dropped.
 */
public interface DatabaseReferenceRepository
{
    /**
     * Given a database name, return the corresponding {@link DatabaseReference} from the system database, if one exists.
     */
    Optional<DatabaseReference> getByName( NormalizedDatabaseName databaseName );

    /**
     * Given a database name, return the corresponding {@link DatabaseReference.Internal} from the system database, if one exists.
     *
     * Note that this reference must point to a database hosted on this DBMS.
     */
    default Optional<DatabaseReference.Internal> getInternalByName( NormalizedDatabaseName databaseName )
    {
        return getByName( databaseName )
                .filter( DatabaseReference.Internal.class::isInstance )
                .map( DatabaseReference.Internal.class::cast );
    }

    /**
     * Given a database name, return the corresponding {@link DatabaseReference.External} from the system database, if one exists.
     *
     * Note that this reference must not point to a database hosted on this DBMS.
     */
    default Optional<DatabaseReference.External> getExternalByName( NormalizedDatabaseName databaseName )
    {
        return getByName( databaseName )
                .filter( DatabaseReference.External.class::isInstance )
                .map( DatabaseReference.External.class::cast );
    }

    /**
     * Given a string representation of a database name, return the corresponding {@link DatabaseReference} from the system database, if one exists.
     */
    default Optional<DatabaseReference> getByName( String databaseName )
    {
        return getByName( new NormalizedDatabaseName( databaseName ) );
    }

    /**
     * Given a database name, return the corresponding {@link DatabaseReference.Internal} from the system database, if one exists.
     *
     * Note that this reference must point to a database hosted on this DBMS.
     */
    default Optional<DatabaseReference.Internal> getInternalByName( String databaseName )
    {
        return getInternalByName( new NormalizedDatabaseName( databaseName ) );
    }

    /**
     * Given a database name, return the corresponding {@link DatabaseReference.External} from the system database, if one exists.
     *
     * Note that this reference must not point to a database hosted on this DBMS.
     */
    default Optional<DatabaseReference.External> getExternalByName( String databaseName )
    {
        return getExternalByName( new NormalizedDatabaseName( databaseName ) );
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

    interface Caching extends DatabaseReferenceRepository
    {
        void invalidateAll();
    }
}
