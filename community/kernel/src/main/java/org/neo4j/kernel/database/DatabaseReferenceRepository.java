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
     * Given a string representation of a database name, return the corresponding {@link DatabaseReference} from the system database, if one exists.
     */
    default Optional<DatabaseReference> getByName( String databaseName )
    {
        return getByName( new NormalizedDatabaseName( databaseName ) );
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
