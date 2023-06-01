package org.neo4j.router.query;

import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.NormalizedDatabaseName;

/**
 * Allows resolving database references from database aliases or database names
 */
public interface DatabaseReferenceResolver {

    DatabaseReference resolve(String name);

    DatabaseReference resolve(NormalizedDatabaseName name);
}
