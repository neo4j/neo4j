package org.neo4j.router.query;

import org.neo4j.kernel.database.DatabaseReference;

/**
 * Determines the target database for the given query
 */
public interface QueryTargetService {

    DatabaseReference determineTarget(Query query);
}
