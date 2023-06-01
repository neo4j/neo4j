package org.neo4j.router.location;

import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.database.DatabaseReference;

public interface LocationService {

    /**
     * Determine the routing location for the database reference
     */
    Location locationOf(DatabaseReference databaseReference);
}
