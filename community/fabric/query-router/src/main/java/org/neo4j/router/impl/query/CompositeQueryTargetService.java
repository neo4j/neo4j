package org.neo4j.router.impl.query;

import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.router.query.Query;

public class CompositeQueryTargetService extends AbstractQueryTargetService {

    public CompositeQueryTargetService(DatabaseReference sessionDatabase) {
        super(sessionDatabase);
    }

    @Override
    public DatabaseReference determineTarget(Query query) {
        return sessionDatabase;
    }
}
