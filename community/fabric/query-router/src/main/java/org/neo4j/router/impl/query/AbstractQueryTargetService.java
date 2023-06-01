package org.neo4j.router.impl.query;

import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.router.query.QueryTargetService;

public abstract class AbstractQueryTargetService implements QueryTargetService {
    protected final DatabaseReference sessionDatabase;

    protected AbstractQueryTargetService(DatabaseReference sessionDatabase) {
        this.sessionDatabase = sessionDatabase;
    }
}
