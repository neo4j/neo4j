package org.neo4j.kernel;

import org.neo4j.kernel.impl.ha.MasterFailureException;

public abstract class MasterFailureReactor<T>
{
    private final HighlyAvailableGraphDatabase db;

    MasterFailureReactor( HighlyAvailableGraphDatabase db )
    {
        this.db = db;
    }
    
    public final T execute()
    {
        try
        {
            return doOperation();
        }
        catch ( MasterFailureException e )
        {
            rollbackStuff();
            db.reevaluateMyself();
            throw e;
        }
    }
    
    private void rollbackStuff()
    {
        // TODO
    }

    protected abstract T doOperation();
}
