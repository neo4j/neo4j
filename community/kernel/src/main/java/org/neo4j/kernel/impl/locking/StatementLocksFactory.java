package org.neo4j.kernel.impl.locking;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.locking.deferred.DeferringStatementLocks;

public class StatementLocksFactory
{
    private final Locks locksManager;
    private final boolean deferringLocks;

    public StatementLocksFactory( Locks locksManager, Config config )
    {
        this.locksManager = locksManager;
        this.deferringLocks = config.get( GraphDatabaseFacadeFactory.Configuration.deferred_locking );
    }

    public StatementLocks newInstance()
    {
        return deferringLocks ? new DeferringStatementLocks( locksManager.newClient() )
                              : new SimpleStatementLocks( locksManager.newClient() );
    }
}
