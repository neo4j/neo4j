package org.neo4j.kernel.impl.locking.deferred;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocks;

/**
 * A {@link StatementLocks} implementation that defers {@link #implicit() implicit}
 * locks using {@link DeferringLockClient}.
 */
public class DeferringStatementLocks implements StatementLocks
{
    private final Locks.Client explicit;
    private final DeferringLockClient implicit;

    public DeferringStatementLocks( Locks.Client explicit )
    {
        this.explicit = explicit;
        this.implicit = new DeferringLockClient( this.explicit );
    }

    @Override
    public Locks.Client explicit()
    {
        return explicit;
    }

    @Override
    public Locks.Client implicit()
    {
        return implicit;
    }

    @Override
    public void prepareForCommit()
    {
        implicit.grabDeferredLocks();
    }

    @Override
    public void stop()
    {
        implicit.stop();
    }

    @Override
    public void close()
    {
        implicit.close();
    }
}
