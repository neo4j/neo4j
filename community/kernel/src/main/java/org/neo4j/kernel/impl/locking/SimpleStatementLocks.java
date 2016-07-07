package org.neo4j.kernel.impl.locking;

/**
 * A {@link StatementLocks} implementation that uses given {@link Locks.Client} for both {@link #implicit() implicit}
 * and {@link #explicit() explicit} locks.
 */
public class SimpleStatementLocks implements StatementLocks
{
    private final Locks.Client client;

    public SimpleStatementLocks( Locks.Client client )
    {
        this.client = client;
    }

    @Override
    public Locks.Client explicit()
    {
        return client;
    }

    @Override
    public Locks.Client implicit()
    {
        return client;
    }

    @Override
    public void prepareForCommit()
    {
        // Locks where grabbed eagerly by client so no need to prepare
    }

    @Override
    public void stop()
    {
        client.stop();
    }

    @Override
    public void close()
    {
        client.close();
    }
}
