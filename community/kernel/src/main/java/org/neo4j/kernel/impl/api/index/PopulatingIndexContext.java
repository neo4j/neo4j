package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.ExecutorService;

import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public class PopulatingIndexContext implements IndexContext
{
    private final ExecutorService executor;
    private final IndexPopulationJob job;

    public PopulatingIndexContext( ExecutorService executor, IndexRule rule, IndexWriter writer,
                                   FlippableIndexContext flipper, NeoStore neoStore )
    {
        this.executor = executor;
        this.job      = new IndexPopulationJob( rule, writer, flipper, neoStore );
    }

    @Override
    public void create()
    {
        executor.submit( job );
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        job.update( updates );
    }

    @Override
    public void drop()
    {
        job.cancel();
    }
}
