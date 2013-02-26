package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.ExecutorService;

import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public class PopulatingIndexContext extends DelegatingIndexContext implements FlipAwareIndexContext
{
    private Flipper flipper;
    private final ExecutorService executor;
    private final NeoStore neoStore;

    public PopulatingIndexContext( IndexContext delegate, ExecutorService executor, NeoStore neoStore )
    {
        super( delegate );
        this.executor = executor;
        this.neoStore = neoStore;
    }

    @Override
    public void setFlipper( Flipper flipper )
    {
        this.flipper = flipper;
    }
    
    @Override
    public void create()
    {
        super.create();
        IndexPopulationJob job = new IndexPopulationJob( getIndexRule(), getDelegate(), neoStore, flipper );
        executor.submit( job );
    }
}
