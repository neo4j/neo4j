package org.neo4j.kernel.impl.api.index;

public abstract class AbstractDelegatingIndexContext<D extends IndexContext> implements IndexContext
{
    protected abstract D getDelegate();

    @Override
    public void create()
    {
        getDelegate().create();
    }
    
    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        getDelegate().update( updates );
    }

    @Override
    public void drop()
    {
        getDelegate().drop();
    }
}
