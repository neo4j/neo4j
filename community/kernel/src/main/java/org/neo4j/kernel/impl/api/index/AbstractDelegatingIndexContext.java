package org.neo4j.kernel.impl.api.index;

import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public abstract class AbstractDelegatingIndexContext implements IndexContext
{
    protected abstract IndexContext getDelegate();

    @Override
    public void create()
    {
        getDelegate().create();
    }
    
    @Override
    public void ready()
    {
        getDelegate().ready();
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
    
    @Override
    public IndexRule getIndexRule()
    {
        return getDelegate().getIndexRule();
    }
}
