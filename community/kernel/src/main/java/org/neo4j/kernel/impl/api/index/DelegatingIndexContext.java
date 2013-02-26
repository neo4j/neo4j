package org.neo4j.kernel.impl.api.index;

public class DelegatingIndexContext extends AbstractDelegatingIndexContext
{
    private final IndexContext delegate;

    public DelegatingIndexContext( IndexContext delegate )
    {
        this.delegate = delegate;
    }

    @Override
    protected IndexContext getDelegate()
    {
        return delegate;
    }
}
