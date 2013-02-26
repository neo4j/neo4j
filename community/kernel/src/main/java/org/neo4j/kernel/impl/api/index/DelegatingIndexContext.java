package org.neo4j.kernel.impl.api.index;

public class DelegatingIndexContext<D extends IndexContext> extends AbstractDelegatingIndexContext<D>
{
    private final D delegate;

    public DelegatingIndexContext( D delegate )
    {
        this.delegate = delegate;
    }

    @Override
    protected D getDelegate()
    {
        return delegate;
    }
}
