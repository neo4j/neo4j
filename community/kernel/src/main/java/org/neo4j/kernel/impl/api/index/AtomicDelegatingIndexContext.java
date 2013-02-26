package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.atomic.AtomicReference;

public class AtomicDelegatingIndexContext extends AbstractDelegatingIndexContext
{
    private final AtomicReference<IndexContext> delegateReference;

    public AtomicDelegatingIndexContext( IndexContext delegate )
    {
        delegateReference = new AtomicReference<IndexContext>( delegate );
    }
    
    @Override
    protected IndexContext getDelegate()
    {
        return delegateReference.get();
    }
    
    public void setDelegate( IndexContext delegate )
    {
        delegateReference.set( delegate );
    }
}
