package org.neo4j.kernel.impl.index;

import javax.transaction.TransactionManager;

import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;

public class ReadOnlyIndexConnectionBroker<T extends XaConnection> extends IndexConnectionBroker<T>
{
    public ReadOnlyIndexConnectionBroker( TransactionManager transactionManager )
    {
        super( transactionManager );
    }
    
    @Override
    public T acquireResourceConnection()
    {
        throw new ReadOnlyDbException();
    }
    
    @Override
    public T acquireReadOnlyResourceConnection()
    {
        return null;
    }
    
    @Override
    protected T newConnection()
    {
        throw new ReadOnlyDbException();
    }
}
