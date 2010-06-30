package org.neo4j.index.impl.lucene;

import javax.transaction.TransactionManager;

import org.neo4j.kernel.impl.core.ReadOnlyDbException;

public class ReadOnlyConnectionBroker extends ConnectionBroker
{
    ReadOnlyConnectionBroker( TransactionManager transactionManager,
            LuceneDataSource dataSource )
    {
        super( transactionManager, dataSource );
    }
    
    @Override
    LuceneXaConnection acquireResourceConnection()
    {
        throw new ReadOnlyDbException();
    }
    
    @Override
    LuceneXaConnection acquireReadOnlyResourceConnection()
    {
        return null;
    }
}
