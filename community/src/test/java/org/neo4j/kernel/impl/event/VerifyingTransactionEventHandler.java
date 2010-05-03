package org.neo4j.kernel.impl.event;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

public class VerifyingTransactionEventHandler implements
        TransactionEventHandler<Object>
{
    private final ExpectedTransactionData expectedData;
    private boolean hasBeenCalled;

    public VerifyingTransactionEventHandler( ExpectedTransactionData expectedData )
    {
        this.expectedData = expectedData;
    }
    
    public void afterCommit( TransactionData data, Object state )
    {
    }

    public void afterRollback( TransactionData data, Object state )
    {
    }

    public Object beforeCommit( TransactionData data ) throws Exception
    {
        this.expectedData.compareTo( data );
        this.hasBeenCalled = true;
        return null;
    }
    
    boolean hasBeenCalled()
    {
        return this.hasBeenCalled;
    }
}
