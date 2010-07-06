package org.neo4j.kernel.impl.ha;

public abstract class ResponseReceiver
{
    public final <T> T receive( Response<T> response )
    {
        applyTransactions( response.transactions() );
//        try
//        {
            return response.response();
//        }
//        catch ( MasterFailureException e )
//        {
//            // TODO: create better handling of this
//            throw new RuntimeException( "TODO: Master failure", e );
//        }
    }

    protected void applyTransactions( TransactionStream transactions )
    {
        throw new UnsupportedOperationException();
    }
}
