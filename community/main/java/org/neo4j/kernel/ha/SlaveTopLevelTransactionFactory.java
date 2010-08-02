package org.neo4j.kernel.ha;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.PlaceboTransaction;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.TopLevelTransactionFactory;
import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.ResponseReceiver;
import org.neo4j.kernel.impl.ha.TransactionStream;
import org.neo4j.kernel.impl.transaction.TxManager;

public class SlaveTopLevelTransactionFactory implements TopLevelTransactionFactory
{
    private final Broker broker;
    private final ResponseReceiver receiver;
    private Transaction placeboTx;

    public SlaveTopLevelTransactionFactory( Broker broker, ResponseReceiver receiver )
    {
        this.broker = broker;
        this.receiver = receiver;
    }

    public Transaction beginTx( TransactionManager txManager )
    {
        return new SlaveTransaction( txManager );
    }

    public Transaction getPlaceboTx( TransactionManager txManager )
    {
        if ( placeboTx == null )
        {
            placeboTx = new PlaceboTransaction( txManager );
        }
        return placeboTx;
    }

    private class SlaveTransaction extends TopLevelTransaction
    {
        SlaveTransaction( TransactionManager tm )
        {
            super( tm );
        }
        
        public void finish()
        {
            boolean successfulFinish = false;
            try
            {
                int localTxId = ((TxManager) getTransactionManager()).getEventIdentifier(); 
                if ( isMarkedAsSuccessful() )
                {
                    // TODO commit
//                    broker.getMaster().commitTransaction( broker.getSlaveContext(),
//                            localTxId, transactionAsStream() );
                }
                else
                {
                    broker.getMaster().rollbackTransaction( broker.getSlaveContext(),
                            localTxId );
                }
                successfulFinish = true;
            }
            finally
            {
                try
                {
                    if ( !successfulFinish )
                    {
                        failure();
                    }
                }
                finally
                {
                    super.finish();
                }
            }
        }

        private TransactionStream transactionAsStream()
        {
            throw new UnsupportedOperationException();
        }
    }
}
