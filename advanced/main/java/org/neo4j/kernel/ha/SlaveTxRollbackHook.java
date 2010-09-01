package org.neo4j.kernel.ha;

import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.transaction.TxRollbackHook;

public class SlaveTxRollbackHook implements TxRollbackHook
{
    private final Broker broker;
    private final ResponseReceiver receiver;

    public SlaveTxRollbackHook( Broker broker, ResponseReceiver receiver )
    {
        this.broker = broker;
        this.receiver = receiver;
    }

    public void rollbackTransaction( int eventIdentifier )
    {
        try
        {
            receiver.receive( broker.getMaster().rollbackTransaction(
                    receiver.getSlaveContext( eventIdentifier ) ) );
        }
        catch ( ZooKeeperException e )
        {
            receiver.somethingIsWrong( e );
            throw e;
        }
        catch ( HaCommunicationException e )
        {
            receiver.somethingIsWrong( e );
            throw e;
        }
    }

    public void doneCommitting( int eventIdentifier )
    {
        try
        {
            receiver.receive( broker.getMaster().doneCommitting(
                    receiver.getSlaveContext( eventIdentifier ) ) );
        }
        catch ( ZooKeeperException e )
        {
            receiver.somethingIsWrong( e );
            throw e;
        }
        catch ( HaCommunicationException e )
        {
            receiver.somethingIsWrong( e );
            throw e;
        }
    }
}
