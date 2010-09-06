package org.neo4j.kernel.ha;

import javax.transaction.Transaction;

import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.transaction.TxFinishHook;

public class SlaveTxRollbackHook implements TxFinishHook
{
    private final Broker broker;
    private final ResponseReceiver receiver;

    public SlaveTxRollbackHook( Broker broker, ResponseReceiver receiver )
    {
        this.broker = broker;
        this.receiver = receiver;
    }
    
    public boolean hasAnyLocks( Transaction tx )
    {
        return ((AbstractGraphDatabase) receiver).getConfig().getLockReleaser().hasLocks( tx );
    }

    public void finishTransaction( int eventIdentifier )
    {
        try
        {
            receiver.receive( broker.getMaster().first().finishTransaction(
                    receiver.getSlaveContext( eventIdentifier ) ) );
        }
        catch ( ZooKeeperException e )
        {
            receiver.newMaster( null, e );
            throw e;
        }
        catch ( HaCommunicationException e )
        {
            receiver.newMaster( null, e );
            throw e;
        }
    }
}
