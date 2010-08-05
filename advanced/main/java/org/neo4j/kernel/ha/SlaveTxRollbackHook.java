package org.neo4j.kernel.ha;

import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.ResponseReceiver;
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
        receiver.receive( broker.getMaster().rollbackTransaction(
                broker.getSlaveContext(), eventIdentifier ) );
    }
}
