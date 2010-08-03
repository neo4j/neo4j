package org.neo4j.kernel.ha;

import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.ResponseReceiver;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class SlaveTxIdFactory implements TxIdFactory
{
    private final Broker broker;
    private final ResponseReceiver receiver;

    public SlaveTxIdFactory( Broker broker, ResponseReceiver receiver )
    {
        this.broker = broker;
        this.receiver = receiver;
    }

    public long generate( XaDataSource dataSource )
    {
        return 0;
    }
}
