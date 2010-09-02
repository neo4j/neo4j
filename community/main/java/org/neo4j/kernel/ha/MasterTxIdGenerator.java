package org.neo4j.kernel.ha;

import javax.transaction.TransactionManager;

import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class MasterTxIdGenerator implements TxIdGenerator
{
    public static class MasterTxIdGeneratorFactory implements TxIdGeneratorFactory
    {
        private final Broker broker;

        public MasterTxIdGeneratorFactory( Broker broker )
        {
            this.broker = broker;
        }
        
        public TxIdGenerator create( TransactionManager txManager )
        {
            return new MasterTxIdGenerator( broker );
        }
    }
    
    private final Broker broker;

    public MasterTxIdGenerator( Broker broker )
    {
        this.broker = broker;
    }
    
    public long generate( XaDataSource dataSource, int identifier )
    {
        return TxIdGenerator.DEFAULT.generate( dataSource, identifier );
    }

    public int getCurrentMasterId()
    {
        return broker.getMyMachineId();
    }
}
