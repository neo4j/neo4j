package org.neo4j.kernel.impl.transaction.xaframework;

public interface TxIdGenerator
{
    public static final TxIdGenerator DEFAULT = new TxIdGenerator()
    {
        public long generate( XaDataSource dataSource, int identifier )
        {
            return dataSource.getLastCommittedTxId() + 1;
        }
        
        public int getCurrentMasterId()
        {
            return XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER;
        }
    };
    
    long generate( XaDataSource dataSource, int identifier );
    
    int getCurrentMasterId();
}
