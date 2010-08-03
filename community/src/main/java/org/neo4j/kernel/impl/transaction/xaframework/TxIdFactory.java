package org.neo4j.kernel.impl.transaction.xaframework;

public interface TxIdFactory
{
    public static final TxIdFactory DEFAULT = new TxIdFactory()
    {
        public long generate( XaDataSource dataSource, int identifier )
        {
            return dataSource.getLastCommittedTxId() + 1;
        }
    };
    
    long generate( XaDataSource dataSource, int identifier );
}
