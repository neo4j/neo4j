package org.neo4j.kernel.manage;

import org.neo4j.kernel.impl.transaction.TxModule;

class TransactionManager extends Neo4jJmx implements TransactionManagerMBean
{
    private final TxModule txModule;

    TransactionManager( int instanceId, TxModule txModule )
    {
        super( instanceId );
        this.txModule = txModule;
    }

    public int getNumberOfOpenTransactions()
    {
        return txModule.getActiveTxCount();
    }

    public int getPeakNumberOfConcurrentTransactions()
    {
        return txModule.getPeakConcurrentTxCount();
    }

    public int getNumberOfOpenedTransactions()
    {
        return txModule.getStartedTxCount();
    }

    public long getNumberOfCommittedTransactions()
    {
        return txModule.getCommittedTxCount();
    }

    public long getNumberOfRollbackedTransactions()
    {
        return txModule.getRolledbackTxCount();
    }
}
