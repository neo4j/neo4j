package org.neo4j.kernel.impl.management;

import javax.management.NotCompliantMBeanException;

import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.management.TransactionManager;

class TransactionManagerBean extends Neo4jMBean implements TransactionManager
{
    private final TxModule txModule;

    TransactionManagerBean( int instanceId, TxModule txModule ) throws NotCompliantMBeanException
    {
        super( instanceId, TransactionManager.class );
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
