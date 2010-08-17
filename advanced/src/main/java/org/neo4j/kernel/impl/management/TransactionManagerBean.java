package org.neo4j.kernel.impl.management;

import javax.management.NotCompliantMBeanException;

import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.management.TransactionManager;

@Description( "Information about the Neo4j transaction manager" )
class TransactionManagerBean extends Neo4jMBean implements TransactionManager
{
    private final TxModule txModule;

    TransactionManagerBean( String instanceId, TxModule txModule )
            throws NotCompliantMBeanException
    {
        super( instanceId, TransactionManager.class );
        this.txModule = txModule;
    }

    @Description( "The number of currently open transactions" )
    public int getNumberOfOpenTransactions()
    {
        return txModule.getActiveTxCount();
    }

    @Description( "The highest number of transactions ever opened concurrently" )
    public int getPeakNumberOfConcurrentTransactions()
    {
        return txModule.getPeakConcurrentTxCount();
    }

    @Description( "The total number started transactions" )
    public int getNumberOfOpenedTransactions()
    {
        return txModule.getStartedTxCount();
    }

    @Description( "The total number of committed transactions" )
    public long getNumberOfCommittedTransactions()
    {
        return txModule.getCommittedTxCount();
    }

    @Description( "The total number of rolled back transactions" )
    public long getNumberOfRolledBackTransactions()
    {
        return txModule.getRolledbackTxCount();
    }
}
