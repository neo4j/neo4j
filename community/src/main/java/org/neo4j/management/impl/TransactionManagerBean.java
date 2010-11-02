package org.neo4j.management.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.management.TransactionManager;

@Service.Implementation( ManagementBeanProvider.class )
public final class TransactionManagerBean extends ManagementBeanProvider
{
    public TransactionManagerBean()
    {
        super( TransactionManager.class );
    }

    @Override
    protected Neo4jMBean createMBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new TransactionManagerImpl( this, kernel );
    }

    @Description( "Information about the Neo4j transaction manager" )
    private static class TransactionManagerImpl extends Neo4jMBean implements TransactionManager
    {
        private final TxModule txModule;

        TransactionManagerImpl( ManagementBeanProvider provider, KernelData kernel )
                throws NotCompliantMBeanException
        {
            super( provider, kernel );
            this.txModule = kernel.getConfig().getTxModule();
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
}
