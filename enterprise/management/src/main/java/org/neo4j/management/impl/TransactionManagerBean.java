/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.management.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.TransactionStats;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.management.TransactionManager;

@Service.Implementation( ManagementBeanProvider.class )
public final class TransactionManagerBean extends ManagementBeanProvider
{
    public TransactionManagerBean()
    {
        super( TransactionManager.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new TransactionManagerImpl( management );
    }

    private static class TransactionManagerImpl extends Neo4jMBean implements TransactionManager
    {
        private final TransactionStats txMonitor;
        private final DataSourceManager xadsm;

        TransactionManagerImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.txMonitor = management.resolveDependency( TransactionStats.class );
            this.xadsm = management.resolveDependency( DataSourceManager.class );
        }

        @Override
        public long getNumberOfOpenTransactions()
        {
            return txMonitor.getNumberOfActiveTransactions();
        }

        @Override
        public long getPeakNumberOfConcurrentTransactions()
        {
            return txMonitor.getPeakConcurrentNumberOfTransactions();
        }

        @Override
        public long getNumberOfOpenedTransactions()
        {
            return txMonitor.getNumberOfStartedTransactions();
        }

        @Override
        public long getNumberOfCommittedTransactions()
        {
            return txMonitor.getNumberOfCommittedTransactions();
        }

        @Override
        public long getNumberOfRolledBackTransactions()
        {
            return txMonitor.getNumberOfRolledBackTransactions();
        }

        @Override
        public long getLastCommittedTxId()
        {
            NeoStoreDataSource neoStoreDataSource = xadsm.getDataSource();
            if ( neoStoreDataSource == null )
            {
                return -1;
            }
            return neoStoreDataSource.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                    .getLastCommittedTransactionId();
        }
    }
}
