/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.management.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.management.TransactionManager;

@Service.Implementation(ManagementBeanProvider.class)
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
        private final TransactionCounters txMonitor;
        private final DataSourceManager xadsm;

        TransactionManagerImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.txMonitor = management.resolveDependency( TransactionCounters.class );
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
            return txMonitor.getNumberOfRolledbackTransactions();
        }

        @Override
        public long getLastCommittedTxId()
        {
            NeoStoreDataSource neoStoreDataSource = xadsm.getDataSource();
            if ( neoStoreDataSource == null )
            {
                return -1;
            }
            return neoStoreDataSource.getNeoStores().getMetaDataStore().getLastCommittedTransactionId();
        }
    }
}
