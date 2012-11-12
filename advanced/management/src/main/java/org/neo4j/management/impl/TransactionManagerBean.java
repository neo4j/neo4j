/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
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
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new TransactionManagerImpl( management );
    }

    private static class TransactionManagerImpl extends Neo4jMBean implements TransactionManager
    {
        private final TxModule txModule;
        private final NeoStore neoStore;

        TransactionManagerImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.txModule = management.getKernelData().getConfig().getTxModule();
            this.neoStore = ( (NeoStoreXaDataSource) txModule.getXaDataSourceManager().getXaDataSource(
                    Config.DEFAULT_DATA_SOURCE_NAME ) ).getNeoStore();
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

        public long getNumberOfRolledBackTransactions()
        {
            return txModule.getRolledbackTxCount();
        }

        public long getLastCommittedTxId()
        {
            return neoStore.getLastCommittedTx();
        }
    }
}
