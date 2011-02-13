/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.neo4j.kernel.KernelData;
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
