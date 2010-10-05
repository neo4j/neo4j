/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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
