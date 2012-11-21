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
package org.neo4j.kernel.ha;

import javax.transaction.Transaction;

import org.neo4j.com.Response;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.TxHook;

public class SlaveTxHook implements TxHook
{
    private final Master master;
    private final HaXaDataSourceManager xaDsm;
    private final RequestContextFactory contextFactory;
    private final AbstractTransactionManager txManager;

    public SlaveTxHook( Master master, HaXaDataSourceManager xaDsm,
                        TxHookModeSwitcher.RequestContextFactoryResolver contextFactory, AbstractTransactionManager txManager )
    {
        this.master = master;
        this.xaDsm = xaDsm;
        this.txManager = txManager;
        this.contextFactory = contextFactory.get();
    }

    @Override
    public void initializeTransaction( int eventIdentifier )
    {
        Response<Void> response = master.initializeTx( contextFactory.newRequestContext( eventIdentifier ) );
        xaDsm.applyTransactions( response );
    }

    @Override
    public boolean hasAnyLocks( Transaction tx )
    {
        return txManager.getTransactionState().hasLocks();
    }

    @Override
    public void finishTransaction( int eventIdentifier, boolean success )
    {
        Response<Void> response = master.finishTransaction(
                contextFactory.newRequestContext( eventIdentifier ), success );
        xaDsm.applyTransactions( response );
    }

    @Override
    public boolean freeIdsDuringRollback()
    {
        return false;
    }
}
