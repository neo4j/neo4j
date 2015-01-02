/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.transaction;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.com.Response;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.util.StringLogger;

public class SlaveTxHook implements RemoteTxHook
{
    private final Master master;
    private final HaXaDataSourceManager xaDsm;
    private final StringLogger log;
    private final RequestContextFactory contextFactory;

    private final Set<Integer> seen = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    public SlaveTxHook( Master master, HaXaDataSourceManager xaDsm,
                        TxHookModeSwitcher.RequestContextFactoryResolver contextFactory, StringLogger log )
    {
        this.master = master;
        this.xaDsm = xaDsm;
        this.log = log;
        this.contextFactory = contextFactory.get();
    }

    @Override
    public void remotelyInitializeTransaction( int eventIdentifier, TransactionState state )
    {
        if(!state.isRemotelyInitialized())
        {
            // Mark first, to ensure we never create more than one transaction on the master (exception below could
            // cause retries).
            state.markAsRemotelyInitialized();

            Response<Void> response = master.initializeTx( contextFactory.newRequestContext( eventIdentifier ) );
            xaDsm.applyTransactions( response );
        }
    }

    @Override
    public void remotelyFinishTransaction( int eventIdentifier, boolean success )
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
