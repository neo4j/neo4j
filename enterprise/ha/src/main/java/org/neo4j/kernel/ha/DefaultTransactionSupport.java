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

import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

class DefaultTransactionSupport extends LifecycleAdapter implements TransactionSupport
{
    private final AbstractTransactionManager txManager;
    private final TxHook txHook;
    private final InstanceAccessGuard switchBlock;
    private final Config config;
    private long stateSwitchTimeout;

    DefaultTransactionSupport( AbstractTransactionManager txManager, TxHook txHook,
                               InstanceAccessGuard switchBlock, Config config )
    {
        this.txManager = txManager;
        this.txHook = txHook;
        this.switchBlock = switchBlock;
        this.config = config;
    }
    
    @Override
    public void start() throws Throwable
    {
        stateSwitchTimeout = config.get( HaSettings.state_switch_timeout );
    }

    @Override
    public boolean hasAnyLocks( Transaction tx )
    {
        return txManager.getTransactionState().hasLocks();
    }

    @Override
    public void makeSureTxHasBeenInitialized()
    {
        try
        {
            Transaction tx = txManager.getTransaction();
            int eventIdentifier = txManager.getEventIdentifier();
            if ( !hasAnyLocks( tx ) )
            {
                if ( !switchBlock.await( stateSwitchTimeout ) )
                {
                    // TODO Specific exception instead?
                    throw new RuntimeException( "Timed out waiting for database to switch state" );
                }

                txHook.initializeTransaction( eventIdentifier );
            }
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
    }
}
