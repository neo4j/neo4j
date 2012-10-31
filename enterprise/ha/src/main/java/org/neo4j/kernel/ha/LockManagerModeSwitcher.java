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

import java.net.URI;

import org.neo4j.kernel.ha.cluster.AbstractModeSwitcher;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockManagerImpl;
import org.neo4j.kernel.impl.transaction.RagManager;
import org.neo4j.kernel.impl.transaction.TxHook;

public class LockManagerModeSwitcher extends AbstractModeSwitcher<LockManager>
{
    private final AbstractTransactionManager txManager;
    private final TxHook txHook;
    private final HaXaDataSourceManager xaDsm;
    private final Master master;
    private final RequestContextFactory requestContextFactory;
    private TransactionSupport transactionSupport;

    public LockManagerModeSwitcher( HighAvailabilityMemberStateMachine stateMachine,
                                    DelegateInvocationHandler<LockManager> delegate,
                                    AbstractTransactionManager txManager,
                                    TxHook txHook, HaXaDataSourceManager xaDsm, Master master,
                                    RequestContextFactory requestContextFactory, TransactionSupport transactionSupport )
    {
        super( stateMachine, delegate );
        this.txManager = txManager;
        this.txHook = txHook;
        this.xaDsm = xaDsm;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.transactionSupport = transactionSupport;
    }

    @Override
    protected LockManager getMasterImpl()
    {
        return new LockManagerImpl( new RagManager( txManager ) );
    }

    @Override
    protected LockManager getSlaveImpl( URI serverHaUri )
    {
        return new SlaveLockManager( transactionSupport, new RagManager( txManager ), requestContextFactory, master, xaDsm );
    }
}
