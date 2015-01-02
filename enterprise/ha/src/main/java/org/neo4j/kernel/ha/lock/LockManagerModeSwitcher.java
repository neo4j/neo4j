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
package org.neo4j.kernel.ha.lock;

import java.net.URI;

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.cluster.AbstractModeSwitcher;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockManagerImpl;
import org.neo4j.kernel.impl.transaction.RagManager;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;

public class LockManagerModeSwitcher extends AbstractModeSwitcher<LockManager>
{
    private final HaXaDataSourceManager xaDsm;
    private final DelegateInvocationHandler<Master> master;
    private final RequestContextFactory requestContextFactory;
    private final AbstractTransactionManager txManager;
    private final RemoteTxHook remoteTxHook;
    private final AvailabilityGuard availabilityGuard;
    private final Config config;

    public LockManagerModeSwitcher( HighAvailabilityMemberStateMachine stateMachine,
                                    DelegateInvocationHandler<LockManager> delegate,
                                    HaXaDataSourceManager xaDsm, DelegateInvocationHandler<Master> master,
                                    RequestContextFactory requestContextFactory, AbstractTransactionManager txManager,
                                    RemoteTxHook remoteTxHook, AvailabilityGuard availabilityGuard, Config config )
    {
        super( stateMachine, delegate );
        this.xaDsm = xaDsm;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.txManager = txManager;
        this.remoteTxHook = remoteTxHook;
        this.availabilityGuard = availabilityGuard;
        this.config = config;
    }

    @Override
    protected LockManager getMasterImpl()
    {
        return new LockManagerImpl( new RagManager() );
    }

    @Override
    protected LockManager getSlaveImpl( URI serverHaUri )
    {
        return new SlaveLockManager( new RagManager(), requestContextFactory, master.cement(), xaDsm, txManager,
                remoteTxHook, availabilityGuard, new SlaveLockManager.Configuration()
        {
            @Override
            public long getAvailabilityTimeout()
            {
                return config.get( HaSettings.lock_read_timeout );
            }
        });
    }
}
