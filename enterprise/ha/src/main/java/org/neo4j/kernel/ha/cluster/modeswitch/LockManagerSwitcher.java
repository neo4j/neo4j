/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster.modeswitch;

import org.neo4j.function.Factory;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.lock.SlaveLockManager;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ReadOnlyLocks;
import org.neo4j.logging.LogProvider;

public class LockManagerSwitcher extends AbstractComponentSwitcher<Locks>
{
    private final DelegateInvocationHandler<Master> master;
    private final RequestContextFactory requestContextFactory;
    private final AvailabilityGuard availabilityGuard;
    private final Factory<Locks> locksFactory;
    private final LogProvider logProvider;
    private final Config config;

    public LockManagerSwitcher( DelegateInvocationHandler<Locks> delegate, DelegateInvocationHandler<Master> master,
                                RequestContextFactory requestContextFactory, AvailabilityGuard availabilityGuard,
                                Factory<Locks> locksFactory, LogProvider logProvider, Config config )
    {
        super( delegate );
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.availabilityGuard = availabilityGuard;
        this.locksFactory = locksFactory;
        this.logProvider = logProvider;
        this.config = config;
    }

    @Override
    protected Locks getMasterImpl()
    {
        return locksFactory.newInstance();
    }

    @Override
    protected Locks getSlaveImpl()
    {
        return new SlaveLockManager( locksFactory.newInstance(), requestContextFactory, master.cement(),
                availabilityGuard, logProvider, config );
    }

    @Override
    protected Locks getPendingImpl()
    {
        return new ReadOnlyLocks();
    }

    @Override
    protected void shutdownOldDelegate( Locks oldLocks )
    {
        if ( oldLocks != null )
        {
            oldLocks.close();
        }
    }
}
