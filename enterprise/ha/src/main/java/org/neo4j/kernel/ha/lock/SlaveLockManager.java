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
package org.neo4j.kernel.ha.lock;

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;

public class SlaveLockManager extends LifecycleAdapter implements Locks
{
    private final RequestContextFactory requestContextFactory;
    private final Locks local;
    private final Master master;
    private final AvailabilityGuard availabilityGuard;
    private final LogProvider logProvider;
    private final boolean txTerminationAwareLocks;

    public SlaveLockManager( Locks localLocks, RequestContextFactory requestContextFactory, Master master,
            AvailabilityGuard availabilityGuard, LogProvider logProvider, Config config )
    {
        this.requestContextFactory = requestContextFactory;
        this.availabilityGuard = availabilityGuard;
        this.local = localLocks;
        this.master = master;
        this.logProvider = logProvider;
        this.txTerminationAwareLocks = config.get( KernelTransactions.tx_termination_aware_locks );
    }

    @Override
    public Client newClient()
    {
        Client client = local.newClient();
        return new SlaveLocksClient( master, client, local, requestContextFactory, availabilityGuard, logProvider,
                txTerminationAwareLocks );
    }

    @Override
    public void accept( Visitor visitor )
    {
        local.accept( visitor );
    }

    @Override
    public void shutdown() throws Throwable
    {
        local.shutdown();
    }
}
