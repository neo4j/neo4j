/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha.lock;

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.logging.LogProvider;

public class SlaveLockManager implements Locks
{
    private final RequestContextFactory requestContextFactory;
    private final Locks local;
    private final Master master;
    private final AvailabilityGuard availabilityGuard;
    private final LogProvider logProvider;

    public SlaveLockManager( Locks localLocks, RequestContextFactory requestContextFactory, Master master,
            AvailabilityGuard availabilityGuard, LogProvider logProvider, Config config )
    {
        this.requestContextFactory = requestContextFactory;
        this.availabilityGuard = availabilityGuard;
        this.local = localLocks;
        this.master = master;
        this.logProvider = logProvider;
    }

    @Override
    public Client newClient()
    {
        Client client = local.newClient();
        return new SlaveLocksClient( master, client, local, requestContextFactory, availabilityGuard, logProvider );
    }

    @Override
    public void accept( Visitor visitor )
    {
        local.accept( visitor );
    }

    @Override
    public void close()
    {
        local.close();
    }
}
