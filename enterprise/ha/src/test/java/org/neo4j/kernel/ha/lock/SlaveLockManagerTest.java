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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.function.Suppliers.singleton;
import static org.neo4j.logging.NullLog.getInstance;

public class SlaveLockManagerTest
{
    private RequestContextFactory requestContextFactory;
    private Master master;
    private AvailabilityGuard availabilityGuard;

    @BeforeEach
    public void setUp()
    {
        requestContextFactory = new RequestContextFactory( 1, singleton( mock( TransactionIdStore.class ) ) );
        master = mock( Master.class );
        availabilityGuard = new AvailabilityGuard( Clocks.systemClock(), getInstance() );
    }

    @Test
    public void shutsDownLocalLocks()
    {
        Locks localLocks = mock( Locks.class );
        SlaveLockManager slaveLockManager = newSlaveLockManager( localLocks );

        slaveLockManager.close();

        verify( localLocks ).close();
    }

    @Test
    public void doesNotCreateClientsAfterShutdown()
    {
        SlaveLockManager slaveLockManager =
                newSlaveLockManager( new CommunityLockManger( Config.defaults(), Clocks.systemClock() ) );

        assertNotNull( slaveLockManager.newClient() );

        slaveLockManager.close();

        try
        {
            slaveLockManager.newClient();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    private SlaveLockManager newSlaveLockManager( Locks localLocks )
    {
        return new SlaveLockManager( localLocks, requestContextFactory, master, availabilityGuard,
                NullLogProvider.getInstance(), Config.defaults() );
    }
}
