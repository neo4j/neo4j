/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.locking.forseti;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.RandomSupport;
import org.neo4j.time.Clocks;

import static org.neo4j.test.Race.throwing;

@ExtendWith( RandomExtension.class )

class ForsetiLockManagerTest
{
    @Inject
    RandomSupport random;

    @Test
    void testMultipleClientsSameTxId() throws Throwable
    {
        //This tests an issue where using the same transaction id for two concurrently used clients would livelock
        //Having non-unique transaction ids should not happen and be addressed on its own but the LockManager should still not hang
        Config config = Config.defaults( GraphDatabaseInternalSettings.lock_manager_verbose_deadlocks, true );
        ForsetiLockManager manager = new ForsetiLockManager( config, Clocks.nanoClock(), ResourceTypes.values() );
        final int TX_ID = 0;

        Race race = new Race();
        race.addContestants( 3, throwing( () -> {
            try ( Locks.Client client = manager.newClient() )
            {
                client.initialize( LeaseService.NoLeaseClient.INSTANCE, TX_ID, EmptyMemoryTracker.INSTANCE, config ); // Note same TX_ID

                if ( random.nextBoolean() )
                {
                    client.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 0 );
                }
                else
                {
                    client.acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 0 );
                }
            }
        } ) );

        race.go( 3, TimeUnit.MINUTES ); //Should not timeout (livelock)
    }
}
