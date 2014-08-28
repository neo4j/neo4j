/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster.member;

import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class ClusterMemberVersionCheck
{
    private final ClusterMembers clusterMembers;
    private final InstanceId myId;
    private final Clock clock;

    public ClusterMemberVersionCheck( ClusterMembers clusterMembers, InstanceId myId, Clock clock )
    {
        this.clusterMembers = clusterMembers;
        this.myId = myId;
        this.clock = clock;
    }

    public boolean doVersionCheck( StoreId storeId, long timeout, TimeUnit timeUnit ) throws InterruptedException
    {
        return waitForAllAliveToBeAvailable( timeout, timeUnit ) && noMismatches( storeId );
    }

    private boolean waitForAllAliveToBeAvailable( long timeout, TimeUnit timeUnit ) throws InterruptedException
    {
        long start = clock.currentTimeMillis();
        long timeoutMillis = timeUnit.toMillis( timeout );

        while ( timeoutMillis > 0 )
        {
            if ( allAliveAreAvailable() )
            {
                return true;
            }
            else
            {
                clusterMembers.waitForEvent( timeoutMillis );
                timeoutMillis = timeoutMillis - (clock.currentTimeMillis() - start);
            }
        }

        return allAliveAreAvailable();
    }

    private boolean allAliveAreAvailable()
    {
        for ( ClusterMember member : clusterMembers.getMembers() )
        {
            if ( !myId.equals( member.getInstanceId() ) && member.isAlive() &&
                    HighAvailabilityModeSwitcher.UNKNOWN.equals( member.getHARole() ) )
            {
                return false;
            }
        }
        return true;
    }

    private boolean noMismatches( StoreId expectedStoreId )
    {
        for ( ClusterMember member : clusterMembers.getMembers() )
        {
            if ( member.isAlive() &&
                    !member.getStoreId().equals( StoreId.DEFAULT ) && !member.getStoreId().equals( expectedStoreId ) )
            {
                return false;
            }
        }
        return true;
    }
}
