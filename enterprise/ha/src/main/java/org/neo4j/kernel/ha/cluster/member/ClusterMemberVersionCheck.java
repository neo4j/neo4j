/*
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
package org.neo4j.kernel.ha.cluster.member;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.store.StoreId;

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

    public Outcome doVersionCheck( StoreId storeId, long timeout, TimeUnit timeUnit ) throws InterruptedException
    {
        Outcome outcome = new Outcome();
        waitForAllAliveToBeAvailable( timeout, timeUnit, outcome );
        noMismatches( storeId, outcome );
        return outcome;
    }

    private void waitForAllAliveToBeAvailable( long timeout, TimeUnit timeUnit,
                                               Outcome outcome ) throws InterruptedException
    {
        long start = clock.currentTimeMillis();
        long timeoutMillis = timeUnit.toMillis( timeout );

        while ( timeoutMillis > 0 )
        {
            allAliveAreAvailable( outcome );
            if ( !outcome.hasUnavailable() )
            {
                return;
            }
            else
            {
                clusterMembers.waitForEvent( timeoutMillis );
                timeoutMillis = timeoutMillis - (clock.currentTimeMillis() - start);
            }
        }

        allAliveAreAvailable( outcome );
    }

    private void allAliveAreAvailable( Outcome outcome )
    {
        outcome.clearUnavailable();

        for ( ClusterMember member : clusterMembers.getMembers() )
        {
            if ( !myId.equals( member.getInstanceId() ) && member.isInitiallyKnown() && member.isAlive() &&
                 HighAvailabilityModeSwitcher.UNKNOWN.equals( member.getHARole() ) )
            {
                outcome.addUnavailable( member.getInstanceId() );
            }
        }
    }

    private void noMismatches( StoreId expectedStoreId, Outcome outcome )
    {
        for ( ClusterMember member : clusterMembers.getMembers() )
        {
            if ( member.isAlive() && member.isInitiallyKnown() && !equal( expectedStoreId, member.getStoreId() ) )
            {
                outcome.addMismatched( member.getInstanceId(), member.getStoreId() );
            }
        }
    }

    private static boolean equal( StoreId first, StoreId second )
    {
        return first.getStoreVersion() != second.getStoreVersion() || first.equalsByUpgradeId( second );
    }

    public static class Outcome
    {
        private final Set<Integer> unavailable = new HashSet<>();
        private final Map<Integer, StoreId> mismatched = new HashMap<>();

        public Set<Integer> getUnavailable()
        {
            return unavailable;
        }

        private void addUnavailable( InstanceId instanceId )
        {
            unavailable.add( instanceId.toIntegerIndex() );
        }

        private void clearUnavailable()
        {
            unavailable.clear();
        }

        public boolean hasUnavailable()
        {
            return !unavailable.isEmpty();
        }

        public Map<Integer, StoreId> getMismatched()
        {
            return mismatched;
        }

        private void addMismatched( InstanceId instanceId, StoreId storeId )
        {
            mismatched.put( instanceId.toIntegerIndex(), storeId );
        }

        public boolean hasMismatched()
        {
            return !mismatched.isEmpty();
        }

        @Override
        public String toString()
        {
            return "Outcome{" + "unavailable=" + unavailable + ", mismatched=" + mismatched + '}';
        }
    }
}
