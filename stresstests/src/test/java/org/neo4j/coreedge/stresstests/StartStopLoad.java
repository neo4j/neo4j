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
package org.neo4j.coreedge.stresstests;

import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.ClusterMember;

class StartStopLoad extends RepeatUntilOnSelectedMemberCallable
{
    StartStopLoad( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster )
    {
        super( keepGoing, onFailure, cluster, cluster.edgeMembers().isEmpty() );
    }

    @Override
    protected void doWorkOnMember( boolean isCore, int id )
    {
        ClusterMember member = isCore ? cluster.getCoreMemberById( id ) : cluster.getEdgeMemberById( id );
        member.shutdown();
        LockSupport.parkNanos( 5_000_000_000L );
        member.start();
    }
}
