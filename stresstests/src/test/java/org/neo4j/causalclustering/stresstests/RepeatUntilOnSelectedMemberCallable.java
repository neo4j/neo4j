/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.stresstests;

import java.util.Collection;
import java.util.Random;
import java.util.function.BooleanSupplier;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.helper.RepeatUntilCallable;

abstract class RepeatUntilOnSelectedMemberCallable extends RepeatUntilCallable
{
    private final Random random = new Random();
    final Cluster cluster;
    private final int numberOfCores;
    private final int numberOfEdges;

    RepeatUntilOnSelectedMemberCallable( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster,
            int numberOfCores, int numberOfEdges )
    {
        super( keepGoing , onFailure );
        this.cluster = cluster;
        this.numberOfCores = numberOfCores;
        this.numberOfEdges = numberOfEdges;
    }

    @Override
    protected final void doWork()
    {
        boolean isCore = numberOfEdges == 0 || random.nextBoolean();
        Collection<? extends ClusterMember> members = isCore ? cluster.coreMembers() : cluster.readReplicas();
        assert !members.isEmpty();
        int id = random.nextInt( isCore ? numberOfCores : numberOfEdges );
        doWorkOnMember( isCore, id );
    }

    protected abstract void doWorkOnMember( boolean isCore, int id );
}
