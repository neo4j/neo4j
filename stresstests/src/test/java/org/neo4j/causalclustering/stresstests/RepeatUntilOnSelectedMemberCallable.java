/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
