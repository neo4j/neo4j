/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import static org.neo4j.helpers.collection.Iterables.limit;
import static org.neo4j.helpers.collection.Iterables.toList;

import java.net.URI;
import java.util.List;

import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;

/**
 * Context shared by all Paxos state machines.
 */
public class MultiPaxosContext
{
    ClusterContext clusterContext;
    ProposerContext proposerContext;
    LearnerContext learnerContext;
    HeartbeatContext heartbeatContext;
    Timeouts timeouts;

    PaxosInstanceStore paxosInstances = new PaxosInstanceStore();

    public MultiPaxosContext( ClusterContext clusterContext,
                              ProposerContext proposerContext,
                              LearnerContext learnerContext,
                              HeartbeatContext heartbeatContext,
                              Timeouts timeouts
    )
    {
        this.clusterContext = clusterContext;
        this.proposerContext = proposerContext;
        this.learnerContext = learnerContext;
        this.heartbeatContext = heartbeatContext;
        this.timeouts = timeouts;
    }

    public int getServerId()
    {
/*
        int i = clusterContext.getMe().hashCode();
        i = i % 100;
        return i;
*/

        int i = clusterContext.getConfiguration().getMembers().indexOf( clusterContext.getMe() );
        if ( i == -1 )
        {
            i = 800 + clusterContext.getMe().hashCode() % 200;
        }

        return i;
    }

    public List<URI> getAcceptors()
    {
        // Only use 2f+1 acceptors
        return toList( limit( clusterContext.getConfiguration()
                .getAllowedFailures() * 2 + 1, clusterContext.getConfiguration().getMembers() ) );
    }

    public Iterable<URI> getLearners()
    {
        return clusterContext.getConfiguration().getMembers();
    }

    public PaxosInstanceStore getPaxosInstances()
    {
        return paxosInstances;
    }

    public int getMinimumQuorumSize( List<URI> acceptors )
    {
        // n >= 2f+1
        if ( acceptors.size() >= 2 * clusterContext.getConfiguration().getAllowedFailures() + 1 )
        {
            return acceptors.size() - clusterContext.getConfiguration().getAllowedFailures();
        }
        else
        {
            return acceptors.size();
        }
    }
}
