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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.ElectionContextImpl;
import org.neo4j.cluster.protocol.cluster.ClusterContext;

/**
 * If a server is promoted or demoted, then use this {@link WinnerStrategy} during election so that that decision is
 * adhered to, if possible.
 */
public class BiasedWinnerStrategy implements WinnerStrategy
{
    private ClusterContext electionContext;
    private final InstanceId biasedNode;
    private final boolean nodePromoted;

    private BiasedWinnerStrategy( ClusterContext electionContext, InstanceId biasedNode, boolean nodePromoted )
    {
        this.electionContext = electionContext;
        this.biasedNode = biasedNode;
        this.nodePromoted = nodePromoted;
    }

    public static BiasedWinnerStrategy promotion( ClusterContext clusterContext, InstanceId biasedNode )
    {
        return new BiasedWinnerStrategy( clusterContext, biasedNode, true );
    }

    public static BiasedWinnerStrategy demotion( ClusterContext clusterContext, InstanceId biasedNode )
    {
        return new BiasedWinnerStrategy( clusterContext, biasedNode, false );
    }

    @Override
    public org.neo4j.cluster.InstanceId pickWinner( Collection<Vote> votes )
    {
        List<Vote> eligibleVotes = ElectionContextImpl.removeBlankVotes( votes );

        moveMostSuitableCandidatesToTop( eligibleVotes );

        logElectionOutcome( votes, eligibleVotes );

        for ( Vote vote : eligibleVotes )
        {
            // Elect the biased instance biased as winner
            if ( winnerIsBiasedInstance( vote ) == nodePromoted )
            {
                return vote.getSuggestedNode();
            }
        }

        // None were chosen - try again, without considering promotions or demotions
        // This most commonly happens if current master is demoted but all other instances are slave-only
        for ( Vote vote : eligibleVotes )
        {
            return vote.getSuggestedNode();
        }

        return null;
    }

    private void moveMostSuitableCandidatesToTop( List<Vote> eligibleVotes )
    {
        Collections.sort( eligibleVotes );
        Collections.reverse( eligibleVotes );
    }

    private void logElectionOutcome( Collection<Vote> votes, List<Vote> eligibleVotes )
    {
        String electionOutcome =
                String.format( "Election: received votes %s, eligible votes %s (Instance #%s has been %s)",
                        votes, eligibleVotes, biasedNode, nodePromoted ? "promoted" : "demoted" );
        electionContext.getLog( getClass() ).debug( electionOutcome );
    }

    private boolean winnerIsBiasedInstance( Vote vote )
    {
        return vote.getSuggestedNode().equals( biasedNode );
    }
}
