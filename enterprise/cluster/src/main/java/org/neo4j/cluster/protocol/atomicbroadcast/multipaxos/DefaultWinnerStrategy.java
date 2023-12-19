/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.ElectionContextImpl;
import org.neo4j.cluster.protocol.cluster.ClusterContext;


public class DefaultWinnerStrategy implements WinnerStrategy
{
    private ClusterContext electionContext;

    public DefaultWinnerStrategy( ClusterContext electionContext )
    {
        this.electionContext = electionContext;
    }

    @Override
    public org.neo4j.cluster.InstanceId pickWinner( Collection<Vote> votes )
    {
        List<Vote> eligibleVotes = ElectionContextImpl.removeBlankVotes( votes );

        moveMostSuitableCandidatesToTop( eligibleVotes );

        logElectionOutcome( votes, eligibleVotes );

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
        String electionOutcome = String.format( "Election: received votes %s, eligible votes %s", votes, eligibleVotes );
        electionContext.getLog( getClass() ).debug( electionOutcome );
    }
}
