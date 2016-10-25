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
