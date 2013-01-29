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
package org.neo4j.cluster.protocol.election;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;

/**
 * Context used by {@link ElectionState}.
 */
public class ElectionContext
{
    private List<ElectionRole> roles = new ArrayList<ElectionRole>();
    private ClusterContext clusterContext;
    private HeartbeatContext heartbeatContext;

    private Map<String, Election> elections = new HashMap<String, Election>();
    private ElectionCredentialsProvider electionCredentialsProvider;

    public ElectionContext( Iterable<ElectionRole> roles, ClusterContext clusterContext,
                            HeartbeatContext heartbeatContext )
    {
        this.heartbeatContext = heartbeatContext;
        Iterables.addAll( this.roles, roles );
        this.clusterContext = clusterContext;
    }

    public void setElectionCredentialsProvider( ElectionCredentialsProvider electionCredentialsProvider )
    {
        this.electionCredentialsProvider = electionCredentialsProvider;
    }

    public void created()
    {
        for ( ElectionRole role : roles )
        {
            // Elect myself for all roles
            clusterContext.elected( role.getName(), clusterContext.getMe() );
        }
    }

    public List<ElectionRole> getPossibleRoles()
    {
        return roles;
    }

    /*
     * Removes all roles from the provided node. This is expected to be the first call when receiving a demote
     * message for a node, since it is the way to ensure that election will happen for each role that node had
     */
    public void nodeFailed( URI node )
    {
        Iterable<String> rolesToDemote = getRoles( node );
        for ( String role : rolesToDemote )
        {
            clusterContext.getConfiguration().getRoles().remove( role );
        }
    }

    public Iterable<String> getRoles( URI server )
    {
        return clusterContext.getConfiguration().getRolesOf( server );
    }

    public ClusterContext getClusterContext()
    {
        return clusterContext;
    }

    public HeartbeatContext getHeartbeatContext()
    {
        return heartbeatContext;
    }

    public void unelect( String roleName )
    {
        clusterContext.getConfiguration().removeElected( roleName );
    }

    public boolean isElectionProcessInProgress( String role )
    {
        return elections.containsKey( role );
    }

    public void startDemotionProcess( String role, final URI demoteNode )
    {
        elections.put( role, new Election( new WinnerStrategy()
        {
            @Override
            public URI pickWinner( List<Vote> voteList )
            {
                // Sort based on credentials
                // The most suited candidate should come out on top
                Collections.sort( voteList );
                Collections.reverse( voteList );

                for ( Vote vote : voteList )
                {
                    // Don't elect as winner the node we are trying to demote
                    // Also don't elect someone that explicitly doesn't want to win
                    if ( !vote.getSuggestedNode().equals( demoteNode ) && !(vote.getCredentials() instanceof NotElectableElectionCredentials))
                    {
                        return vote.getSuggestedNode();
                    }
                }

                // No possible winner
                return null;
            }
        } ) );
    }

    public void startElectionProcess( String role )
    {
        elections.put( role, new Election( new WinnerStrategy()
        {
            @Override
            public URI pickWinner( List<Vote> voteList )
            {
                // Sort based on credentials
                // The most suited candidate should come out on top
                Collections.sort( voteList );
                Collections.reverse( voteList );

                for ( Vote vote : voteList )
                {
                    // Don't elect someone that explicitly doesn't want to win
                    if ( !(vote.getCredentials() instanceof NotElectableElectionCredentials))
                    {
                        return vote.getSuggestedNode();
                    }
                }

                // No possible winner
                return null;
            }
        } ) );
    }

    public void startPromotionProcess( String role, final URI promoteNode )
    {
        elections.put( role, new Election( new WinnerStrategy()
        {
            @Override
            public URI pickWinner( List<Vote> voteList )
            {

                // Sort based on credentials
                // The most suited candidate should come out on top
                Collections.sort( voteList );
                Collections.reverse( voteList );

                for ( Vote vote : voteList )
                {
                    // Don't elect as winner the node we are trying to demote
                    // Also don't elect someone that explicitly doesn't want to win
                    if ( !vote.getSuggestedNode().equals( promoteNode ) && !(vote.getCredentials() instanceof NotElectableElectionCredentials))
                    {
                        return vote.getSuggestedNode();
                    }
                }

                // No possible winner
                return null;
            }
        } ) );
    }

    public void voted( String role, URI suggestedNode, Comparable<Object> suggestionCredentials )
    {
        if ( isElectionProcessInProgress( role ) )
        {
            List<Vote> voteList = elections.get( role ).getVotes();
            voteList.add( new Vote( suggestedNode, suggestionCredentials ) );
        }
    }

    public URI getElectionWinner( String role )
    {
        Election election = elections.get( role );
        if ( election == null || election.getVotes().isEmpty() )
        {
            return null;
        }

        elections.remove( role );

        return election.pickWinner();
    }

    public Comparable<Object> getCredentialsForRole( String role )
    {
        return electionCredentialsProvider.getCredentials( role );
    }

    public int getVoteCount( String role )
    {
        Election election = elections.get( role );
        if ( election != null )
        {
            List<Vote> voteList = election.getVotes();
            if ( voteList == null )
            {
                return 0;
            }

            return voteList.size();
        }
        else
        {
            return 0;
        }
    }

    public int getNeededVoteCount()
    {
        return clusterContext.getConfiguration().getMembers().size() - heartbeatContext.getFailed().size();
    }

    public void cancelElection( String role )
    {
        elections.remove( role );
    }

    public Iterable<String> getRolesRequiringElection()
    {
        return filter( new Predicate<String>() // Only include roles that are not elected
        {
            @Override
            public boolean accept( String role )
            {
                return clusterContext.getConfiguration().getElected( role ) == null;
            }
        }, map( new Function<ElectionRole, String>() // Convert ElectionRole to String
        {
            @Override
            public String apply( ElectionRole role )
            {
                return role.getName();
            }
        }, roles ) );
    }

    private static class Vote
            implements Comparable<Vote>
    {
        private final URI suggestedNode;
        private final Comparable<Object> voteCredentials;

        private Vote( URI suggestedNode, Comparable<Object> voteCredentials )
        {
            this.suggestedNode = suggestedNode;
            this.voteCredentials = voteCredentials;
        }

        public URI getSuggestedNode()
        {
            return suggestedNode;
        }

        @Override
        public int compareTo( Vote o )
        {
            return this.voteCredentials.compareTo( o.voteCredentials );
        }

        @Override
        public String toString()
        {
            return suggestedNode + ":" + voteCredentials;
        }

        public Comparable<Object> getCredentials()
        {
            return voteCredentials;
        }
    }

    private static class Election
    {
        private WinnerStrategy winnerStrategy;
        List<Vote> votes = new ArrayList<Vote>();

        private Election( WinnerStrategy winnerStrategy )
        {
            this.winnerStrategy = winnerStrategy;
        }

        public List<Vote> getVotes()
        {
            return votes;
        }

        public URI pickWinner()
        {
            return winnerStrategy.pickWinner( votes );
        }
    }

    interface WinnerStrategy
    {
        URI pickWinner( List<Vote> votes );
    }
}
