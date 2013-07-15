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

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.StringLogger;

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
            clusterContext.elected( role.getName(), clusterContext.getMyId() );
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
    public void nodeFailed( InstanceId node )
    {
        Iterable<String> rolesToDemote = getRoles( node );
        for ( String role : rolesToDemote )
        {
            clusterContext.getConfiguration().removeElected( role );
        }
    }

    public Iterable<String> getRoles( InstanceId server )
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

    public void startDemotionProcess( String role, final InstanceId demoteNode )
    {
        elections.put( role, new Election( new WinnerStrategy()
        {
            @Override
            public InstanceId pickWinner( List<Vote> voteList )
            {

                // Remove blank votes
                List<Vote> filteredVoteList = Iterables.toList( Iterables.filter( new Predicate<Vote>()
                {
                    @Override
                    public boolean accept( Vote item )
                    {
                        return !( item.getCredentials() instanceof NotElectableElectionCredentials );
                    }
                }, voteList ) );

                // Sort based on credentials
                // The most suited candidate should come out on top
                Collections.sort( filteredVoteList );
                Collections.reverse( filteredVoteList );

                for ( Vote vote : filteredVoteList )
                {
                    // Don't elect as winner the node we are trying to demote
                    if ( !vote.getSuggestedNode().equals( demoteNode ) )
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
        clusterContext.getLogger( getClass() ).info( "Doing elections for role " + role );
        elections.put( role, new Election( new WinnerStrategy()
        {
            @Override
            public InstanceId pickWinner( List<Vote> voteList )
            {
                // Remove blank votes
                List<Vote> filteredVoteList = Iterables.toList( Iterables.filter( new Predicate<Vote>()
                {
                    @Override
                    public boolean accept( Vote item )
                    {
                        return !( item.getCredentials() instanceof NotElectableElectionCredentials );
                    }
                }, voteList ) );

                // Sort based on credentials
                // The most suited candidate should come out on top
                Collections.sort( filteredVoteList );
                Collections.reverse( filteredVoteList );

                clusterContext.getLogger( getClass() ).debug( "Elections ended up with list " + filteredVoteList );

                for ( Vote vote : filteredVoteList )
                {
                    return vote.getSuggestedNode();
                }

                // No possible winner
                return null;
            }
        } ) );
    }

    public void startPromotionProcess( String role, final InstanceId promoteNode )
    {
        elections.put( role, new Election( new WinnerStrategy()
        {
            @Override
            public InstanceId pickWinner( List<Vote> voteList )
            {

                // Remove blank votes
                List<Vote> filteredVoteList = Iterables.toList( Iterables.filter( new Predicate<Vote>()
                {
                    @Override
                    public boolean accept( Vote item )
                    {
                        return !( item.getCredentials() instanceof NotElectableElectionCredentials);
                    }
                }, voteList ) );

                // Sort based on credentials
                // The most suited candidate should come out on top
                Collections.sort( filteredVoteList );
                Collections.reverse( filteredVoteList );

                for ( Vote vote : filteredVoteList )
                {
                    // Don't elect as winner the node we are trying to demote
                    if ( !vote.getSuggestedNode().equals( promoteNode ) )
                    {
                        return vote.getSuggestedNode();
                    }
                }

                // No possible winner
                return null;
            }
        } ) );
    }

    public void voted( String role, InstanceId suggestedNode, Comparable<Object> suggestionCredentials )
    {
        if ( isElectionProcessInProgress( role ) )
        {
            List<Vote> voteList = elections.get( role ).getVotes();
            voteList.add( new Vote( suggestedNode, suggestionCredentials ) );
        }
    }

    public InstanceId getElectionWinner( String role )
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

    public boolean electionOk()
    {
        return heartbeatContext.getFailed().size() <= clusterContext.getConfiguration().getMembers().size() /2;
    }

    public boolean isInCluster()
    {
        return getClusterContext().isInCluster();
    }

    public Iterable<InstanceId> getAlive()
    {
        return getHeartbeatContext().getAlive();
    }

    public InstanceId getMyId()
    {
        return getClusterContext().getMyId();
    }

    public boolean isElector()
    {
        // Only the first alive server should try elections. Everyone else waits
        List<InstanceId> aliveInstances = Iterables.toList( getAlive() );
        Collections.sort( aliveInstances );
        return aliveInstances.indexOf( getMyId() ) == 0;
    }

    public Map<InstanceId, URI> getMembers()
    {
        return getClusterContext().getConfiguration().getMembers();
    }

    public boolean isFailed( InstanceId key )
    {
        return getHeartbeatContext().getFailed().contains( key );
    }

    public InstanceId getElected( String roleName )
    {
        return getClusterContext().getConfiguration().getElected( roleName );
    }

    public void setTimeout( String key, Message<ElectionMessage> timeout )
    {
        getClusterContext().timeouts.setTimeout( key, timeout );
    }

    public StringLogger getLogger()
    {
        return clusterContext.getLogger( ElectionState.class );
    }

    private static class Vote
            implements Comparable<Vote>
    {
        private final InstanceId suggestedNode;
        private final Comparable<Object> voteCredentials;

        private Vote( InstanceId suggestedNode, Comparable<Object> voteCredentials )
        {
            this.suggestedNode = suggestedNode;
            this.voteCredentials = voteCredentials;
        }

        public InstanceId getSuggestedNode()
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

        public InstanceId pickWinner()
        {
            return winnerStrategy.pickWinner( votes );
        }
    }

    interface WinnerStrategy
    {
        InstanceId pickWinner( List<Vote> votes );
    }
}
