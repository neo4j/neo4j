/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.cluster.ClusterInstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.election.ElectionContext;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionRole;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentials;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.cluster.util.Quorums.isQuorum;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.Iterables.toList;

class ElectionContextImpl
    extends AbstractContextImpl
    implements ElectionContext
{
    private final ClusterContext clusterContext;
    private final HeartbeatContext heartbeatContext;

    private final List<ElectionRole> roles;
    private final Map<String, Election> elections;
    private final ElectionCredentialsProvider electionCredentialsProvider;

    ElectionContextImpl( ClusterInstanceId me, CommonContextState commonState,
                         Logging logging,
                         Timeouts timeouts, Iterable<ElectionRole> roles, ClusterContext clusterContext,
                         HeartbeatContext heartbeatContext, ElectionCredentialsProvider electionCredentialsProvider )
    {
        super( me, commonState, logging, timeouts );
        this.electionCredentialsProvider = electionCredentialsProvider;
        this.roles = new ArrayList<>(toList(roles));
        this.elections = new HashMap<>();
        this.clusterContext = clusterContext;
        this.heartbeatContext = heartbeatContext;
    }

    ElectionContextImpl( ClusterInstanceId me, CommonContextState commonState, Logging logging, Timeouts timeouts,
                         ClusterContext clusterContext, HeartbeatContext heartbeatContext, List<ElectionRole> roles,
                         Map<String, Election> elections, ElectionCredentialsProvider electionCredentialsProvider )
    {
        super( me, commonState, logging, timeouts );
        this.clusterContext = clusterContext;
        this.heartbeatContext = heartbeatContext;
        this.roles = roles;
        this.elections = elections;
        this.electionCredentialsProvider = electionCredentialsProvider;
    }

    @Override
    public void created()
    {
        for ( ElectionRole role : roles )
        {
            // Elect myself for all roles
            clusterContext.elected( role.getName(), clusterContext.getMyId() );
        }
    }

    @Override
    public List<ElectionRole> getPossibleRoles()
    {
        return roles;
    }

    /*
     * Removes all roles from the provided node. This is expected to be the first call when receiving a demote
     * message for a node, since it is the way to ensure that election will happen for each role that node had
     */
    @Override
    public void nodeFailed( ClusterInstanceId node )
    {
        Iterable<String> rolesToDemote = getRoles( node );
        for ( String role : rolesToDemote )
        {
            clusterContext.getConfiguration().removeElected( role );
        }
    }

    @Override
    public Iterable<String> getRoles( ClusterInstanceId server )
    {
        return clusterContext.getConfiguration().getRolesOf( server );
    }

    @Override
    public void unelect( String roleName )
    {
        clusterContext.getConfiguration().removeElected( roleName );
    }

    @Override
    public boolean isElectionProcessInProgress( String role )
    {
        return elections.containsKey( role );
    }

    @Override
    public void startDemotionProcess( String role, final ClusterInstanceId demoteNode )
    {
        elections.put( role, new Election( new BiasedWinnerStrategy( demoteNode, false /*demotion*/,
                clusterContext.getLogger( BiasedWinnerStrategy.class ) ) ) );
    }

    @Override
    public void startElectionProcess( String role )
    {
        clusterContext.getLogger( getClass() ).info( "Doing elections for role " + role );
        elections.put( role, new Election( new WinnerStrategy()
        {
            @Override
            public ClusterInstanceId pickWinner( Collection<Vote> voteList )
            {
                // Remove blank votes
                List<Vote> filteredVoteList = removeBlankVotes( voteList );

                // Sort based on credentials
                // The most suited candidate should come out on top
                Collections.sort( filteredVoteList );
                Collections.reverse( filteredVoteList );

                clusterContext.getLogger( getClass() ).debug( "Election started with " + voteList +
                        ", ended up with " + filteredVoteList );

                // Elect this highest voted instance
                for ( Vote vote : filteredVoteList )
                {
                    return vote.getSuggestedNode();
                }

                // No possible winner
                return null;
            }
        } ) );
    }

    @Override
    public void startPromotionProcess( String role, final ClusterInstanceId promoteNode )
    {
        elections.put( role, new Election( new BiasedWinnerStrategy( promoteNode, true /*promotion*/,
                clusterContext.getLogger( BiasedWinnerStrategy.class ) ) ) );
    }

    @Override
    public void voted( String role, ClusterInstanceId suggestedNode, Comparable<Object> suggestionCredentials )
    {
        if ( isElectionProcessInProgress( role ) )
        {
            Map<ClusterInstanceId, Vote> votes = elections.get( role ).getVotes();
            votes.put( suggestedNode, new Vote( suggestedNode, suggestionCredentials ) );
        }
    }

    @Override
    public ClusterInstanceId getElectionWinner( String role )
    {
        Election election = elections.get( role );
        if ( election == null || election.getVotes().size() != getNeededVoteCount() )
        {
            return null;
        }

        elections.remove( role );

        return election.pickWinner();
    }

    @Override
    public Comparable<Object> getCredentialsForRole( String role )
    {
        return electionCredentialsProvider.getCredentials( role );
    }

    @Override
    public int getVoteCount( String role )
    {
        Election election = elections.get( role );
        if ( election != null )
        {
            Map<ClusterInstanceId, Vote> voteList = election.getVotes();
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

    @Override
    public int getNeededVoteCount()
    {
        return clusterContext.getConfiguration().getMembers().size() - heartbeatContext.getFailed().size();
    }

    @Override
    public void cancelElection( String role )
    {
        elections.remove( role );
    }

    @Override
    public Iterable<String> getRolesRequiringElection()
    {
        return Iterables.filter( new Predicate<String>() // Only include roles that are not elected
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

    @Override
    public boolean electionOk()
    {
        int total = clusterContext.getConfiguration().getMembers().size();
        int available = total - heartbeatContext.getFailed().size();
        return isQuorum(available, total);
    }

    @Override
    public boolean isInCluster()
    {
        return clusterContext.isInCluster();
    }

    @Override
    public Iterable<ClusterInstanceId> getAlive()
    {
        return heartbeatContext.getAlive();
    }

    @Override
    public ClusterInstanceId getMyId()
    {
        return clusterContext.getMyId();
    }

    @Override
    public boolean isElector()
    {
        // Only the first alive server should try elections. Everyone else waits
        List<ClusterInstanceId> aliveInstances = toList( getAlive() );
        Collections.sort( aliveInstances );
        return aliveInstances.indexOf( getMyId() ) == 0;
    }

    @Override
    public boolean isFailed( ClusterInstanceId key )
    {
        return heartbeatContext.getFailed().contains( key );
    }

    @Override
    public ClusterInstanceId getElected( String roleName )
    {
        return clusterContext.getConfiguration().getElected( roleName );
    }

    @Override
    public boolean hasCurrentlyElectedVoted( String role, ClusterInstanceId currentElected )
    {
        return elections.containsKey( role ) && elections.get(role).getVotes().containsKey( currentElected );
    }

    @Override
    public Set<ClusterInstanceId> getFailed()
    {
        return heartbeatContext.getFailed();
    }

    public ElectionContextImpl snapshot( CommonContextState commonStateSnapshot, Logging logging, Timeouts timeouts,
                                         ClusterContextImpl snapshotClusterContext,
                                         HeartbeatContextImpl snapshotHeartbeatContext,
                                         ElectionCredentialsProvider credentialsProvider )

    {
        Map<String, Election> electionsSnapshot = new HashMap<>();
        for ( Map.Entry<String, Election> election : elections.entrySet() )
        {
            electionsSnapshot.put( election.getKey(), election.getValue().snapshot() );
        }

        return new ElectionContextImpl( me, commonStateSnapshot, logging, timeouts, snapshotClusterContext,
                snapshotHeartbeatContext, new ArrayList<>(roles), electionsSnapshot, credentialsProvider );
    }

    public static class Vote
            implements Comparable<Vote>
    {
        private final ClusterInstanceId suggestedNode;
        private final Comparable<Object> voteCredentials;

        public Vote( ClusterInstanceId suggestedNode, Comparable<Object> voteCredentials )
        {
            this.suggestedNode = suggestedNode;
            this.voteCredentials = voteCredentials;
        }

        public ClusterInstanceId getSuggestedNode()
        {
            return suggestedNode;
        }

        public Comparable<Object> getCredentials()
        {
            return voteCredentials;
        }

        @Override
        public String toString()
        {
            return suggestedNode + ":" + voteCredentials;
        }

        @Override
        public int compareTo( Vote o )
        {
            return this.voteCredentials.compareTo( o.voteCredentials );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Vote vote = (Vote) o;

            if ( !suggestedNode.equals( vote.suggestedNode ) )
            {
                return false;
            }
            if ( !voteCredentials.equals( vote.voteCredentials ) )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = suggestedNode.hashCode();
            result = 31 * result + voteCredentials.hashCode();
            return result;
        }
    }

    static class Election
    {
        private final WinnerStrategy winnerStrategy;
        private final Map<ClusterInstanceId, Vote> votes;

        private Election( WinnerStrategy winnerStrategy )
        {
            this.winnerStrategy = winnerStrategy;
            this.votes = new HashMap<ClusterInstanceId, Vote>();
        }

        private Election( WinnerStrategy winnerStrategy, HashMap<ClusterInstanceId, Vote> votes )
        {
            this.votes = votes;
            this.winnerStrategy = winnerStrategy;
        }

        public Map<ClusterInstanceId, Vote> getVotes()
        {
            return votes;
        }

        public ClusterInstanceId pickWinner()
        {
            return winnerStrategy.pickWinner( votes.values() );
        }

        public Election snapshot()
        {
            return new Election( winnerStrategy, new HashMap<>(votes));
        }
    }

    interface WinnerStrategy
    {
        ClusterInstanceId pickWinner( Collection<Vote> votes );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ElectionContextImpl that = (ElectionContextImpl) o;

        if ( elections != null ? !elections.equals( that.elections ) : that.elections != null )
        {
            return false;
        }
        if ( roles != null ? !roles.equals( that.roles ) : that.roles != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = roles != null ? roles.hashCode() : 0;
        result = 31 * result + (elections != null ? elections.hashCode() : 0);
        return result;
    }
    
    public static class BiasedWinnerStrategy implements WinnerStrategy
    {
        private final ClusterInstanceId biasedNode;
        private final boolean positiveSuggestion;
        private final StringLogger logger;

        public static BiasedWinnerStrategy promotion(ClusterContext clusterContext, ClusterInstanceId biasedNode)
        {
            return new BiasedWinnerStrategy( biasedNode, true, clusterContext.getLogger( BiasedWinnerStrategy.class ) );
        }

        public static BiasedWinnerStrategy demotion(ClusterContext clusterContext, ClusterInstanceId biasedNode)
        {
            return new BiasedWinnerStrategy( biasedNode, false, clusterContext.getLogger( BiasedWinnerStrategy.class ) );
        }

        public BiasedWinnerStrategy( ClusterInstanceId biasedNode, boolean positiveSuggestion, StringLogger logger )
        {
            this.biasedNode = biasedNode;
            this.positiveSuggestion = positiveSuggestion;
            this.logger = logger;
        }

        @Override
        public ClusterInstanceId pickWinner( Collection<Vote> voteList )
        {
            // Remove blank votes
            List<Vote> filteredVoteList = removeBlankVotes( voteList );

            // Sort based on credentials
            // The most suited candidate should come out on top
            Collections.sort( filteredVoteList );
            Collections.reverse( filteredVoteList );

            logger.debug( "Election started with " + voteList +
                    ", ended up with " + filteredVoteList + " where " + biasedNode + " is biased for " +
                    (positiveSuggestion ? "promotion" : "demotion") );

            for ( Vote vote : filteredVoteList )
            {
                // Elect the biased instance biased as winner
                if ( vote.getSuggestedNode().equals( biasedNode ) == positiveSuggestion )
                {
                    return vote.getSuggestedNode();
                }
            }

            // No possible winner
            return null;
        }
    }

    private static List<Vote> removeBlankVotes( Collection<Vote> voteList )
    {
        return toList( Iterables.filter( new Predicate<Vote>()
        {
            @Override
            public boolean accept( Vote item )
            {
                return !(item.getCredentials() instanceof NotElectableElectionCredentials);
            }
        }, voteList ) );
    }
}
