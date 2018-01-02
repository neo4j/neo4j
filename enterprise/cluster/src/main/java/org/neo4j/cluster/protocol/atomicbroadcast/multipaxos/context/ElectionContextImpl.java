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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.BiasedWinnerStrategy;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.Vote;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.WinnerStrategy;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.election.ElectionContext;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionRole;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentials;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.function.Function;
import org.neo4j.function.Predicate;
import org.neo4j.logging.LogProvider;

import static org.neo4j.cluster.util.Quorums.isQuorum;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.Iterables.toList;

public class ElectionContextImpl
        extends AbstractContextImpl
        implements ElectionContext, HeartbeatListener
{
    private final ClusterContext clusterContext;
    private final HeartbeatContext heartbeatContext;

    private final List<ElectionRole> roles;
    private final Map<String, Election> elections;
    private final ElectionCredentialsProvider electionCredentialsProvider;

    ElectionContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState,
                         LogProvider logging,
                         Timeouts timeouts, Iterable<ElectionRole> roles, ClusterContext clusterContext,
                         HeartbeatContext heartbeatContext, ElectionCredentialsProvider electionCredentialsProvider )
    {
        super( me, commonState, logging, timeouts );
        this.electionCredentialsProvider = electionCredentialsProvider;
        this.roles = new ArrayList<>(toList(roles));
        this.elections = new HashMap<>();
        this.clusterContext = clusterContext;
        this.heartbeatContext = heartbeatContext;

        heartbeatContext.addHeartbeatListener( this );
    }

    ElectionContextImpl( InstanceId me, CommonContextState commonState, LogProvider logging, Timeouts timeouts,
                         ClusterContext clusterContext, HeartbeatContext heartbeatContext, List<ElectionRole> roles,
                         Map<String, Election> elections, ElectionCredentialsProvider electionCredentialsProvider )
    {
        super( me, commonState, logging, timeouts );
        this.clusterContext = clusterContext;
        this.heartbeatContext = heartbeatContext;
        this.roles = roles;
        this.elections = elections;
        this.electionCredentialsProvider = electionCredentialsProvider;

        heartbeatContext.addHeartbeatListener( this );
    }

    @Override
    public void created()
    {
        for ( ElectionRole role : roles )
        {
            // Elect myself for all roles
            clusterContext.elected( role.getName(), clusterContext.getMyId(), clusterContext.getMyId(), 1 );
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
    public void nodeFailed( org.neo4j.cluster.InstanceId node )
    {
        Iterable<String> rolesToDemote = getRoles( node );
        for ( String role : rolesToDemote )
        {
            clusterContext.getConfiguration().removeElected( role );
        }
    }

    @Override
    public Iterable<String> getRoles( org.neo4j.cluster.InstanceId server )
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
    public void startDemotionProcess( String role, final org.neo4j.cluster.InstanceId demoteNode )
    {
        elections.put( role, new Election( BiasedWinnerStrategy.demotion( clusterContext, demoteNode ) ) );
    }

    @Override
    public void startElectionProcess( String role )
    {
        clusterContext.getLog( getClass() ).info( "Doing elections for role " + role );
        if ( !clusterContext.getMyId().equals( clusterContext.getLastElector() ) )
        {
            clusterContext.setLastElector( clusterContext.getMyId() );
        }
        elections.put( role, new Election( new WinnerStrategy()
        {
            @Override
            public org.neo4j.cluster.InstanceId pickWinner( Collection<Vote> voteList )
            {
                // Remove blank votes
                List<Vote> filteredVoteList = removeBlankVotes( voteList );

                // Sort based on credentials
                // The most suited candidate should come out on top
                Collections.sort( filteredVoteList );
                Collections.reverse( filteredVoteList );

                clusterContext.getLog( getClass() ).debug( "Election started with " + voteList +
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
    public void startPromotionProcess( String role, final org.neo4j.cluster.InstanceId promoteNode )
    {
        elections.put( role, new Election( BiasedWinnerStrategy.promotion( clusterContext, promoteNode )  ) );
    }

    @Override
    public boolean voted( String role, org.neo4j.cluster.InstanceId suggestedNode,
                          Comparable<Object> suggestionCredentials, long electionVersion )
    {
        if ( !isElectionProcessInProgress( role ) ||
                (electionVersion != -1 && electionVersion < clusterContext.getLastElectorVersion() ) )
        {
            return false;
        }
        Map<org.neo4j.cluster.InstanceId, Vote> votes = elections.get( role ).getVotes();
        votes.put( suggestedNode, new Vote( suggestedNode, suggestionCredentials ) );
        return true;
    }

    @Override
    public org.neo4j.cluster.InstanceId getElectionWinner( String role )
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
            Map<org.neo4j.cluster.InstanceId, Vote> voteList = election.getVotes();
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
        // TODO increment election epoch
    }

    @Override
    public void forgetElection( String role )
    {
        elections.remove( role );
        clusterContext.setLastElectorVersion( clusterContext.getLastElectorVersion() + 1 );
    }

    @Override
    public Iterable<String> getRolesRequiringElection()
    {
        return filter( new Predicate<String>() // Only include roles that are not elected
        {
            @Override
            public boolean test( String role )
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
        return isQuorum( available, total );
    }

    @Override
    public boolean isInCluster()
    {
        return clusterContext.isInCluster();
    }

    @Override
    public Iterable<org.neo4j.cluster.InstanceId> getAlive()
    {
        return heartbeatContext.getAlive();
    }

    @Override
    public org.neo4j.cluster.InstanceId getMyId()
    {
        return clusterContext.getMyId();
    }

    @Override
    public boolean isElector()
    {
        // Only the first alive server should try elections. Everyone else waits
        List<org.neo4j.cluster.InstanceId> aliveInstances = toList( getAlive() );
        Collections.sort( aliveInstances );
        return aliveInstances.indexOf( getMyId() ) == 0;
    }

    @Override
    public boolean isFailed( org.neo4j.cluster.InstanceId key )
    {
        return heartbeatContext.getFailed().contains( key );
    }

    @Override
    public org.neo4j.cluster.InstanceId getElected( String roleName )
    {
        return clusterContext.getConfiguration().getElected( roleName );
    }

    @Override
    public boolean hasCurrentlyElectedVoted( String role, org.neo4j.cluster.InstanceId currentElected )
    {
        return elections.containsKey( role ) && elections.get(role).getVotes().containsKey( currentElected );
    }

    @Override
    public Set<InstanceId> getFailed()
    {
        return heartbeatContext.getFailed();
    }

    public ElectionContextImpl snapshot( CommonContextState commonStateSnapshot, LogProvider logging, Timeouts timeouts,
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

    private static class Election
    {
        private final WinnerStrategy winnerStrategy;
        private final Map<org.neo4j.cluster.InstanceId, Vote> votes;

        private Election( WinnerStrategy winnerStrategy )
        {
            this.winnerStrategy = winnerStrategy;
            this.votes = new HashMap<InstanceId, Vote>();
        }

        private Election( WinnerStrategy winnerStrategy, HashMap<InstanceId, Vote> votes )
        {
            this.votes = votes;
            this.winnerStrategy = winnerStrategy;
        }

        public Map<InstanceId, Vote> getVotes()
        {
            return votes;
        }

        public InstanceId pickWinner()
        {
            return winnerStrategy.pickWinner( votes.values() );
        }

        public Election snapshot()
        {
            return new Election( winnerStrategy, new HashMap<>(votes));
        }
    }

    @Override
    public ClusterMessage.VersionedConfigurationStateChange newConfigurationStateChange()
    {
        ClusterMessage.VersionedConfigurationStateChange result = new ClusterMessage
                .VersionedConfigurationStateChange();
        result.setElector( clusterContext.getMyId() );
        result.setVersion( clusterContext.getLastElectorVersion() );
        return result;
    }

    @Override
    public VoteRequest voteRequestForRole( ElectionRole role )
    {
        return new VoteRequest( role.getName(), clusterContext.getLastElectorVersion() );
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

    @Override
    public void failed( org.neo4j.cluster.InstanceId server )
    {
        for ( Map.Entry<String, Election> ongoingElection : elections.entrySet() )
        {
            ongoingElection.getValue().getVotes().remove( server );
        }
    }

    @Override
    public void alive( org.neo4j.cluster.InstanceId server )
    {
        // Not needed
    }

    public static List<Vote> removeBlankVotes( Collection<Vote> voteList )
    {
        return toList( filter( new Predicate<Vote>()
        {
            @Override
            public boolean test( Vote item )
            {
                return !(item.getCredentials() instanceof NotElectableElectionCredentials);
            }
        }, voteList ) );
    }
}
