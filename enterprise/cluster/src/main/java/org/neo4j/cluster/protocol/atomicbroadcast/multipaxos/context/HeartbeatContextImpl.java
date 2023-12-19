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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.LogProvider;

class HeartbeatContextImpl extends AbstractContextImpl implements HeartbeatContext
{
    // HeartbeatContext
    private Set<InstanceId> failed = new HashSet<>();

    private Map<InstanceId, Set<InstanceId>> nodeSuspicions = new HashMap<>();

    private final Listeners<HeartbeatListener> heartBeatListeners;

    private final Executor executor;
    private ClusterContext clusterContext;
    private LearnerContext learnerContext;

    HeartbeatContextImpl( InstanceId me, CommonContextState commonState, LogProvider logging,
                          Timeouts timeouts, Executor executor )
    {
        super( me, commonState, logging, timeouts );
        this.executor = executor;
        this.heartBeatListeners = new Listeners<>();
    }

    private HeartbeatContextImpl( InstanceId me, CommonContextState commonState, LogProvider logging, Timeouts timeouts,
                                  Set<InstanceId> failed, Map<InstanceId, Set<InstanceId>> nodeSuspicions,
                                  Listeners<HeartbeatListener> heartBeatListeners, Executor executor )
    {
        super( me, commonState, logging, timeouts );
        this.failed = failed;
        this.nodeSuspicions = nodeSuspicions;
        this.heartBeatListeners = heartBeatListeners;
        this.executor = executor;
    }

    void setCircularDependencies( ClusterContext clusterContext, LearnerContext learnerContext )
    {
        this.clusterContext = clusterContext;
        this.learnerContext = learnerContext;
    }

    @Override
    public void started()
    {
        failed.clear();
    }

    /**
     * @return True iff the node was suspected
     */
    @Override
    public boolean alive( InstanceId node )
    {
        Set<InstanceId> serverSuspicions = suspicionsFor( getMyId() );
        boolean suspected = serverSuspicions.remove( node );

        if ( !isFailedBasedOnSuspicions( node ) && failed.remove( node ) )
        {
            getLog( HeartbeatContext.class ).info( "Notifying listeners that instance " + node + " is alive" );
            heartBeatListeners.notify( executor, listener -> listener.alive( node ) );
        }

        return suspected;
    }

    @Override
    public void suspect( InstanceId node )
    {
        Set<InstanceId> serverSuspicions = suspicionsFor( getMyId() );

        if ( !serverSuspicions.contains( node ) )
        {
            serverSuspicions.add( node );

            getLog( HeartbeatContext.class ).info( getMyId() + "(me) is now suspecting " + node );
        }

        if ( isFailedBasedOnSuspicions( node ) && !failed.contains( node ) )
        {
            getLog( HeartbeatContext.class ).info( "Notifying listeners that instance " + node + " is failed" );
            failed.add( node );
            heartBeatListeners.notify( executor, listener -> listener.failed( node ) );
        }

        if ( checkSuspectEverybody() )
        {
            getLog( HeartbeatContext.class )
                    .warn( "All other instances are being suspected. Moving on to mark all other instances as failed" );
            markAllOtherMembersAsFailed();
        }
    }

    /*
     * Alters state so that all instances are marked as failed. The state is changed so that any timeouts will not
     * reset an instance to alive, allowing only for real heartbeats from an instance to mark it again as alive. This
     * method is expected to be called in the event where all instances are being suspected, in which case a network
     * partition has happened and we need to set ourselves in an unavailable state.
     * The way this method achieves its task is by introducing suspicions from everybody about everybody. This mimics
     * the normal way of doing things, effectively faking a series of suspicion messages from every other instance
     * before connectivity was lost. As a result, when connectivity is restored, the state will be restored properly
     * for every instance that actually manages to reconnect.
     */
    private void markAllOtherMembersAsFailed()
    {
        Set<InstanceId> everyoneElse = new HashSet<>();
        for ( InstanceId instanceId : getMembers().keySet() )
        {
            if ( !isMe( instanceId ) )
            {
                everyoneElse.add( instanceId );
            }
        }

        for ( InstanceId instanceId : everyoneElse )
        {
            Set<InstanceId> instancesThisInstanceSuspects = new HashSet<>( everyoneElse );
            instancesThisInstanceSuspects.remove( instanceId ); // obviously an instance cannot suspect itself
            suspicions( instanceId, instancesThisInstanceSuspects );
        }
    }

    /**
     * Returns true iff this instance suspects every other instance currently in the cluster, except for itself.
     */
    private boolean checkSuspectEverybody()
    {
        Map<InstanceId, URI> allClusterMembers = getMembers();
        Set<InstanceId> suspectedInstances = getSuspicionsFor( getMyId() );
        int suspected = 0;
        for ( InstanceId suspectedInstance : suspectedInstances )
        {
            if ( allClusterMembers.containsKey( suspectedInstance ) )
            {
                suspected++;
            }
        }

        return suspected == allClusterMembers.size() - 1;
    }

    @Override
    public void suspicions( InstanceId from, Set<InstanceId> suspicions )
    {
        /*
         * A thing to be careful about here is the case where a cluster member is marked as failed but it's not yet
         * in the failed set. This implies the member has gathered enough suspicions to be marked as failed but is
         * not yet marked as such. This can happen if there is a cluster partition containing only us, in which case
         * markAllOthersAsFailed() will suspect everyone but not add them to failed (this happens here, further down).
         * In this case, all suspicions must be processed, since after processing half, the other half of the cluster
         * will be marked as failed (it has gathered enough suspicions) but we still need to process their messages, in
         * order to mark as failed the other half.
         */
        if ( isFailedBasedOnSuspicions( from ) && !failed.contains( from ) )
        {
            getLog( HeartbeatContext.class ).info(
                    "Ignoring suspicions from failed instance " + from + ": " + Iterables.toString( suspicions, "," ) );
            return;
        }

        Set<InstanceId> serverSuspicions = suspicionsFor( from );

        // Check removals
        Iterator<InstanceId> suspicionsIterator = serverSuspicions.iterator();
        while ( suspicionsIterator.hasNext() )
        {
            InstanceId currentSuspicion = suspicionsIterator.next();
            if ( !suspicions.contains( currentSuspicion ) )
            {
                getLog( HeartbeatContext.class ).info( from + " is no longer suspecting " + currentSuspicion );
                suspicionsIterator.remove();
            }
        }

        // Check additions
        for ( InstanceId suspicion : suspicions )
        {
            if ( !serverSuspicions.contains( suspicion ) )
            {
                getLog( HeartbeatContext.class ).info( from + " is now suspecting " + suspicion );
                serverSuspicions.add( suspicion );
            }
        }

        // Check if anyone is considered failed
        for ( InstanceId node : suspicions )
        {
            if ( isFailedBasedOnSuspicions( node ) && !failed.contains( node ) )
            {
                failed.add( node );
                heartBeatListeners.notify( executor, listener -> listener.failed( node ) );
            }
        }
    }

    @Override
    public Set<InstanceId> getFailed()
    {
        return failed;
    }

    @Override
    public Iterable<InstanceId> getAlive()
    {
        return Iterables.filter( item -> !isFailedBasedOnSuspicions( item ), commonState.configuration().getMemberIds() );
    }

    @Override
    public void addHeartbeatListener( HeartbeatListener listener )
    {
        heartBeatListeners.add( listener );
    }

    @Override
    public void removeHeartbeatListener( HeartbeatListener listener )
    {
        heartBeatListeners.remove( listener );
    }

    @Override
    public void serverLeftCluster( InstanceId node )
    {
        failed.remove( node );
        for ( Set<InstanceId> uris : nodeSuspicions.values() )
        {
            uris.remove( node );
        }
    }

    @Override
    public boolean isFailedBasedOnSuspicions( InstanceId node )
    {
        List<InstanceId> suspicionsForNode = getSuspicionsOf( node );
        int countOfInstancesSuspectedByMe = getSuspicionsFor( getMyId() ).size();

        /*
         * If more than half *non suspected instances* suspect this node, fail it. This takes care of partitions
         * that contain less than half of the cluster, ensuring that they will eventually detect the disconnect without
         * waiting to have a majority of suspicions. This is accomplished by counting as quorum only instances
         * that are not suspected by me.
         */
        return suspicionsForNode.size() >
                (commonState.configuration().getMembers().size() - countOfInstancesSuspectedByMe ) / 2;
    }

    /**
     * Get all of the servers which suspect a specific member.
     *
     * @param instanceId for the member of interest.
     * @return a set of servers which suspect the specified member.
     */
    @Override
    public List<InstanceId> getSuspicionsOf( InstanceId instanceId )
    {
        List<InstanceId> suspicions = new ArrayList<>();
        for ( InstanceId member : commonState.configuration().getMemberIds() )
        {
            Set<InstanceId> memberSuspicions = nodeSuspicions.get( member );
            if ( memberSuspicions != null && !failed.contains( member )
                    && memberSuspicions.contains( instanceId ) )
            {
                suspicions.add( member );
            }
        }

        return suspicions;
    }

    /**
     * Get the suspicions as reported by a specific server.
     *
     * @param instanceId which might suspect someone.
     * @return a list of those members which server suspects.
     */
    @Override
    public Set<InstanceId> getSuspicionsFor( InstanceId instanceId )
    {
        Set<org.neo4j.cluster.InstanceId> suspicions = suspicionsFor( instanceId );
        return new HashSet<>( suspicions );
    }

    private Set<InstanceId> suspicionsFor( InstanceId instanceId )
    {
        return nodeSuspicions.computeIfAbsent( instanceId, k -> new HashSet<>() );
    }

    @Override
    public Iterable<InstanceId> getOtherInstances()
    {
        return clusterContext.getOtherInstances();
    }

    @Override
    public long getLastKnownLearnedInstanceInCluster()
    {
        return learnerContext.getLastKnownLearnedInstanceInCluster();
    }

    @Override
    public long getLastLearnedInstanceId()
    {
        return learnerContext.getLastLearnedInstanceId();
    }

    @Override
    public void failed( InstanceId instanceId )
    {
        failed.add( instanceId );
    }

    public HeartbeatContextImpl snapshot( CommonContextState commonStateSnapshot, LogProvider logging,
            Timeouts timeouts, Executor executor )
    {
        return new HeartbeatContextImpl( me, commonStateSnapshot, logging, timeouts, new HashSet<>( failed ),
                new HashMap<>( nodeSuspicions ), new Listeners<>( heartBeatListeners ), executor );
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

        HeartbeatContextImpl that = (HeartbeatContextImpl) o;

        if ( failed != null ? !failed.equals( that.failed ) : that.failed != null )
        {
            return false;
        }
        return nodeSuspicions != null ? nodeSuspicions.equals( that.nodeSuspicions ) : that.nodeSuspicions == null;
    }

    @Override
    public int hashCode()
    {
        int result = failed != null ? failed.hashCode() : 0;
        result = 31 * result + (nodeSuspicions != null ? nodeSuspicions.hashCode() : 0);
        return result;
    }
}
