/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.helpers.collection.Iterables.filter;

class HeartbeatContextImpl
        extends AbstractContextImpl
        implements HeartbeatContext
{
    // HeartbeatContext
    private Set<InstanceId> failed = new HashSet<InstanceId>();

    private Map<InstanceId, Set<InstanceId>> nodeSuspicions = new HashMap<InstanceId, Set<InstanceId>>();

    private Iterable<HeartbeatListener> heartBeatListeners = Listeners.newListeners();

    private final Executor executor;
    private ClusterContext clusterContext;
    private LearnerContext learnerContext;

    HeartbeatContextImpl( InstanceId me, CommonContextState commonState, Logging logging,
                          Timeouts timeouts, Executor executor )
    {
        super( me, commonState, logging, timeouts );
        this.executor = executor;
    }

    public void setCircularDependencies( ClusterContext clusterContext, LearnerContext learnerContext )
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
    public boolean alive( final org.neo4j.cluster.InstanceId node )
    {
        Set<org.neo4j.cluster.InstanceId> serverSuspicions = suspicionsFor( getMyId() );
        boolean suspected = serverSuspicions.remove( node );

        if ( !isFailed( node ) && failed.remove( node ) )
        {
            getLogger( HeartbeatContext.class ).info( "Notifying listeners that instance " + node + " is alive" );
            Listeners.notifyListeners( heartBeatListeners, new Listeners.Notification<HeartbeatListener>()
            {
                @Override
                public void notify( HeartbeatListener listener )
                {
                    listener.alive( node );
                }
            } );
        }

        return suspected;
    }

    @Override
    public void suspect( final org.neo4j.cluster.InstanceId node )
    {
        Set<org.neo4j.cluster.InstanceId> serverSuspicions = suspicionsFor( getMyId() );

        if ( !serverSuspicions.contains( node ) )
        {
            serverSuspicions.add( node );

            getLogger( HeartbeatContext.class ).info( getMyId() + "(me) is now suspecting " + node );
        }

        if ( isFailed( node ) && !failed.contains( node ) )
        {
            getLogger( HeartbeatContext.class ).info( "Notifying listeners that instance " + node + " is failed" );
            failed.add( node );
            Listeners.notifyListeners( heartBeatListeners, executor, new Listeners.Notification<HeartbeatListener>()
            {
                @Override
                public void notify( HeartbeatListener listener )
                {
                    listener.failed( node );
                }
            } );
        }
    }

    @Override
    public void suspicions( org.neo4j.cluster.InstanceId from, Set<org.neo4j.cluster.InstanceId> suspicions )
    {
        Set<org.neo4j.cluster.InstanceId> serverSuspicions = suspicionsFor( from );

        // Check removals
        Iterator<InstanceId> suspicionsIterator = serverSuspicions.iterator();
        while ( suspicionsIterator.hasNext() )
        {
            org.neo4j.cluster.InstanceId currentSuspicion = suspicionsIterator.next();
            if ( !suspicions.contains( currentSuspicion ) )
            {
                getLogger( HeartbeatContext.class ).info( from + " is no longer suspecting " + currentSuspicion );
                suspicionsIterator.remove();
            }
        }

        // Check additions
        for ( org.neo4j.cluster.InstanceId suspicion : suspicions )
        {
            if ( !serverSuspicions.contains( suspicion ) )
            {
                getLogger( HeartbeatContext.class ).info( from + " is now suspecting " + suspicion );
                serverSuspicions.add( suspicion );
            }
        }

        // Check if anyone is considered failed
        for ( final org.neo4j.cluster.InstanceId node : suspicions )
        {
            if ( isFailed( node ) && !failed.contains( node ) )
            {
                failed.add( node );
                Listeners.notifyListeners( heartBeatListeners, executor, new Listeners.Notification<HeartbeatListener>()
                {
                    @Override
                    public void notify( HeartbeatListener listener )
                    {
                        listener.failed( node );
                    }
                } );
            }
        }
    }

    @Override
    public Set<org.neo4j.cluster.InstanceId> getFailed()
    {
        return failed;
    }

    @Override
    public Iterable<org.neo4j.cluster.InstanceId> getAlive()
    {
        return filter( new Predicate<InstanceId>()
        {
            @Override
            public boolean accept( org.neo4j.cluster.InstanceId item )
            {
                return !isFailed( item );
            }
        }, commonState.configuration().getMemberIds() );
    }

    @Override
    public void addHeartbeatListener( HeartbeatListener listener )
    {
        heartBeatListeners = Listeners.addListener( listener, heartBeatListeners );
    }

    @Override
    public void removeHeartbeatListener( HeartbeatListener listener )
    {
        heartBeatListeners = Listeners.removeListener( listener, heartBeatListeners );
    }

    @Override
    public void serverLeftCluster( org.neo4j.cluster.InstanceId node )
    {
        failed.remove( node );
        for ( Set<org.neo4j.cluster.InstanceId> uris : nodeSuspicions.values() )
        {
            uris.remove( node );
        }
    }

    @Override
    public boolean isFailed( org.neo4j.cluster.InstanceId node )
    {
        List<InstanceId> suspicions = getSuspicionsOf( node );

            /*
             * This looks weird but trust me, there is a reason for it.
             * See below in the test, where we subtract the failed size() from the total cluster size? If the instance
             * under question is already in the failed set then that's it, as expected. But if it is not in the failed set
             * then we must not take it's opinion under consideration (which we implicitly don't for every member of the
             * failed set). That's what the adjust represents - the node's opinion on whether it is alive or not. Run a
             * 3 cluster simulation in your head with 2 instances failed and one coming back online and you'll see why.
             */
        int adjust = failed.contains( node ) ? 0 : 1;

        // If more than half suspect this node, fail it
        return suspicions.size() >
                (commonState.configuration().getMembers().size() - failed.size() - adjust) / 2;
    }

    @Override
    public List<org.neo4j.cluster.InstanceId> getSuspicionsOf( org.neo4j.cluster.InstanceId server )
    {
        List<org.neo4j.cluster.InstanceId> suspicions = new ArrayList<InstanceId>();
        for ( org.neo4j.cluster.InstanceId member : commonState.configuration().getMemberIds() )
        {
            Set<org.neo4j.cluster.InstanceId> memberSuspicions = nodeSuspicions.get( member );
            if ( memberSuspicions != null && !failed.contains( member )
                    && memberSuspicions.contains( server ) )
            {
                suspicions.add( member );
            }
        }

        return suspicions;
    }

    @Override
    public Set<org.neo4j.cluster.InstanceId> getSuspicionsFor( org.neo4j.cluster.InstanceId uri )
    {
        Set<org.neo4j.cluster.InstanceId> suspicions = suspicionsFor( uri );
        return new HashSet<org.neo4j.cluster.InstanceId>( suspicions );
    }

    private Set<org.neo4j.cluster.InstanceId> suspicionsFor( org.neo4j.cluster.InstanceId uri )
    {
        Set<org.neo4j.cluster.InstanceId> serverSuspicions = nodeSuspicions.get( uri );
        if ( serverSuspicions == null )
        {
            serverSuspicions = new HashSet<org.neo4j.cluster.InstanceId>();
            nodeSuspicions.put( uri, serverSuspicions );
        }
        return serverSuspicions;
    }

    @Override
    public Iterable<org.neo4j.cluster.InstanceId> getOtherInstances()
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
}
