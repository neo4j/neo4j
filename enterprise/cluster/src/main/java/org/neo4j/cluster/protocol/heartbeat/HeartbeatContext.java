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
package org.neo4j.cluster.protocol.heartbeat;

import static org.neo4j.cluster.com.message.Message.timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Context used by the {@link HeartbeatState} state machine.
 */
public class HeartbeatContext
{
    private ClusterContext clusterContext;
    private LearnerContext learnerContext;
    private Executor executor;
    private final StringLogger logger;
    Set<InstanceId> failed = new HashSet<InstanceId>();

    Map<InstanceId, Set<InstanceId>> nodeSuspicions = new HashMap<InstanceId, Set<InstanceId>>();

    Iterable<HeartbeatListener> listeners = Listeners.newListeners();

    public HeartbeatContext( ClusterContext clusterContext, LearnerContext learnerContext, Executor executor )
    {
        this.clusterContext = clusterContext;
        this.learnerContext = learnerContext;
        this.executor = executor;
        this.logger = clusterContext.getLogger( getClass() );
    }

    public void started()
    {
        failed.clear();
    }

    /**
     * @return True iff the node was suspected
     */
    public boolean alive( final InstanceId node )
    {
        Set<InstanceId> serverSuspicions = getSuspicionsFor( clusterContext.getMyId() );
        boolean suspected = serverSuspicions.remove( node );

        if ( !isFailed( node ) && failed.remove( node ) )
        {
            logger.info( "Notifying listeners that instance " + node + " is alive" );
            Listeners.notifyListeners( listeners, new Listeners.Notification<HeartbeatListener>()
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

    public void suspect( final InstanceId node )
    {
        logger.info( "Suspecting " + node );
        Set<InstanceId> serverSuspicions = getSuspicionsFor( clusterContext.getMyId() );
        serverSuspicions.add( node );

        if ( isFailed( node ) && !failed.contains( node ) )
        {
            logger.info( "Notifying listeners that node " + node + " is failed" );
            failed.add( node );
            Listeners.notifyListeners( listeners, executor, new Listeners.Notification<HeartbeatListener>()
            {
                @Override
                public void notify( HeartbeatListener listener )
                {
                    listener.failed( node );
                }
            } );
        }
    }

    public void suspicions( InstanceId from, Set<InstanceId> suspicions )
    {
        Set<InstanceId> serverSuspicions = getSuspicionsFor( from );
        serverSuspicions.clear();
        serverSuspicions.addAll( suspicions );

        for ( final InstanceId node : suspicions )
        {
            if ( isFailed( node ) && !failed.contains( node ) )
            {
                failed.add( node );
                Listeners.notifyListeners( listeners, executor, new Listeners.Notification<HeartbeatListener>()
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

    public Set<InstanceId> getFailed()
    {
        return failed;
    }

    public Iterable<InstanceId> getAlive()
    {
        return Iterables.filter( new Predicate<InstanceId>()
        {
            @Override
            public boolean accept( InstanceId item )
            {
                return !isFailed( item );
            }
        }, clusterContext.getConfiguration().getMemberIds() );
    }

    public ClusterContext getClusterContext()
    {
        return clusterContext;
    }

    public LearnerContext getLearnerContext()
    {
        return learnerContext;
    }

    public void addHeartbeatListener( HeartbeatListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    public void removeHeartbeatListener( HeartbeatListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    public void startHeartbeatTimers( Message<?> message )
    {
        // Start timers for sending and receiving heartbeats
        for ( InstanceId server : clusterContext.getConfiguration().getMemberIds() )
        {
            if ( !clusterContext.isMe( server ) )
            {
                clusterContext.timeouts.setTimeout( HeartbeatMessage.i_am_alive + "-" + server,
                        timeout( HeartbeatMessage.timed_out, message, server ) );
                clusterContext.timeouts.setTimeout( HeartbeatMessage.sendHeartbeat + "-" + server,
                        timeout( HeartbeatMessage.sendHeartbeat, message, server ) );
            }
        }
    }

    public void serverLeftCluster( InstanceId node )
    {
        failed.remove( node );
        for ( Set<InstanceId> uris : nodeSuspicions.values() )
        {
            uris.remove( node );
        }
    }

    public boolean isFailed( InstanceId node )
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
                ( clusterContext.getConfiguration().getMembers().size() - failed.size() - adjust ) / 2;
    }

    public List<InstanceId> getSuspicionsOf( InstanceId server )
    {
        List<InstanceId> suspicions = new ArrayList<InstanceId>();
        for ( Map.Entry<InstanceId, Set<InstanceId>> uriSetEntry : nodeSuspicions.entrySet() )
        {
            if ( !failed.contains( uriSetEntry.getKey() )
                    && uriSetEntry.getValue().contains( server ) )
            {
                suspicions.add( uriSetEntry.getKey() );
            }
        }

        return suspicions;
    }

    public Set<InstanceId> getSuspicionsFor( InstanceId uri )
    {
        Set<InstanceId> serverSuspicions = nodeSuspicions.get( uri );
        if ( serverSuspicions == null )
        {
            serverSuspicions = new HashSet<InstanceId>();
            nodeSuspicions.put( uri, serverSuspicions );
        }
        return serverSuspicions;
    }
}
