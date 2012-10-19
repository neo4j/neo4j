/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.Specification;
import org.neo4j.helpers.collection.Iterables;

/**
 * Context used by the {@link HeartbeatState} state machine.
 */
public class HeartbeatContext
{
    private ClusterContext clusterContext;
    private LearnerContext learnerContext;
    private Executor executor;
    List<URI> failed = new ArrayList<URI>();

    Map<URI, Set<URI>> nodeSuspicions = new HashMap<URI, Set<URI>>();

    Iterable<HeartbeatListener> listeners = Listeners.newListeners();

    public HeartbeatContext( ClusterContext clusterContext, LearnerContext learnerContext, Executor executor )
    {
        this.clusterContext = clusterContext;
        this.learnerContext = learnerContext;
        this.executor = executor;
    }

    public void started()
    {
        failed.clear();
    }

    public boolean alive( final URI node )
    {
        Set<URI> serverSuspicions = getSuspicionsFor( clusterContext.getMe() );
        boolean suspected = serverSuspicions.remove( node );

        if ( !isFailed( node ) && failed.remove( node ) )
        {
            Listeners.notifyListeners( listeners, executor, new Listeners.Notification<HeartbeatListener>()
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

    public void suspect( final URI node )
    {
        Set<URI> serverSuspicions = getSuspicionsFor( clusterContext.getMe() );
        serverSuspicions.add( node );

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

    public void suspicions( URI from, Set<URI> suspicions )
    {
        Set<URI> serverSuspicions = getSuspicionsFor( from );
        serverSuspicions.clear();
        serverSuspicions.addAll( suspicions );

        for ( final URI node : suspicions )
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

    public List<URI> getFailed()
    {
        return failed;
    }

    public Iterable<URI> getAlive()
    {
        return Iterables.filter( new Specification<URI>()
        {
            @Override
            public boolean satisfiedBy( URI item )
            {
                return !isFailed( item );
            }
        }, clusterContext.getConfiguration().getMembers() );
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
        for ( URI server : clusterContext.getConfiguration().getMembers() )
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

    public void serverLeftCluster( URI node )
    {
        failed.remove( node );
        for ( Set<URI> uris : nodeSuspicions.values() )
        {
            uris.remove( node );
        }
    }

    public boolean isFailed( URI node )
    {
        List<URI> suspicions = getSuspicionsOf( node );

        return suspicions.size() > (clusterContext.getConfiguration().getMembers().size() - failed.size() - 1) / 2;
    }

    public List<URI> getSuspicionsOf( URI uri )
    {
        List<URI> suspicions = new ArrayList<URI>();
        for ( Map.Entry<URI, Set<URI>> uriSetEntry : nodeSuspicions.entrySet() )
        {
            if ( uriSetEntry.getValue().contains( uri ) )
            {
                suspicions.add( uriSetEntry.getKey() );
            }
        }

        return suspicions;
    }

    public Set<URI> getSuspicionsFor( URI uri )
    {
        Set<URI> serverSuspicions = nodeSuspicions.get( uri );
        if ( serverSuspicions == null )
        {
            serverSuspicions = new HashSet<URI>();
            nodeSuspicions.put( uri, serverSuspicions );
        }
        return serverSuspicions;
    }
}
