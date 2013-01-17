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
package org.neo4j.cluster.protocol.cluster;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

/**
 * Context for cluster API state machine
 *
 * @see ClusterState
 */
public class ClusterContext
{
    URI me;
    Iterable<ClusterListener> listeners = Listeners.newListeners();
    ProposerContext proposerContext;
    LearnerContext learnerContext;
    public ClusterConfiguration configuration;
    public final Timeouts timeouts;
    private Executor executor;
    private Logging logging;
    private List<URI> discoveredInstances = new ArrayList<URI>(  );
    private String joiningClusterName;
    private Iterable<URI> joiningInstances;

    public ClusterContext( ProposerContext proposerContext,
                           LearnerContext learnerContext,
                           ClusterConfiguration configuration,
                           Timeouts timeouts, Executor executor,
                           Logging logging
    )
    {
        this.proposerContext = proposerContext;
        this.learnerContext = learnerContext;
        this.configuration = configuration;
        this.timeouts = timeouts;
        this.executor = executor;
        this.logging = logging;
    }

    // Cluster API
    public void addClusterListener( ClusterListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    public void removeClusterListener( ClusterListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    // Implementation
    public void created( String name )
    {
        configuration = new ClusterConfiguration( name, Collections.singleton( me ) );
        Listeners.notifyListeners( listeners, executor, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.enteredCluster( configuration );
            }
        } );
    }

    public void joining(String name, Iterable<URI> instanceList)
    {
        joiningClusterName = name;
        joiningInstances = instanceList;
        discoveredInstances.clear();
    }

    public void acquiredConfiguration( final List<URI> memberList, final Map<String, URI> roles )
    {
        configuration.setMembers( memberList );
        configuration.setRoles( roles );
    }

    public void joined()
    {
        configuration.joined( me );
        Listeners.notifyListeners( listeners, executor, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.enteredCluster( configuration );
            }
        } );
    }

    public void left()
    {
        timeouts.cancelAllTimeouts();
        configuration.left();
        Listeners.notifyListeners( listeners, executor, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.leftCluster();
            }
        } );
    }

    public void joined( final URI node )
    {
        if ( configuration.getMembers().contains( node ) )
        {
            return; // Already know that this node is in - ignore
        }

        configuration.joined( node );

        if ( configuration.getMembers().contains( me ) )
        {
            // Make sure this node is in cluster before notifying of others joining and leaving
            Listeners.notifyListeners( listeners, executor, new Listeners.Notification<ClusterListener>()
            {
                @Override
                public void notify( ClusterListener listener )
                {
                    listener.joinedCluster( node );
                }
            } );
        }
        else
        {
            // This typically happens in situations when several nodes join at once, and the ordering
            // of join messages is a little out of whack.
        }
    }

    public void left( final URI node )
    {
        configuration.left( node );
        Listeners.notifyListeners( listeners, executor, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.leftCluster( node );
            }
        } );
    }

    public void elected( final String name, final URI node )
    {
        if ( !node.equals( configuration.getElected( name ) ) )
        {
            configuration.elected( name, node );
            Listeners.notifyListeners( listeners, executor, new Listeners.Notification<ClusterListener>()
            {
                @Override
                public void notify( ClusterListener listener )
                {
                    listener.elected( name, node );
                }
            } );
        }
    }

    public synchronized void setMe( URI me )
    {
        this.me = me;
    }

    public URI getMe()
    {
        return me;
    }

    public ClusterConfiguration getConfiguration()
    {
        return configuration;
    }

    public synchronized boolean isMe( URI server )
    {
        return me.equals( server );
    }

    public boolean isElectedAs( String roleName )
    {
        return me.equals( configuration.getElected( roleName ) );
    }

    public boolean isInCluster()
    {
        return !configuration.getMembers().isEmpty();
    }

    public Iterable<URI> getJoiningInstances()
    {
        return joiningInstances;
    }

    public List<URI> getDiscoveredInstances()
    {
        return discoveredInstances;
    }

    public StringLogger getLogger( Class loggingClass )
    {
        return logging.getLogger( loggingClass );
    }

    @Override
    public String toString()
    {
        return "Me: " + me + " Config:" + configuration;
    }
}
