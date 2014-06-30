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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.helpers.Predicates.in;
import static org.neo4j.helpers.Predicates.not;
import static org.neo4j.helpers.Uris.parameter;

class ClusterContextImpl
    extends AbstractContextImpl
    implements ClusterContext
{
    // ClusterContext
    private Iterable<ClusterListener> clusterListeners = Listeners.newListeners();
    private final List<ClusterMessage.ConfigurationRequestState> discoveredInstances = new ArrayList<ClusterMessage
                .ConfigurationRequestState>();
    private Iterable<URI> joiningInstances;
    private ClusterMessage.ConfigurationResponseState joinDeniedConfigurationResponseState;
    private final Map<InstanceId, URI> currentlyJoiningInstances =
            new HashMap<InstanceId, URI>();


    private final Executor executor;
    private final ObjectOutputStreamFactory objectOutputStreamFactory;
    private final ObjectInputStreamFactory objectInputStreamFactory;

    private final LearnerContext learnerContext;
    private final HeartbeatContext heartbeatContext;

    private long electorVersion;
    private InstanceId lastElector;

    ClusterContextImpl( InstanceId me, CommonContextState commonState, Logging logging,
                        Timeouts timeouts, Executor executor,
                        ObjectOutputStreamFactory objectOutputStreamFactory,
                        ObjectInputStreamFactory objectInputStreamFactory,
                        LearnerContext learnerContext, HeartbeatContext heartbeatContext )
    {
        super( me, commonState, logging, timeouts );
        this.executor = executor;
        this.objectOutputStreamFactory = objectOutputStreamFactory;
        this.objectInputStreamFactory = objectInputStreamFactory;
        this.learnerContext = learnerContext;
        this.heartbeatContext = heartbeatContext;
    }

    public long getLastElectorVersion()
    {
        return electorVersion;
    }

    @Override
    public void setLastElectorVersion( long lastElectorVersion )
    {
        this.electorVersion = lastElectorVersion;
    }

    public org.neo4j.cluster.InstanceId getLastElector()
    {
        return lastElector;
    }

    @Override
    public void setLastElector( org.neo4j.cluster.InstanceId lastElector )
    {
        this.lastElector = lastElector;
    }

    // Cluster API
    @Override
    public void addClusterListener( ClusterListener listener )
    {
        clusterListeners = Listeners.addListener( listener, clusterListeners );
    }

    @Override
    public void removeClusterListener( ClusterListener listener )
    {
        clusterListeners = Listeners.removeListener( listener, clusterListeners );
    }

    // Implementation
    @Override
    public void created( String name )
    {
        commonState.setConfiguration( new ClusterConfiguration( name, logging.getMessagesLog( ClusterConfiguration.class ),
                Collections.singleton( commonState.boundAt() ) ));
        joined();
    }

    @Override
    public void joining( String name, Iterable<URI> instanceList )
    {
        joiningInstances = instanceList;
        discoveredInstances.clear();
        joinDeniedConfigurationResponseState = null;
    }

    @Override
    public void acquiredConfiguration( final Map<org.neo4j.cluster.InstanceId, URI> memberList, final Map<String,
            org.neo4j.cluster.InstanceId> roles )
    {
        commonState.configuration().setMembers( memberList );
        commonState.configuration().setRoles( roles );
    }

    @Override
    public void joined()
    {
        commonState.configuration().joined( me, commonState.boundAt() );
        Listeners.notifyListeners( clusterListeners, executor, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.enteredCluster( commonState.configuration() );
            }
        } );
    }

    @Override
    public void left()
    {
        timeouts.cancelAllTimeouts();
        commonState.configuration().left();
        Listeners.notifyListeners( clusterListeners, executor, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.leftCluster();
            }
        } );
    }

    @Override
    public void joined( final org.neo4j.cluster.InstanceId instanceId, final URI atURI )
    {
        commonState.configuration().joined( instanceId, atURI );

        if ( commonState.configuration().getMembers().containsKey( me ) )
        {
            // Make sure this node is in cluster before notifying of others joining and leaving
            Listeners.notifyListeners( clusterListeners, executor, new Listeners.Notification<ClusterListener>()
            {
                @Override
                public void notify( ClusterListener listener )
                {
                    listener.joinedCluster( instanceId, atURI );
                }
            } );
        }
        // else:
        //   This typically happens in situations when several nodes join at once, and the ordering
        //   of join messages is a little out of whack.

        currentlyJoiningInstances.remove( instanceId );
    }

    @Override
    public void left( final InstanceId node )
    {
        final URI member = commonState.configuration().getUriForId( node );
        commonState.configuration().left( node );
        Listeners.notifyListeners( clusterListeners, executor, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.leftCluster( node, member );
            }
        } );
    }

    @Override
    public void elected( final String roleName, final InstanceId instanceId )
    {
        elected( roleName, instanceId, null, -1 );
    }

    @Override
    public void elected( final String roleName, final InstanceId instanceId,
                         org.neo4j.cluster.InstanceId electorId, long version )
    {
        if ( electorId != null )
        {
            if ( electorId.equals( getMyId() ) )
            {
                getLogger( getClass() ).debug( "I elected instance " + instanceId + " for role "
                        + roleName + " at version " + version );
                if ( version < electorVersion )
                {
                    return;
                }
            }
            else if ( electorId.equals( lastElector ) && (version < electorVersion && version > 1) )
            {
                getLogger( getClass() ).warn( "Election result for role " + roleName +
                        " received from elector instance " + electorId + " with version " + version +
                        ". I had version " + electorVersion + " for elector " + lastElector );
                return;
            }
            else
            {
                getLogger( getClass() ).debug( "Setting elector to " + electorId + " and its version to " + version );
            }

            this.electorVersion = version;
            this.lastElector = electorId;
        }
        commonState.configuration().elected( roleName, instanceId );
        Listeners.notifyListeners( clusterListeners, executor, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.elected( roleName, instanceId, commonState.configuration().getUriForId( instanceId ) );
            }
        } );
    }

    @Override
    public void unelected( final String roleName, final org.neo4j.cluster.InstanceId instanceId )
    {
        unelected( roleName, instanceId, null, -1 );
    }

    @Override
    public void unelected( final String roleName, final org.neo4j.cluster.InstanceId instanceId,
                           org.neo4j.cluster.InstanceId electorId, long version )
    {
        commonState.configuration().unelected( roleName );
        Listeners.notifyListeners( clusterListeners, executor, new Listeners.Notification<ClusterListener>()
        {
            @Override
            public void notify( ClusterListener listener )
            {
                listener.unelected( roleName, instanceId, commonState.configuration().getUriForId( instanceId ) );
            }
        } );
    }

    @Override
    public ClusterConfiguration getConfiguration()
    {
        return commonState.configuration();
    }

    @Override
    public boolean isElectedAs( String roleName )
    {
        return me.equals( commonState.configuration().getElected( roleName ) );
    }

    @Override
    public boolean isInCluster()
    {
        return Iterables.count( commonState.configuration().getMemberURIs() ) != 0;
    }

    @Override
    public Iterable<URI> getJoiningInstances()
    {
        return joiningInstances;
    }

    @Override
    public ObjectOutputStreamFactory getObjectOutputStreamFactory()
    {
        return objectOutputStreamFactory;
    }

    @Override
    public ObjectInputStreamFactory getObjectInputStreamFactory()
    {
        return objectInputStreamFactory;
    }

    @Override
    public List<ClusterMessage.ConfigurationRequestState> getDiscoveredInstances()
    {
        return discoveredInstances;
    }

    @Override
    public String toString()
    {
        return "Me: " + me + " Bound at: " + commonState.boundAt() + " Config:" + commonState.configuration();
    }

    @Override
    public void setBoundAt( URI boundAt )
    {
        commonState.setBoundAt( me, boundAt );
    }

    @Override
    public void joinDenied( ClusterMessage.ConfigurationResponseState configurationResponseState )
    {
        if ( configurationResponseState == null )
        {
            throw new IllegalArgumentException( "Join denied configuration response state was null" );
        }
        this.joinDeniedConfigurationResponseState = configurationResponseState;
    }

    @Override
    public boolean hasJoinBeenDenied()
    {
        return joinDeniedConfigurationResponseState != null;
    }

    @Override
    public ClusterMessage.ConfigurationResponseState getJoinDeniedConfigurationResponseState()
    {
        if ( !hasJoinBeenDenied() )
        {
            throw new IllegalStateException( "Join has not been denied" );
        }
        return joinDeniedConfigurationResponseState;
    }

    @Override
    public Iterable<org.neo4j.cluster.InstanceId> getOtherInstances()
    {
        return Iterables.filter( not( in( me ) ), commonState.configuration().getMemberIds() );
    }

    /** Used to ensure that no other instance is trying to join with the same id from a different machine */
    @Override
    public boolean isInstanceJoiningFromDifferentUri( org.neo4j.cluster.InstanceId joiningId, URI uri )
    {
        return currentlyJoiningInstances.containsKey( joiningId )
                && !currentlyJoiningInstances.get( joiningId ).equals(uri);
    }

    @Override
    public void instanceIsJoining( org.neo4j.cluster.InstanceId joiningId, URI uri )
    {
        currentlyJoiningInstances.put( joiningId, uri );
    }

    @Override
    public String myName()
    {
        String name = parameter( "name" ).apply( commonState.boundAt() );
        if ( name != null )
        {
            return name;
        }
        else
        {
            return me.toString();
        }
    }

    @Override
    public void discoveredLastReceivedInstanceId( long id )
    {
        learnerContext.setLastDeliveredInstanceId( id );
        learnerContext.learnedInstanceId( id );
        learnerContext.setNextInstanceId( id + 1);
    }

    @Override
    public boolean isCurrentlyAlive( org.neo4j.cluster.InstanceId joiningId )
    {
        return !heartbeatContext.getFailed().contains( joiningId );
    }

    @Override
    public long getLastDeliveredInstanceId()
    {
        return learnerContext.getLastDeliveredInstanceId();
    }
}
