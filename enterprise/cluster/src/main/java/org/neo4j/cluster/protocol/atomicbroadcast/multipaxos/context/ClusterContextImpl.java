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
import org.neo4j.cluster.protocol.cluster.ClusterState;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.Predicates.in;
import static org.neo4j.helpers.Predicates.not;
import static org.neo4j.helpers.Uris.parameter;
import static org.neo4j.helpers.collection.Iterables.toList;

/**
 * Context for {@link ClusterState} state machine.
 */
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

    ClusterContextImpl( InstanceId me, CommonContextState commonState, LogProvider logging,
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
        heartbeatContext.addHeartbeatListener(

                /*
                 * Here for invalidating the elector if it fails, so when it comes back, if no elections
                 * happened in the meantime it can resume sending election results
                 */
                new HeartbeatListener.Adapter()
                {
                    @Override
                    public void failed( InstanceId server )
                    {
                        invalidateElectorIfNecessary( server );
                    }
                }
        );
    }

    private void invalidateElectorIfNecessary( InstanceId server )
    {
        if ( server.equals( lastElector ) )
        {
            lastElector = InstanceId.NONE;
            electorVersion = NO_ELECTOR_VERSION;
        }
    }

    private ClusterContextImpl( InstanceId me, CommonContextState commonState, LogProvider logging, Timeouts timeouts,
                        Iterable<URI> joiningInstances, ClusterMessage.ConfigurationResponseState
            joinDeniedConfigurationResponseState, Executor executor,
                        ObjectOutputStreamFactory objectOutputStreamFactory,
                        ObjectInputStreamFactory objectInputStreamFactory, LearnerContext learnerContext,
                        HeartbeatContext heartbeatContext )
    {
        super( me, commonState, logging, timeouts );
        this.joiningInstances = joiningInstances;
        this.joinDeniedConfigurationResponseState = joinDeniedConfigurationResponseState;
        this.executor = executor;
        this.objectOutputStreamFactory = objectOutputStreamFactory;
        this.objectInputStreamFactory = objectInputStreamFactory;
        this.learnerContext = learnerContext;
        this.heartbeatContext = heartbeatContext;
    }

    // Cluster API
    @Override
    public long getLastElectorVersion()
    {
        return electorVersion;
    }

    @Override
    public void setLastElectorVersion( long lastElectorVersion )
    {
        this.electorVersion = lastElectorVersion;
    }

    @Override
    public InstanceId getLastElector()
    {
        return lastElector;
    }

    @Override
    public void setLastElector( InstanceId lastElector )
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
        commonState.setConfiguration( new  ClusterConfiguration( name, logProvider,
                Collections.singleton( commonState.boundAt() ) ) );
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
    public void acquiredConfiguration( final Map<InstanceId, URI> memberList, final Map<String, InstanceId> roles )
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
    public void joined( final InstanceId instanceId, final URI atURI )
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
        invalidateElectorIfNecessary( instanceId );
    }

    @Override
    public void left( final InstanceId node )
    {
        final URI member = commonState.configuration().getUriForId( node );
        commonState.configuration().left( node );
        invalidateElectorIfNecessary( node );
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
        elected( roleName, instanceId, InstanceId.NONE, NO_ELECTOR_VERSION );
    }

    @Override
    public void elected( final String roleName, final InstanceId instanceId, InstanceId electorId, long version )
    {
        if ( electorId != null )
        {
            if ( electorId.equals( getMyId() ) )
            {
                getLog( getClass() ).debug( "I elected instance " + instanceId + " for role "
                        + roleName + " at version " + version );
                if ( version < electorVersion )
                {
                    return;
                }
            }
            else if ( electorId.equals( lastElector ) && ( version < electorVersion && version > 1 ) )
            {
                getLog( getClass() ).warn( "Election result for role " + roleName +
                        " received from elector instance " + electorId + " with version " + version +
                        ". I had version " + electorVersion + " for elector " + lastElector );
                return;
            }
            else
            {
                getLog( getClass() ).debug( "Setting elector to " + electorId + " and its version to " + version );
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
        unelected( roleName, instanceId, InstanceId.NONE, NO_ELECTOR_VERSION );
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
    public boolean isCurrentlyAlive( InstanceId joiningId )
    {
        return !heartbeatContext.getFailed().contains( joiningId );
    }

    @Override
    public long getLastDeliveredInstanceId()
    {
        return learnerContext.getLastDeliveredInstanceId();
    }

    public ClusterContextImpl snapshot( CommonContextState commonStateSnapshot, LogProvider logging, Timeouts timeouts,
                                        Executor executor, ObjectOutputStreamFactory objectOutputStreamFactory,
                                        ObjectInputStreamFactory objectInputStreamFactory,
                                        LearnerContextImpl snapshotLearnerContext,
                                        HeartbeatContextImpl snapshotHeartbeatContext )
    {
        return new ClusterContextImpl( me, commonStateSnapshot, logging, timeouts,
                joiningInstances == null ? null : new ArrayList<>(toList(joiningInstances)),
                joinDeniedConfigurationResponseState == null ? null : joinDeniedConfigurationResponseState.snapshot(),
                executor, objectOutputStreamFactory, objectInputStreamFactory, snapshotLearnerContext,
                snapshotHeartbeatContext );
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

        ClusterContextImpl that = (ClusterContextImpl) o;

        if ( currentlyJoiningInstances != null ? !currentlyJoiningInstances.equals( that.currentlyJoiningInstances )
                : that.currentlyJoiningInstances != null )
        {
            return false;
        }
        if ( discoveredInstances != null ? !discoveredInstances.equals( that.discoveredInstances ) : that
                .discoveredInstances != null )
        {
            return false;
        }
        if ( heartbeatContext != null ? !heartbeatContext.equals( that.heartbeatContext ) : that.heartbeatContext !=
                null )
        {
            return false;
        }
        if ( joinDeniedConfigurationResponseState != null ? !joinDeniedConfigurationResponseState.equals( that
                .joinDeniedConfigurationResponseState ) : that.joinDeniedConfigurationResponseState != null )
        {
            return false;
        }
        if ( joiningInstances != null ? !joiningInstances.equals( that.joiningInstances ) : that.joiningInstances !=
                null )
        {
            return false;
        }
        if ( learnerContext != null ? !learnerContext.equals( that.learnerContext ) : that.learnerContext != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = 0;
        result = 31 * result + (discoveredInstances != null ? discoveredInstances.hashCode() : 0);
        result = 31 * result + (joiningInstances != null ? joiningInstances.hashCode() : 0);
        result = 31 * result + (joinDeniedConfigurationResponseState != null ? joinDeniedConfigurationResponseState
                .hashCode() : 0);
        result = 31 * result + (currentlyJoiningInstances != null ? currentlyJoiningInstances.hashCode() : 0);
        result = 31 * result + (learnerContext != null ? learnerContext.hashCode() : 0);
        result = 31 * result + (heartbeatContext != null ? heartbeatContext.hashCode() : 0);
        return result;
    }
}
