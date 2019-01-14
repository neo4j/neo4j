/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import org.neo4j.cluster.ClusterSettings;
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
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

import static org.neo4j.function.Predicates.in;
import static org.neo4j.helpers.Uris.parameter;
import static org.neo4j.helpers.collection.Iterables.asList;

/**
 * Context for {@link ClusterState} state machine.
 */
class ClusterContextImpl
        extends AbstractContextImpl
        implements ClusterContext
{
    private static final String DISCOVERY_HEADER_SEPARATOR = ",";

    // ClusterContext
    private final Listeners<ClusterListener> clusterListeners = new Listeners<>();
    /*
     * Holds instances that we have contacted and which have contacted us. This is achieved by filtering on
     * receipt via contactingInstances and the DISCOVERED header.
     * Cleared at the end of each discovery round.
     */
    private final List<ClusterMessage.ConfigurationRequestState> discoveredInstances = new LinkedList<>();

    /*
     * Holds instances that have contacted us, along with a set of the instances they have in turn been contacted
     * from. This is used to determine which instances that have contacted us have received messages from us and thus
     * are in our initial_hosts. This is used to filter who goes in discoveredInstances.
     * This map is also used to create the DISCOVERED header, which is basically the keyset in string form.
     */
    private final Map<InstanceId, Set<InstanceId>> contactingInstances = new HashMap<>();

    private Iterable<URI> joiningInstances;
    private ClusterMessage.ConfigurationResponseState joinDeniedConfigurationResponseState;
    private final Map<InstanceId, URI> currentlyJoiningInstances = new HashMap<>();

    private final Executor executor;
    private final ObjectOutputStreamFactory objectOutputStreamFactory;
    private final ObjectInputStreamFactory objectInputStreamFactory;

    private final LearnerContext learnerContext;
    private final HeartbeatContext heartbeatContext;
    private final Config config;

    private long electorVersion;
    private InstanceId lastElector;

    ClusterContextImpl( InstanceId me, CommonContextState commonState, LogProvider logging,
                        Timeouts timeouts, Executor executor,
                        ObjectOutputStreamFactory objectOutputStreamFactory,
                        ObjectInputStreamFactory objectInputStreamFactory,
                        LearnerContext learnerContext, HeartbeatContext heartbeatContext,
                        Config config )
    {
        super( me, commonState, logging, timeouts );
        this.executor = executor;
        this.objectOutputStreamFactory = objectOutputStreamFactory;
        this.objectInputStreamFactory = objectInputStreamFactory;
        this.learnerContext = learnerContext;
        this.heartbeatContext = heartbeatContext;
        this.config = config;
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
                        HeartbeatContext heartbeatContext, Config config )
    {
        super( me, commonState, logging, timeouts );
        this.joiningInstances = joiningInstances;
        this.joinDeniedConfigurationResponseState = joinDeniedConfigurationResponseState;
        this.executor = executor;
        this.objectOutputStreamFactory = objectOutputStreamFactory;
        this.objectInputStreamFactory = objectInputStreamFactory;
        this.learnerContext = learnerContext;
        this.heartbeatContext = heartbeatContext;
        this.config = config;
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
    public boolean shouldFilterContactingInstances()
    {
        return config.get( ClusterSettings.strict_initial_hosts );
    }

    @Override
    public Set<InstanceId> getFailedInstances()
    {
        return heartbeatContext.getFailed();
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
        clusterListeners.add( listener );
    }

    @Override
    public void removeClusterListener( ClusterListener listener )
    {
        clusterListeners.remove( listener );
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
    public void acquiredConfiguration( final Map<InstanceId, URI> memberList, final Map<String, InstanceId> roles,
                                       final Set<InstanceId> failedInstances )
    {
        commonState.configuration().setMembers( memberList );
        commonState.configuration().setRoles( roles );
        for ( InstanceId failedInstance : failedInstances )
        {
            if ( !failedInstance.equals( me  ) )
            {
                logProvider.getLog( ClusterContextImpl.class ).debug( "Adding instance " + failedInstance + " as failed from the start" );
                heartbeatContext.failed( failedInstance );
            }
        }
    }

    @Override
    public void joined()
    {
        commonState.configuration().joined( me, commonState.boundAt() );
        clusterListeners.notify( executor,
                listener -> listener.enteredCluster( commonState.configuration() ) );
    }

    @Override
    public void left()
    {
        timeouts.cancelAllTimeouts();
        commonState.configuration().left();
        clusterListeners.notify( executor, ClusterListener::leftCluster );
    }

    @Override
    public void joined( InstanceId instanceId, URI atURI )
    {
        commonState.configuration().joined( instanceId, atURI );

        if ( commonState.configuration().getMembers().containsKey( me ) )
        {
            // Make sure this node is in cluster before notifying of others joining and leaving
            clusterListeners.notify( executor, listener -> listener.joinedCluster( instanceId, atURI ) );
        }
        // else:
        //   This typically happens in situations when several nodes join at once, and the ordering
        //   of join messages is a little out of whack.

        currentlyJoiningInstances.remove( instanceId );
        invalidateElectorIfNecessary( instanceId );
    }

    @Override
    public void left( InstanceId node )
    {
        final URI member = commonState.configuration().getUriForId( node );
        commonState.configuration().left( node );
        invalidateElectorIfNecessary( node );
        clusterListeners.notify( executor, listener -> listener.leftCluster( node, member ) );
    }

    @Override
    public void elected( final String roleName, final InstanceId instanceId )
    {
        elected( roleName, instanceId, InstanceId.NONE, NO_ELECTOR_VERSION );
    }

    @Override
    public void elected( String roleName, InstanceId instanceId, InstanceId electorId, long version )
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
        clusterListeners.notify( executor, listener ->
                listener.elected( roleName, instanceId, commonState.configuration().getUriForId( instanceId ) ) );
    }

    @Override
    public void unelected( final String roleName, final org.neo4j.cluster.InstanceId instanceId )
    {
        unelected( roleName, instanceId, InstanceId.NONE, NO_ELECTOR_VERSION );
    }

    @Override
    public void unelected( String roleName, org.neo4j.cluster.InstanceId instanceId,
                           org.neo4j.cluster.InstanceId electorId, long version )
    {
        commonState.configuration().unelected( roleName );
        clusterListeners.notify( executor, listener ->
                listener.unelected( roleName, instanceId, commonState.configuration().getUriForId( instanceId ) ) );
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
    public boolean haveWeContactedInstance( ClusterMessage.ConfigurationRequestState configurationRequested )
    {
        return contactingInstances.containsKey( configurationRequested.getJoiningId() ) && contactingInstances.get(
                configurationRequested.getJoiningId() ).contains( getMyId() );
    }

    @Override
    public void addContactingInstance( ClusterMessage.ConfigurationRequestState instance, String discoveryHeader )
    {
        Set<InstanceId> contactsOfRemote = contactingInstances.computeIfAbsent( instance.getJoiningId(), k -> new
                HashSet<>() );
        // Duplicates of previous calls will be ignored by virtue of this being a set
        contactsOfRemote.addAll( parseDiscoveryHeader( discoveryHeader ) );
    }

    @Override
    public String generateDiscoveryHeader()
    {
        /*
         * Maps the keyset of contacting instances from InstanceId to strings, collects them in a Set and joins them
         * in a string with the appropriate separator
         */
        return String.join( DISCOVERY_HEADER_SEPARATOR,
                contactingInstances.keySet().stream().map( InstanceId::toString ).collect( Collectors.toSet() ) );
    }

    private Set<InstanceId> parseDiscoveryHeader( String discoveryHeader )
    {
        String[] instanceIds = discoveryHeader.split( DISCOVERY_HEADER_SEPARATOR );
        Set<InstanceId> result = new HashSet<>();
        for ( String instanceId : instanceIds )
        {
            try
            {
                result.add( new InstanceId( Integer.parseInt( instanceId.trim() ) ) );
            }
            catch ( NumberFormatException e )
            {
                /*
                 * This will happen if the message did not contain a DISCOVERY header. There are two reasons for this.
                 * One, the first configurationRequest going out from every instance does have the header but
                 * it is empty, since it's sent before any configurationRequests are processed.
                 * The other is practically the backwards compatibility code for versions which do not carry this header.
                 *
                 * Since the header will be empty (default value for it is empty string), the split above will create
                 * an array with a single empty string. This fails the integer parse.
                 */
                getLog( getClass() ).debug( "Could not parse discovery header for contacted instances, its value was " +
                        discoveryHeader );
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        return "Me: " + me + " Bound at: " + commonState.boundAt() + " Config:" + commonState.configuration() +
                " Current state: " + commonState;
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
        return Iterables.filter( in( me ).negate(), commonState.configuration().getMemberIds() );
    }

    /** Used to ensure that no other instance is trying to join with the same id from a different machine */
    @Override
    public boolean isInstanceJoiningFromDifferentUri( org.neo4j.cluster.InstanceId joiningId, URI uri )
    {
        return currentlyJoiningInstances.containsKey( joiningId )
                && !currentlyJoiningInstances.get( joiningId ).equals( uri );
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
        learnerContext.setNextInstanceId( id + 1 );
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
                joiningInstances == null ? null : new ArrayList<>( asList( joiningInstances ) ),
                joinDeniedConfigurationResponseState == null ? null : joinDeniedConfigurationResponseState.snapshot(),
                executor, objectOutputStreamFactory, objectInputStreamFactory, snapshotLearnerContext,
                snapshotHeartbeatContext, config );
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
        if ( discoveredInstances != null ? !discoveredInstances.equals( that.discoveredInstances ) :
                that.discoveredInstances != null )
        {
            return false;
        }
        if ( heartbeatContext != null ? !heartbeatContext.equals( that.heartbeatContext ) : that.heartbeatContext !=
                null )
        {
            return false;
        }
        if ( joinDeniedConfigurationResponseState != null ? !joinDeniedConfigurationResponseState.equals(
                that.joinDeniedConfigurationResponseState ) : that.joinDeniedConfigurationResponseState != null )
        {
            return false;
        }
        if ( joiningInstances != null ? !joiningInstances.equals( that.joiningInstances ) : that.joiningInstances !=
                null )
        {
            return false;
        }
        return learnerContext != null ? learnerContext.equals( that.learnerContext ) : that.learnerContext == null;
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
