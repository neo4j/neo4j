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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.cluster.ClusterMessage.ConfigurationResponseState;
import org.neo4j.cluster.protocol.election.ElectionContext;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionRole;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentials;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.cluster.util.Quorums.isQuorum;
import static org.neo4j.helpers.Predicates.in;
import static org.neo4j.helpers.Predicates.not;
import static org.neo4j.helpers.Uris.parameter;
import static org.neo4j.helpers.collection.Iterables.limit;
import static org.neo4j.helpers.collection.Iterables.map;

/**
 * Context that implements all the context interfaces used by the Paxos state machines.
 * <p/>
 * The design here is that all shared external services and stae is in the class itself,
 * whereas any state specific for any particular context is in the individual context implementations.
 */
public class MultiPaxosContext
{
    // Shared state - all context specific state is in the inner classes
    final private AcceptorInstanceStore instanceStore;
    final private Timeouts timeouts;
    final private Executor executor;
    final private Logging logging;

    private final PaxosInstanceStore paxosInstances = new PaxosInstanceStore();
    final private org.neo4j.cluster.InstanceId me;
    private ClusterConfiguration configuration;
    private URI boundAt;
    private long lastKnownLearnedInstanceInCluster = -1;
    private final ObjectInputStreamFactory objectInputStreamFactory;
    private final ObjectOutputStreamFactory objectOutputStreamFactory;
    private long nextInstanceId = 0;

    private final ClusterContext clusterContext;
    private final ProposerContext proposerContext;
    private final AcceptorContext acceptorContext;
    private final LearnerContext learnerContext;
    private final HeartbeatContext heartbeatContext;
    private final ElectionContextImpl electionContext;
    private final AtomicBroadcastContextImpl atomicBroadcastContext;

    public MultiPaxosContext( org.neo4j.cluster.InstanceId me,
                              Iterable<ElectionRole> roles,
                              ClusterConfiguration configuration,
                              Executor executor,
                              Logging logging,
                              ObjectInputStreamFactory objectInputStreamFactory,
                              ObjectOutputStreamFactory objectOutputStreamFactory,
                              AcceptorInstanceStore instanceStore,
                              Timeouts timeouts
    )
    {
        this.me = me;
        this.configuration = configuration;
        this.executor = executor;
        this.logging = logging;
        this.objectInputStreamFactory = objectInputStreamFactory;
        this.objectOutputStreamFactory = objectOutputStreamFactory;
        this.instanceStore = instanceStore;
        this.timeouts = timeouts;

        clusterContext = new ClusterContextImpl();
        proposerContext = new ProposerContextImpl();
        acceptorContext = new AcceptorContextImpl();
        learnerContext = new LearnerContextImpl();
        heartbeatContext = new HeartbeatContextImpl();
        electionContext = new ElectionContextImpl( roles );
        atomicBroadcastContext = new AtomicBroadcastContextImpl();
    }

    public ClusterContext getClusterContext()
    {
        return clusterContext;
    }

    public ProposerContext getProposerContext()
    {
        return proposerContext;
    }

    public AcceptorContext getAcceptorContext()
    {
        return acceptorContext;
    }

    public LearnerContext getLearnerContext()
    {
        return learnerContext;
    }

    public HeartbeatContext getHeartbeatContext()
    {
        return heartbeatContext;
    }

    public ElectionContext getElectionContext()
    {
        return electionContext;
    }

    public AtomicBroadcastContextImpl getAtomicBroadcastContext()
    {
        return atomicBroadcastContext;
    }

    private class AbstractContextImpl
            implements TimeoutsContext, LoggingContext, ConfigurationContext
    {
        @Override
        public StringLogger getLogger( Class loggingClass )
        {
            return logging.getMessagesLog( loggingClass );
        }

        // TimeoutsContext
        @Override
        public void setTimeout( Object key, Message<? extends MessageType> timeoutMessage )
        {
            timeouts.setTimeout( key, timeoutMessage );
        }

        @Override
        public void cancelTimeout( Object key )
        {
            timeouts.cancelTimeout( key );
        }

        // ConfigurationContext
        @Override
        public List<URI> getMemberURIs()
        {
            return Iterables.toList( configuration.getMemberURIs() );
        }

        @Override
        public org.neo4j.cluster.InstanceId getMyId()
        {
            return me;
        }

        @Override
        public URI boundAt()
        {
            return boundAt;
        }

        @Override
        public List<URI> getAcceptors()
        {
            // Only use 2f+1 acceptors
            return Iterables.toList( limit( configuration
                    .getAllowedFailures() * 2 + 1, configuration.getMemberURIs() ) );
        }

        @Override
        public Map<org.neo4j.cluster.InstanceId, URI> getMembers()
        {
            return configuration.getMembers();
        }

        @Override
        public org.neo4j.cluster.InstanceId getCoordinator()
        {
            return configuration.getElected( ClusterConfiguration.COORDINATOR );
        }

        @Override
        public URI getUriForId(org.neo4j.cluster.InstanceId node )
        {
            return configuration.getUriForId( node );
        }

        @Override
        public org.neo4j.cluster.InstanceId getIdForUri( URI uri )
        {
            return configuration.getIdForUri( uri );
        }

        @Override
        public synchronized boolean isMe( org.neo4j.cluster.InstanceId server )
        {
            return me.equals( server );
        }
    }

    private class AtomicBroadcastContextImpl
        extends AbstractContextImpl
        implements AtomicBroadcastContext
    {
        private Iterable<AtomicBroadcastListener> listeners = Listeners.newListeners();

        @Override
        public void addAtomicBroadcastListener( AtomicBroadcastListener listener )
        {
            listeners = Listeners.addListener( listener, listeners );
        }

        @Override
        public void removeAtomicBroadcastListener( AtomicBroadcastListener listener )
        {
            listeners = Listeners.removeListener( listener, listeners );
        }

        @Override
        public void receive( final Payload value )
        {
            Listeners.notifyListeners( listeners, executor, new Listeners.Notification<AtomicBroadcastListener>()
            {
                @Override
                public void notify( final AtomicBroadcastListener listener )
                {
                    listener.receive( value );
                }
            } );
        }
    }


    private class ProposerContextImpl
            extends AbstractContextImpl
            implements ProposerContext
    {
        public static final int MAX_CONCURRENT_INSTANCES = 10;

        // ProposerContext
        final Deque<Message> pendingValues = new LinkedList<Message>();
        final Map<InstanceId, Message> bookedInstances = new HashMap<InstanceId, Message>();

        @Override
        public InstanceId newInstanceId()
        {
            // Never propose something lower than last received instance id
            if ( lastKnownLearnedInstanceInCluster >= nextInstanceId )
            {
                nextInstanceId = lastKnownLearnedInstanceInCluster + 1;
            }

            return new InstanceId( nextInstanceId++ );
        }

        @Override
        public void leave()
        {
            pendingValues.clear();
            bookedInstances.clear();
            nextInstanceId = 0;

            paxosInstances.leave();
        }

        @Override
        public void bookInstance( InstanceId instanceId, Message message )
        {
            bookedInstances.put( instanceId, message );
        }

        @Override
        public PaxosInstance getPaxosInstance( InstanceId instanceId )
        {
            return paxosInstances.getPaxosInstance( instanceId );
        }

        @Override
        public void pendingValue( Message message )
        {
            pendingValues.offerFirst( message );
        }

        @Override
        public boolean hasPendingValues()
        {
            return !pendingValues.isEmpty();
        }

        @Override
        public Message popPendingValue()
        {
            return pendingValues.remove();
        }

        @Override
        public boolean canBookInstance()
        {
            return bookedInstances.size() < MAX_CONCURRENT_INSTANCES;
        }

        @Override
        public Message getBookedInstance( InstanceId id )
        {
            return bookedInstances.get( id );
        }

        @Override
        public Message<ProposerMessage> unbookInstance( InstanceId id )
        {
            return bookedInstances.remove( id );
        }

        @Override
        public int nrOfBookedInstances()
        {
            return bookedInstances.size();
        }

        @Override
        public int getMinimumQuorumSize( List<URI> acceptors )
        {
            // n >= 2f+1
            if ( acceptors.size() >= 2 * configuration.getAllowedFailures() + 1 )
            {
                return acceptors.size() - configuration.getAllowedFailures();
            }
            else
            {
                return acceptors.size();
            }
        }

        /**
         * This patches the booked instances that are pending in case the configuration of the cluster changes. This
         * should be called only when we learn a ConfigurationChangeState i.e. when we receive an accepted for
         * such a message. This won't "learn" the message, as in applying it on the cluster configuration, but will
         * just update properly the set of acceptors for pending instances.
         */
        @Override
        public void patchBookedInstances( ClusterMessage.ConfigurationChangeState value )
        {
            if ( value.getJoin() != null )
            {
                for ( InstanceId instanceId : bookedInstances.keySet() )
                {
                    PaxosInstance instance = paxosInstances.getPaxosInstance( instanceId );
                    if ( instance.getAcceptors() != null )
                    {
                        instance.getAcceptors().remove( configuration.getMembers().get( value.getJoin() ) );

                        getLogger( ProposerContext.class ).debug( "For booked instance " + instance +
                                " removed gone member "
                                + configuration.getMembers().get( value.getJoin() ) + " added joining member " +
                                value.getJoinUri() );

                        if ( !instance.getAcceptors().contains( value.getJoinUri() ) )
                        {
                            instance.getAcceptors().add( value.getJoinUri() );
                        }
                    }
                }
            }
            else if ( value.getLeave() != null )
            {
                for ( InstanceId instanceId : bookedInstances.keySet() )
                {
                    PaxosInstance instance = paxosInstances.getPaxosInstance( instanceId );
                    if ( instance.getAcceptors() != null )
                    {
                        getLogger( ProposerContext.class ).debug( "For booked instance " + instance +
                                " removed leaving member "
                                + value.getLeave() + " (at URI " + configuration.getMembers().get( value.getLeave() )
                                + ")" );
                        instance.getAcceptors().remove( configuration.getMembers().get( value.getLeave() ) );
                    }
                }
            }
        }
    }

    private class ClusterContextImpl
            extends AbstractContextImpl
            implements ClusterContext
    {
        // ClusterContext
        Iterable<ClusterListener> clusterListeners = Listeners.newListeners();
        private final List<ClusterMessage.ConfigurationRequestState> discoveredInstances = new ArrayList<ClusterMessage
                .ConfigurationRequestState>();
        private Iterable<URI> joiningInstances;
        private ConfigurationResponseState joinDeniedConfigurationResponseState;
        private final Map<org.neo4j.cluster.InstanceId, URI> currentlyJoiningInstances =
                new HashMap<org.neo4j.cluster.InstanceId, URI>();

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
            configuration = new ClusterConfiguration( name, logging.getMessagesLog( ClusterConfiguration.class ),
                    Collections.singleton( boundAt ) );
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
            configuration.setMembers( memberList );
            configuration.setRoles( roles );
        }

        @Override
        public void joined()
        {
            configuration.joined( me, boundAt );
            Listeners.notifyListeners( clusterListeners, executor, new Listeners.Notification<ClusterListener>()
            {
                @Override
                public void notify( ClusterListener listener )
                {
                    listener.enteredCluster( configuration );
                }
            } );
        }

        @Override
        public void left()
        {
            timeouts.cancelAllTimeouts();
            configuration.left();
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
            configuration.joined( instanceId, atURI );

            if ( configuration.getMembers().containsKey( me ) )
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
        public void left( final org.neo4j.cluster.InstanceId node )
        {
            configuration.left( node );
            Listeners.notifyListeners( clusterListeners, executor, new Listeners.Notification<ClusterListener>()
            {
                @Override
                public void notify( ClusterListener listener )
                {
                    listener.leftCluster( node );
                }
            } );
        }

        @Override
        public void elected( final String roleName, final org.neo4j.cluster.InstanceId instanceId )
        {
            configuration.elected( roleName, instanceId );
            Listeners.notifyListeners( clusterListeners, executor, new Listeners.Notification<ClusterListener>()
            {
                @Override
                public void notify( ClusterListener listener )
                {
                    listener.elected( roleName, instanceId, configuration.getUriForId( instanceId ) );
                }
            } );
        }

        @Override
        public void unelected( final String roleName, final org.neo4j.cluster.InstanceId instanceId )
        {
            configuration.unelected( roleName );
            Listeners.notifyListeners( clusterListeners, executor, new Listeners.Notification<ClusterListener>()
            {
                @Override
                public void notify( ClusterListener listener )
                {
                    listener.unelected( roleName, instanceId, configuration.getUriForId( instanceId ) );
                }
            } );
        }

        @Override
        public ClusterConfiguration getConfiguration()
        {
            return configuration;
        }

        @Override
        public boolean isElectedAs( String roleName )
        {
            return me.equals( configuration.getElected( roleName ) );
        }

        @Override
        public boolean isInCluster()
        {
            return Iterables.count( configuration.getMemberURIs() ) != 0;
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
            return "Me: " + me + " Bound at: " + boundAt + " Config:" + configuration;
        }

        @Override
        public void setBoundAt( URI boundAt )
        {
            MultiPaxosContext.this.boundAt = boundAt;
        }
        
        @Override
        public void joinDenied( ConfigurationResponseState configurationResponseState )
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
        public ConfigurationResponseState getJoinDeniedConfigurationResponseState()
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
            return Iterables.filter( not( in( me ) ), configuration.getMemberIds() );
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
            String name = parameter( "name" ).apply( boundAt );
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

    private class AcceptorContextImpl
            extends AbstractContextImpl
            implements AcceptorContext
    {
        @Override
        public AcceptorInstance getAcceptorInstance( InstanceId instanceId )
        {
            return instanceStore.getAcceptorInstance( instanceId );
        }

        @Override
        public void promise( AcceptorInstance instance, long ballot )
        {
            instanceStore.promise( instance, ballot );
        }

        @Override
        public void accept( AcceptorInstance instance, Object value )
        {
            instanceStore.accept( instance, value );
        }

        @Override
        public void leave()
        {
            instanceStore.clear();
        }
    }

    private class LearnerContextImpl
            extends AbstractContextImpl
            implements LearnerContext
    {
        // LearnerContext
        private long lastDeliveredInstanceId = -1;
        private long lastLearnedInstanceId = -1;

        @Override
        public long getLastDeliveredInstanceId()
        {
            return lastDeliveredInstanceId;
        }

        @Override
        public void setLastDeliveredInstanceId( long lastDeliveredInstanceId )
        {
            this.lastDeliveredInstanceId = lastDeliveredInstanceId;
            instanceStore.lastDelivered( new InstanceId( lastDeliveredInstanceId ) );
        }

        @Override
        public long getLastLearnedInstanceId()
        {
            return lastLearnedInstanceId;
        }

        @Override
        public long getLastKnownLearnedInstanceInCluster()
        {
            return lastKnownLearnedInstanceInCluster;
        }

        @Override
        public void setLastKnownLearnedInstanceInCluster( long lastKnownLearnedInstanceInCluster )
        {
            MultiPaxosContext.this.lastKnownLearnedInstanceInCluster = lastKnownLearnedInstanceInCluster;
        }

        @Override
        public void learnedInstanceId( long instanceId )
        {
            this.lastLearnedInstanceId = Math.max( lastLearnedInstanceId, instanceId );
            if ( lastLearnedInstanceId > lastKnownLearnedInstanceInCluster )
            {
                lastKnownLearnedInstanceInCluster = lastLearnedInstanceId;
            }
        }

        @Override
        public boolean hasDeliveredAllKnownInstances()
        {
            return lastDeliveredInstanceId == lastKnownLearnedInstanceInCluster;
        }

        @Override
        public void leave()
        {
            lastDeliveredInstanceId = -1;
            lastLearnedInstanceId = -1;
            lastKnownLearnedInstanceInCluster = -1;
        }

        @Override
        public PaxosInstance getPaxosInstance( InstanceId instanceId )
        {
            return paxosInstances.getPaxosInstance( instanceId );
        }

        @Override
        public AtomicBroadcastSerializer newSerializer()
        {
            return new AtomicBroadcastSerializer( objectInputStreamFactory, objectOutputStreamFactory );
        }

        @Override
        public Iterable<org.neo4j.cluster.InstanceId> getAlive()
        {
            return heartbeatContext.getAlive();
        }

        @Override
        public void setNextInstanceId( long id )
        {
            nextInstanceId = id;
        }
    }

    private class HeartbeatContextImpl
        extends AbstractContextImpl
        implements HeartbeatContext
    {
        // HeartbeatContext
        Set<org.neo4j.cluster.InstanceId> failed = new HashSet<org.neo4j.cluster.InstanceId>();

        Map<org.neo4j.cluster.InstanceId, Set<org.neo4j.cluster.InstanceId>> nodeSuspicions = new HashMap<org.neo4j
                .cluster.InstanceId, Set<org.neo4j.cluster.InstanceId>>();

        Iterable<HeartbeatListener> heartBeatListeners = Listeners.newListeners();

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
            Set<org.neo4j.cluster.InstanceId> serverSuspicions = getSuspicionsFor( getMyId() );
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
            Set<org.neo4j.cluster.InstanceId> serverSuspicions = getSuspicionsFor( getMyId() );

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
            Set<org.neo4j.cluster.InstanceId> serverSuspicions = getSuspicionsFor( from );

            // Check removals
            Iterator<org.neo4j.cluster.InstanceId> suspicionsIterator = serverSuspicions.iterator();
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
            return Iterables.filter( new Predicate<org.neo4j.cluster.InstanceId>()
            {
                @Override
                public boolean accept( org.neo4j.cluster.InstanceId item )
                {
                    return !isFailed( item );
                }
            }, configuration.getMemberIds() );
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
            List<org.neo4j.cluster.InstanceId> suspicions = getSuspicionsOf( node );

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
                    (configuration.getMembers().size() - failed.size() - adjust) / 2;
        }

        @Override
        public List<org.neo4j.cluster.InstanceId> getSuspicionsOf( org.neo4j.cluster.InstanceId server )
        {
            List<org.neo4j.cluster.InstanceId> suspicions = new ArrayList<org.neo4j.cluster.InstanceId>();
            for ( org.neo4j.cluster.InstanceId member : configuration.getMemberIds() )
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

    private class ElectionContextImpl
        extends AbstractContextImpl
        implements ElectionContext
    {
        private final List<ElectionRole> roles = new ArrayList<ElectionRole>();

        private final Map<String, Election> elections = new HashMap<String, Election>();
        private ElectionCredentialsProvider electionCredentialsProvider;

        public ElectionContextImpl( Iterable<ElectionRole> roles )
        {
            Iterables.addAll( this.roles, roles );
        }

        @Override
        public void setElectionCredentialsProvider( ElectionCredentialsProvider electionCredentialsProvider )
        {
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
            elections.put( role, new Election( new WinnerStrategy()
            {
                @Override
                public org.neo4j.cluster.InstanceId pickWinner( Collection<Vote> voteList )
                {

                    // Remove blank votes
                    List<Vote> filteredVoteList = Iterables.toList( Iterables.filter( new Predicate<Vote>()
                    {
                        @Override
                        public boolean accept( Vote item )
                        {
                            return !( item.getCredentials() instanceof NotElectableElectionCredentials);
                        }
                    }, voteList ) );

                    // Sort based on credentials
                    // The most suited candidate should come out on top
                    Collections.sort( filteredVoteList );
                    Collections.reverse( filteredVoteList );

                    for ( Vote vote : filteredVoteList )
                    {
                        // Don't elect as winner the node we are trying to demote
                        if ( !vote.getSuggestedNode().equals( demoteNode ) )
                        {
                            return vote.getSuggestedNode();
                        }
                    }

                    // No possible winner
                    return null;
                }
            } ) );
        }

        @Override
        public void startElectionProcess( String role )
        {
            clusterContext.getLogger( getClass() ).info( "Doing elections for role " + role );
            elections.put( role, new Election( new WinnerStrategy()
            {
                @Override
                public org.neo4j.cluster.InstanceId pickWinner( Collection<Vote> voteList )
                {
                    // Remove blank votes
                    List<Vote> filteredVoteList = Iterables.toList( Iterables.filter( new Predicate<Vote>()
                    {
                        @Override
                        public boolean accept( Vote item )
                        {
                            return !( item.getCredentials() instanceof NotElectableElectionCredentials );
                        }
                    }, voteList ) );

                    // Sort based on credentials
                    // The most suited candidate should come out on top
                    Collections.sort( filteredVoteList );
                    Collections.reverse( filteredVoteList );

                    clusterContext.getLogger( getClass() ).debug( "Elections ended up with list " + filteredVoteList );

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
            elections.put( role, new Election( new WinnerStrategy()
            {
                @Override
                public org.neo4j.cluster.InstanceId pickWinner( Collection<Vote> voteList )
                {

                    // Remove blank votes
                    List<Vote> filteredVoteList = Iterables.toList( Iterables.filter( new Predicate<Vote>()
                    {
                        @Override
                        public boolean accept( Vote item )
                        {
                            return !( item.getCredentials() instanceof NotElectableElectionCredentials);
                        }
                    }, voteList ) );

                    // Sort based on credentials
                    // The most suited candidate should come out on top
                    Collections.sort( filteredVoteList );
                    Collections.reverse( filteredVoteList );

                    for ( Vote vote : filteredVoteList )
                    {
                        // Don't elect as winner the node we are trying to demote
                        if ( !vote.getSuggestedNode().equals( promoteNode ) )
                        {
                            return vote.getSuggestedNode();
                        }
                    }

                    // No possible winner
                    return null;
                }
            } ) );
        }

        @Override
        public void voted( String role, org.neo4j.cluster.InstanceId suggestedNode, Comparable<Object> suggestionCredentials )
        {
            if ( isElectionProcessInProgress( role ) )
            {
                Map<org.neo4j.cluster.InstanceId, Vote> votes = elections.get( role ).getVotes();
                votes.put( suggestedNode, new Vote( suggestedNode, suggestionCredentials ) );
            }
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
            return getClusterContext().isInCluster();
        }

        @Override
        public Iterable<org.neo4j.cluster.InstanceId> getAlive()
        {
            return getHeartbeatContext().getAlive();
        }

        @Override
        public org.neo4j.cluster.InstanceId getMyId()
        {
            return getClusterContext().getMyId();
        }

        @Override
        public boolean isElector()
        {
            // Only the first alive server should try elections. Everyone else waits
            List<org.neo4j.cluster.InstanceId> aliveInstances = Iterables.toList( getAlive() );
            Collections.sort( aliveInstances );
            return aliveInstances.indexOf( getMyId() ) == 0;
        }

        @Override
        public boolean isFailed( org.neo4j.cluster.InstanceId key )
        {
            return getHeartbeatContext().getFailed().contains( key );
        }

        @Override
        public org.neo4j.cluster.InstanceId getElected( String roleName )
        {
            return getClusterContext().getConfiguration().getElected( roleName );
        }

        @Override
        public boolean hasCurrentlyElectedVoted( String role, org.neo4j.cluster.InstanceId currentElected )
        {
            return elections.containsKey( role ) && elections.get(role).getVotes().containsKey( currentElected );
        }

        @Override
        public Set<org.neo4j.cluster.InstanceId> getFailed()
        {
            return heartbeatContext.getFailed();
        }
    }

    private static class Vote
            implements Comparable<Vote>
    {
        private final org.neo4j.cluster.InstanceId suggestedNode;
        private final Comparable<Object> voteCredentials;

        private Vote( org.neo4j.cluster.InstanceId suggestedNode, Comparable<Object> voteCredentials )
        {
            this.suggestedNode = suggestedNode;
            this.voteCredentials = voteCredentials;
        }

        public org.neo4j.cluster.InstanceId getSuggestedNode()
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

    private static class Election
    {
        private final WinnerStrategy winnerStrategy;
//        List<Vote> votes = new ArrayList<Vote>();
        private final Map<org.neo4j.cluster.InstanceId, Vote> votes = new HashMap<org.neo4j.cluster.InstanceId, Vote>();

        private Election( WinnerStrategy winnerStrategy )
        {
            this.winnerStrategy = winnerStrategy;
        }

        public Map<org.neo4j.cluster.InstanceId, Vote> getVotes()
        {
            return votes;
        }

        public org.neo4j.cluster.InstanceId pickWinner()
        {
            return winnerStrategy.pickWinner( votes.values() );
        }
    }

    interface WinnerStrategy
    {
        org.neo4j.cluster.InstanceId pickWinner( Collection<Vote> votes );
    }
}
