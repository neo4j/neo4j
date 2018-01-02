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
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.cluster.member.ObservedClusterMembers;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.cluster.util.Quorums.isQuorum;
import static org.neo4j.kernel.AvailabilityGuard.AvailabilityRequirement;
import static org.neo4j.kernel.AvailabilityGuard.availabilityRequirement;

/**
 * State machine that listens for global cluster events, and coordinates
 * the internal transitions between {@link HighAvailabilityMemberState}. Internal services
 * that wants to know what is going on should register {@link HighAvailabilityMemberListener} implementations
 * which will receive callbacks on state changes.
 * <p>
 * HA in Neo4j is built on top of the clustering functionality. So, this state machine essentially reacts to cluster
 * events,
 * and implements the rules for how HA roles should change, for example, the cluster coordinator should become the HA
 * master.
 */
public class HighAvailabilityMemberStateMachine extends LifecycleAdapter implements HighAvailability
{
    public static final AvailabilityRequirement AVAILABILITY_REQUIREMENT =
            availabilityRequirement( "High Availability member state not ready" );
    private final HighAvailabilityMemberContext context;
    private final AvailabilityGuard availabilityGuard;
    private final ClusterMemberEvents events;
    private Log log;

    private Iterable<HighAvailabilityMemberListener> memberListeners = Listeners.newListeners();
    private volatile HighAvailabilityMemberState state;
    private StateMachineClusterEventListener eventsListener;
    private final ObservedClusterMembers members;
    private final Election election;

    public HighAvailabilityMemberStateMachine( HighAvailabilityMemberContext context,
                                               AvailabilityGuard availabilityGuard,
                                               ObservedClusterMembers members,
                                               ClusterMemberEvents events,
                                               Election election,
                                               LogProvider logProvider )
    {
        this.context = context;
        this.availabilityGuard = availabilityGuard;
        this.members = members;
        this.events = events;
        this.election = election;
        this.log = logProvider.getLog( getClass() );
        state = HighAvailabilityMemberState.PENDING;
    }

    @Override
    public void init() throws Throwable
    {
        events.addClusterMemberListener( eventsListener = new StateMachineClusterEventListener() );
        // On initial startup, disallow database access
        availabilityGuard.require( AVAILABILITY_REQUIREMENT );
    }

    @Override
    public void stop() throws Throwable
    {
        events.removeClusterMemberListener( eventsListener );
        HighAvailabilityMemberState oldState = state;
        state = HighAvailabilityMemberState.PENDING;
        final HighAvailabilityMemberChangeEvent event =
                new HighAvailabilityMemberChangeEvent( oldState, state, null, null );
        Listeners.notifyListeners( memberListeners, new Listeners.Notification<HighAvailabilityMemberListener>()
        {
            @Override
            public void notify( HighAvailabilityMemberListener listener )
            {
                listener.instanceStops( event );
            }
        } );

        // If we were previously in a state that allowed access, we must now deny access
        if ( oldState.isAccessAllowed() )
        {
            availabilityGuard.require( AVAILABILITY_REQUIREMENT );
        }

        context.setAvailableHaMasterId( null );
    }

    @Override
    public void addHighAvailabilityMemberListener( HighAvailabilityMemberListener toAdd )
    {
        memberListeners = Listeners.addListener( toAdd, memberListeners );
    }

    @Override
    public void removeHighAvailabilityMemberListener( HighAvailabilityMemberListener toRemove )
    {
        memberListeners = Listeners.removeListener( toRemove, memberListeners );
    }

    public HighAvailabilityMemberState getCurrentState()
    {
        return state;
    }

    public boolean isMaster()
    {
        return getCurrentState() == HighAvailabilityMemberState.MASTER;
    }

    /**
     * This listener will get all events about cluster instances, and depending on the current state it will
     * correctly transition to the next internal state and notify listeners of this change.
     */
    private class StateMachineClusterEventListener implements ClusterMemberListener
    {
        @Override
        public synchronized void coordinatorIsElected( InstanceId coordinatorId )
        {
            try
            {
                HighAvailabilityMemberState oldState = state;
                InstanceId previousElected = context.getElectedMasterId();

                context.setAvailableHaMasterId( null );
                if ( !acceptNewState( state.masterIsElected( context, coordinatorId ) ) )
                {
                    return;
                }

                context.setElectedMasterId( coordinatorId );
                final HighAvailabilityMemberChangeEvent event =
                        new HighAvailabilityMemberChangeEvent( oldState, state, coordinatorId, null );
                Listeners.notifyListeners( memberListeners,
                        new Listeners.Notification<HighAvailabilityMemberListener>()
                        {
                            @Override
                            public void notify( HighAvailabilityMemberListener listener )
                            {
                                listener.masterIsElected( event );
                            }
                        } );

                if ( oldState.isAccessAllowed() && oldState != state )
                {
                    availabilityGuard.require( AVAILABILITY_REQUIREMENT );
                }

                log.debug( "Got masterIsElected(" + coordinatorId + "), moved to " + state + " from " + oldState
                        + ". Previous elected master is " + previousElected );
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( t );
            }
        }

        @Override
        public synchronized void memberIsAvailable( String role, InstanceId instanceId, URI roleUri, StoreId storeId )
        {
            try
            {
                /**
                 * Do different things depending on whether the cluster member is in master or slave state
                 */
                if ( role.equals( HighAvailabilityModeSwitcher.MASTER ) )
                {
                    HighAvailabilityMemberState oldState = state;
                    context.setAvailableHaMasterId( roleUri );
                    if ( !acceptNewState( state.masterIsAvailable( context, instanceId, roleUri ) ) )
                    {
                        return;
                    }
                    log.debug( "Got masterIsAvailable(" + instanceId + "), moved to " + state + " from " +
                            oldState );
                    final HighAvailabilityMemberChangeEvent event = new HighAvailabilityMemberChangeEvent( oldState,
                            state, instanceId, roleUri );
                    Listeners.notifyListeners( memberListeners,
                            new Listeners.Notification<HighAvailabilityMemberListener>()
                            {
                                @Override
                                public void notify( HighAvailabilityMemberListener listener )
                                {
                                    listener.masterIsAvailable( event );
                                }
                            } );

                    if ( oldState == HighAvailabilityMemberState.TO_MASTER && state ==
                            HighAvailabilityMemberState.MASTER )
                    {
                        availabilityGuard.fulfill( AVAILABILITY_REQUIREMENT );
                    }
                }
                else if ( role.equals( HighAvailabilityModeSwitcher.SLAVE ) )
                {
                    HighAvailabilityMemberState oldState = state;
                    if ( !acceptNewState( state.slaveIsAvailable( context, instanceId, roleUri ) ) )
                    {
                        return;
                    }
                    log.debug( "Got slaveIsAvailable(" + instanceId + "), " +
                            "moved to " + state + " from " + oldState );
                    final HighAvailabilityMemberChangeEvent event = new HighAvailabilityMemberChangeEvent( oldState,
                            state, instanceId, roleUri );
                    Listeners.notifyListeners( memberListeners,
                            new Listeners.Notification<HighAvailabilityMemberListener>()
                            {
                                @Override
                                public void notify( HighAvailabilityMemberListener listener )
                                {
                                    listener.slaveIsAvailable( event );
                                }
                            } );

                    if ( oldState == HighAvailabilityMemberState.TO_SLAVE &&
                            state == HighAvailabilityMemberState.SLAVE )
                    {
                        availabilityGuard.fulfill( AVAILABILITY_REQUIREMENT );
                    }
                }
            }
            catch ( Throwable throwable )
            {
                log.warn( "Exception while receiving member availability notification", throwable );
            }
        }

        @Override
        public void memberIsUnavailable( String role, InstanceId unavailableId )
        {
            if ( context.getMyId().equals( unavailableId ) &&
                    HighAvailabilityModeSwitcher.SLAVE.equals( role ) &&
                    state == HighAvailabilityMemberState.SLAVE )
            {
                HighAvailabilityMemberState oldState = state;
                changeStateToPending();
                log.debug( "Got memberIsUnavailable(" + unavailableId + "), moved to " + state + " from " + oldState );
            }
            else
            {
                log.debug( "Got memberIsUnavailable(" + unavailableId + ")" );
            }
        }

        @Override
        public void memberIsFailed( InstanceId instanceId )
        {
            // If we don't have quorum anymore with the currently alive members, then go to pending
            if ( !isQuorum( getAliveCount(), getTotalCount() ) )
            {
                HighAvailabilityMemberState oldState = state;
                changeStateToDetached();
                log.debug( "Got memberIsFailed(" + instanceId + ") and cluster lost quorum to continue, moved to "
                        + state + " from " + oldState + ", while maintaining read only capability." );
            }
            else if ( instanceId.equals( context.getElectedMasterId() ) && state == HighAvailabilityMemberState.SLAVE )
            {
                HighAvailabilityMemberState oldState = state;
                changeStateToDetached();
                log.debug( "Got memberIsFailed(" + instanceId + ") which was the master and i am a slave, moved to "
                        + state + " from " + oldState + ", while maintaining read only capability." );
            }
            else
            {
                log.debug( "Got memberIsFailed(" + instanceId + ")" );
            }
        }

        @Override
        public void memberIsAlive( InstanceId instanceId )
        {
            // If we now have quorum and the previous state was pending, then ask for an election
            if ( isQuorum( getAliveCount(), getTotalCount() ) && state.equals( HighAvailabilityMemberState.PENDING ) )
            {
                election.performRoleElections();
            }
        }

        private void changeStateToPending()
        {
            if ( state.isAccessAllowed() )
            {
                availabilityGuard.require( AVAILABILITY_REQUIREMENT );
            }

            final HighAvailabilityMemberChangeEvent event =
                    new HighAvailabilityMemberChangeEvent( state, HighAvailabilityMemberState.PENDING, null, null );

            state = HighAvailabilityMemberState.PENDING;

            Listeners.notifyListeners( memberListeners, new Listeners.Notification<HighAvailabilityMemberListener>()
            {
                @Override
                public void notify( HighAvailabilityMemberListener listener )
                {
                    listener.instanceStops( event );
                }
            } );

            context.setAvailableHaMasterId( null );
            context.setElectedMasterId( null );
        }

        private void changeStateToDetached()
        {
            state = HighAvailabilityMemberState.PENDING;
            final HighAvailabilityMemberChangeEvent event =
                    new HighAvailabilityMemberChangeEvent( state, HighAvailabilityMemberState.PENDING, null, null );
            Listeners.notifyListeners( memberListeners, new Listeners.Notification<HighAvailabilityMemberListener>()
            {
                @Override
                public void notify( HighAvailabilityMemberListener listener )
                {
                    listener.instanceDetached( event );
                }
            } );

            context.setAvailableHaMasterId( null );
            context.setElectedMasterId( null );
        }

        private long getAliveCount()
        {
            return Iterables.count( members.getAliveMembers() );
        }

        private long getTotalCount()
        {
            return Iterables.count( members.getMembers() );
        }

        /**
         * Checks if the new state is ILLEGAL. If so, it sets the state to PENDING and issues a request for
         * elections. Otherwise it sets the current state to newState.
         * @return false iff the newState is illegal. true otherwise.
         */
        private boolean acceptNewState( HighAvailabilityMemberState newState )
        {
            if ( newState == HighAvailabilityMemberState.ILLEGAL )
            {
                log.warn( format( "Message received resulted in illegal state transition. I was in state %s, " +
                        "context was %s. The error message is %s. This instance will now transition to PENDING state " +
                        "and " +
                        "ask for new elections. While this may fix the error, it may indicate that there is some " +
                        "connectivity issue or some instability of cluster members.", state, context, newState
                        .errorMessage() ) );
                context.setElectedMasterId( null );
                context.setAvailableHaMasterId( null );
                changeStateToPending();
                election.performRoleElections();
                return false;
            }
            else
            {
                state = newState;
            }
            return true;
        }
    }
}
