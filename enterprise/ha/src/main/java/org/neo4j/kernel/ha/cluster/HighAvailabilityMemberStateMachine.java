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
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.cluster.util.Quorums.isQuorum;

/**
 * State machine that listens for global cluster events, and coordinates
 * the internal transitions between ClusterMemberStates. Internal services
 * that wants to know what is going on should register ClusterMemberListener implementations
 * which will receive callbacks on state changes.
 */
public class HighAvailabilityMemberStateMachine extends LifecycleAdapter implements HighAvailability,
        AvailabilityGuard.AvailabilityRequirement
{
    private final HighAvailabilityMemberContext context;
    private final AvailabilityGuard availabilityGuard;
    private final ClusterMemberEvents events;
    private StringLogger logger;
    private Iterable<HighAvailabilityMemberListener> memberListeners = Listeners.newListeners();
    private volatile HighAvailabilityMemberState state;
    private StateMachineClusterEventListener eventsListener;
    private final ClusterMembers members;
    private final Election election;

    public HighAvailabilityMemberStateMachine( HighAvailabilityMemberContext context,
                                               AvailabilityGuard availabilityGuard,
                                               ClusterMembers members, ClusterMemberEvents events, Election election,
                                               StringLogger logger )
    {
        this.context = context;
        this.availabilityGuard = availabilityGuard;
        this.members = members;
        this.events = events;
        this.election = election;
        this.logger = logger;
        state = HighAvailabilityMemberState.PENDING;
    }

    @Override
    public void init() throws Throwable
    {
        events.addClusterMemberListener( eventsListener = new StateMachineClusterEventListener() );
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

        // If we are in a state that allows access, we must deny now that we shut down.
        if ( oldState.isAccessAllowed() )
        {
            availabilityGuard.deny(this);
        }

        context.setAvailableHaMasterId( null );
    }

    public void addHighAvailabilityMemberListener( HighAvailabilityMemberListener toAdd )
    {
        memberListeners = Listeners.addListener( toAdd, memberListeners );
    }

    public void removeHighAvailabilityMemberListener( HighAvailabilityMemberListener toRemove )
    {
        memberListeners = Listeners.removeListener( toRemove, memberListeners );
    }

    public HighAvailabilityMemberState getCurrentState()
    {
        return state;
    }

    @Override
    public String description()
    {
        return getClass().getSimpleName() + "[" + getCurrentState() + "]";
    }

    private class StateMachineClusterEventListener extends ClusterMemberListener.Adapter
    {
        @Override
        public synchronized void coordinatorIsElected( InstanceId coordinatorId )
        {
            try
            {
                HighAvailabilityMemberState oldState = state;
                InstanceId previousElected = context.getElectedMasterId();

                // Check if same coordinator was elected
//                if ( !coordinatorId.equals( previousElected ) )
                {
                    state = state.masterIsElected( context, coordinatorId );


                    context.setElectedMasterId( coordinatorId );
                    final HighAvailabilityMemberChangeEvent event = new HighAvailabilityMemberChangeEvent( oldState,
                            state, coordinatorId,
                            null );
                    Listeners.notifyListeners( memberListeners,
                            new Listeners.Notification<HighAvailabilityMemberListener>()
                            {
                                @Override
                                public void notify( HighAvailabilityMemberListener listener )
                                {
                                    listener.masterIsElected( event );
                                }
                            } );
                    context.setAvailableHaMasterId( null );

                    if ( oldState.isAccessAllowed() && oldState != state )
                    {
                        availabilityGuard.deny(HighAvailabilityMemberStateMachine.this);
                    }

                    logger.debug( "Got masterIsElected(" + coordinatorId + "), changed " + oldState + " -> " +
                            state + ". Previous elected master is " + previousElected );
                }
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( t );
            }
        }

        @Override
        public synchronized void memberIsAvailable( String role, InstanceId instanceId, URI roleUri )
        {
            try
            {
                if ( role.equals( HighAvailabilityModeSwitcher.MASTER ) )
                {
//                    if ( !roleUri.equals( context.getAvailableHaMaster() ) )
                    {
                        HighAvailabilityMemberState oldState = state;
                        context.setAvailableHaMasterId( roleUri );
                        state = state.masterIsAvailable( context, instanceId, roleUri );
                        logger.debug( "Got masterIsAvailable(" + instanceId + "), moved to " + state + " from " +
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
                            availabilityGuard.grant(HighAvailabilityMemberStateMachine.this);
                        }
                    }
                }
                else if ( role.equals( HighAvailabilityModeSwitcher.SLAVE ) )
                {
                    HighAvailabilityMemberState oldState = state;
                    state = state.slaveIsAvailable( context, instanceId, roleUri );
                    logger.debug( "Got slaveIsAvailable(" + instanceId + "), " +
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
                        availabilityGuard.grant(HighAvailabilityMemberStateMachine.this);
                    }
                }
            }
            catch ( Throwable throwable )
            {
                logger.warn( "Exception while receiving member availability notification", throwable );
            }
        }

        @Override
        public void memberIsFailed( InstanceId instanceId )
        {
            if ( !isQuorum(getAliveCount(), getTotalCount()) )
            {
                try
                {
                    if(state.isAccessAllowed())
                    {
                        availabilityGuard.deny(HighAvailabilityMemberStateMachine.this);
                    }

                    final HighAvailabilityMemberChangeEvent event =
                            new HighAvailabilityMemberChangeEvent(
                                    state, HighAvailabilityMemberState.PENDING, null, null );
                    state = HighAvailabilityMemberState.PENDING;
                    Listeners.notifyListeners( memberListeners, new Listeners
                            .Notification<HighAvailabilityMemberListener>()
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
                catch ( Throwable throwable )
                {
                    throw new RuntimeException( throwable );
                }
            }
        }

        @Override
        public void memberIsAlive( InstanceId instanceId )
        {
            if ( isQuorum(getAliveCount(), getTotalCount()) && state.equals( HighAvailabilityMemberState.PENDING ) )
            {
                election.performRoleElections();
            }
        }

        private long getAliveCount()
        {
            return Iterables.count( Iterables.filter( ClusterMembers.ALIVE, members.getMembers() ) );
        }

        private long getTotalCount()
        {
            return Iterables.count( members.getMembers() );
        }
    }
}
