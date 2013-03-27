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
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha.InstanceAccessGuard;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * State machine that listens for global cluster events, and coordinates
 * the internal transitions between ClusterMemberStates. Internal services
 * that wants to know what is going on should register ClusterMemberListener implementations
 * which will receive callbacks on state changes.
 */
public class HighAvailabilityMemberStateMachine extends LifecycleAdapter implements HighAvailability
{
    private final HighAvailabilityMemberContext context;
    private final InstanceAccessGuard accessGuard;
    private final ClusterMemberEvents events;
    private StringLogger logger;
    private Iterable<HighAvailabilityMemberListener> memberListeners = Listeners.newListeners();
    private HighAvailabilityMemberState state;
    private StateMachineClusterEventListener eventsListener;
    private final ClusterMembers members;
    private final Election election;

    public HighAvailabilityMemberStateMachine( HighAvailabilityMemberContext context, InstanceAccessGuard accessGuard,
                                               ClusterMembers members, ClusterMemberEvents events, Election election,
                                               StringLogger logger )
    {
        this.context = context;
        this.accessGuard = accessGuard;
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
        accessGuard.setState( state );
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

    private class StateMachineClusterEventListener extends ClusterMemberListener.Adapter
    {
        @Override
        public synchronized void coordinatorIsElected( InstanceId coordinatorId )
        {
            try
            {
                HighAvailabilityMemberState oldState = state;
                InstanceId previousElected = context.getElectedMasterId();
                state = state.masterIsElected( context, coordinatorId );

                context.setElectedMasterId( coordinatorId );
                final HighAvailabilityMemberChangeEvent event = new HighAvailabilityMemberChangeEvent( oldState, state, coordinatorId,
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
                accessGuard.setState( state );
                logger.debug( "Got masterIsElected(" + coordinatorId + "), changed " + oldState + " -> " +
                        state + ". Previous elected master is " + previousElected );
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
                    if ( !roleUri.equals( context.getAvailableHaMaster() ) )
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
                        accessGuard.setState( state );
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
                    accessGuard.setState( state );
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
            if ( getAliveCount() <= getTotalCount() / 2 )
            {
                try
                {
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
                    accessGuard.setState( state );
                    context.setAvailableHaMasterId( null );
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
            if ( getAliveCount() > getTotalCount() / 2
                    && state.equals( HighAvailabilityMemberState.PENDING ) )
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
