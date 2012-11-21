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

package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.com.ServerUtil;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.ha.InstanceAccessGuard;
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
    private final HighAvailabilityEvents events;
    private StringLogger logger;
    private Iterable<HighAvailabilityMemberListener> memberListeners = Listeners.newListeners();
    private HighAvailabilityMemberState state;
    private StateMachineClusterEventListener eventsListener;

    public HighAvailabilityMemberStateMachine( HighAvailabilityMemberContext context, InstanceAccessGuard accessGuard,
                                      HighAvailabilityEvents events, StringLogger logger )
    {
        this.context = context;
        this.accessGuard = accessGuard;
        this.events = events;
        this.logger = logger;
        state = HighAvailabilityMemberState.PENDING;
    }

    @Override
    public void init() throws Throwable
    {
        events.addClusterEventListener( eventsListener = new StateMachineClusterEventListener() );
    }

    @Override
    public void stop() throws Throwable
    {
        events.removeClusterEventListener( eventsListener );
        HighAvailabilityMemberState oldState = state;
        state = HighAvailabilityMemberState.PENDING;
        final HighAvailabilityMemberChangeEvent event = new HighAvailabilityMemberChangeEvent( oldState, state, null, null );
        Listeners.notifyListeners( memberListeners, new Listeners.Notification<HighAvailabilityMemberListener>()
        {
            @Override
            public void notify( HighAvailabilityMemberListener listener )
            {
                listener.instanceStops( event );
            }
        } );
        accessGuard.setState( state );
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

    private class StateMachineClusterEventListener implements HighAvailabilityListener
    {
        @Override
        public void masterIsElected( URI masterUri )
        {
            try
            {
                HighAvailabilityMemberState oldState = state;
                URI previousElected = context.getElectedMasterId();
                String msg = "";
                if ( oldState.equals( HighAvailabilityMemberState.MASTER ) && masterUri.equals( context.getMyId() ) )
                {
                    events.memberIsAvailable( ClusterConfiguration.COORDINATOR );
                    msg = "(Sent masterIsAvailable) ";
                }
                else //if ( !masterUri.equals( context.getMyId() ) )
                {
                    state = state.masterIsElected( context, masterUri );

                    context.setElectedMasterId( masterUri );
                    final HighAvailabilityMemberChangeEvent event = new HighAvailabilityMemberChangeEvent( oldState, state, masterUri,
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
                }
                logger.debug( msg + "Got masterIsElected(" + masterUri + "), moved to " + state + " from " +
                        oldState + ". Previous elected master is " + previousElected );
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( t );
            }
        }

        @Override
        public void memberIsAvailable( String role, URI instanceClusterUri, Iterable<URI> instanceUris )
        {
            try
            {
                if ( role.equals( ClusterConfiguration.COORDINATOR ) )
                {
                    URI masterHaUri = ServerUtil.getUriForScheme( "ha", instanceUris );
                    if ( !masterHaUri.equals( context.getAvailableHaMaster() ) )
                    {
                        HighAvailabilityMemberState oldState = state;
                        context.setAvailableHaMasterId( masterHaUri );
                        state = state.masterIsAvailable( context, instanceClusterUri, masterHaUri );
                        logger.debug( "Got masterIsAvailable(" + instanceClusterUri + ", moved to " + state + " from " +
                                oldState );
                        final HighAvailabilityMemberChangeEvent event = new HighAvailabilityMemberChangeEvent( oldState, state,
                                instanceClusterUri,
                                masterHaUri );
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
                else if ( role.equals( ClusterConfiguration.SLAVE ) )
                {
                    URI slaveHaUri = ServerUtil.getUriForScheme( "ha", instanceUris );
                    HighAvailabilityMemberState oldState = state;
                    state = state.slaveIsAvailable( context, instanceClusterUri );
                    logger.debug( "Got slaveIsAvailable(" + instanceClusterUri + "), " +
                            "moved to " + state + " from " + oldState );
                    final HighAvailabilityMemberChangeEvent event = new HighAvailabilityMemberChangeEvent( oldState, state,
                            instanceClusterUri,
                            slaveHaUri );
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
    }
}
