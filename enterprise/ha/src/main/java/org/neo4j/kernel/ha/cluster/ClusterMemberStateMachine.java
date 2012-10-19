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
public class ClusterMemberStateMachine extends LifecycleAdapter
{
    private final ClusterMemberContext context;
    private final InstanceAccessGuard accessGuard;
    private final ClusterEvents clusterEvents;
    private StringLogger logger;
    private Iterable<ClusterMemberListener> clusterMemberListeners = Listeners.newListeners();
    private ClusterMemberState state;

    public ClusterMemberStateMachine( ClusterMemberContext context, InstanceAccessGuard accessGuard,
                                      ClusterEvents clusterEvents, StringLogger logger )
    {
        this.context = context;
        this.accessGuard = accessGuard;
        this.clusterEvents = clusterEvents;
        this.logger = logger;
        clusterEvents.addClusterEventListener( new StateMachineClusterEventListener() );
        state = ClusterMemberState.PENDING;
    }

    @Override
    public void stop() throws Throwable
    {
        ClusterMemberState oldState = state;
        state = ClusterMemberState.PENDING;
        final ClusterMemberChangeEvent event = new ClusterMemberChangeEvent( oldState, state, null, null );
        Listeners.notifyListeners( clusterMemberListeners, new Listeners.Notification<ClusterMemberListener>()
        {
            @Override
            public void notify( ClusterMemberListener listener )
            {
                listener.instanceStops( event );
            }
        } );

        accessGuard.setState( state );
    }

    public void addClusterMemberListener( ClusterMemberListener toAdd )
    {
        clusterMemberListeners = Listeners.addListener( toAdd, clusterMemberListeners );
    }

    public ClusterMemberState getCurrentState()
    {
        return state;
    }

    private class StateMachineClusterEventListener implements ClusterEventListener
    {
        @Override
        public void masterIsElected( URI masterUri )
        {
            try
            {
                ClusterMemberState oldState = state;
                URI previousElected = context.getElectedMasterId();
                String msg = "";
                if ( oldState.equals( ClusterMemberState.MASTER ) && masterUri.equals( context.getMyId() ) )
                {
                    clusterEvents.memberIsAvailable( ClusterConfiguration.COORDINATOR );
                    msg = "(Sent masterIsAvailable) ";
                }
                else //if ( !masterUri.equals( context.getMyId() ) )
                {
                    state = state.masterIsElected( context, masterUri );

                    context.setElectedMasterId( masterUri );
                    final ClusterMemberChangeEvent event = new ClusterMemberChangeEvent( oldState, state, masterUri,
                            null );
                    Listeners.notifyListeners( clusterMemberListeners,
                            new Listeners.Notification<ClusterMemberListener>()
                            {
                                @Override
                                public void notify( ClusterMemberListener listener )
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
                        ClusterMemberState oldState = state;
                        context.setAvailableHaMasterId( masterHaUri );
                        state = state.masterIsAvailable( context, instanceClusterUri, masterHaUri );
                        logger.debug( "Got masterIsAvailable(" + instanceClusterUri + ", moved to " + state + " from " +
                                oldState );
                        final ClusterMemberChangeEvent event = new ClusterMemberChangeEvent( oldState, state,
                                instanceClusterUri,
                                masterHaUri );
                        Listeners.notifyListeners( clusterMemberListeners,
                                new Listeners.Notification<ClusterMemberListener>()
                                {
                                    @Override
                                    public void notify( ClusterMemberListener listener )
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
                    ClusterMemberState oldState = state;
                    state = state.slaveIsAvailable( context, instanceClusterUri );
                    logger.debug( "Got slaveIsAvailable(" + instanceClusterUri + "), " +
                            "moved to " + state + " from " + oldState );
                    final ClusterMemberChangeEvent event = new ClusterMemberChangeEvent( oldState, state,
                            instanceClusterUri,
                            slaveHaUri );
                    Listeners.notifyListeners( clusterMemberListeners,
                            new Listeners.Notification<ClusterMemberListener>()
                            {
                                @Override
                                public void notify( ClusterMemberListener listener )
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
