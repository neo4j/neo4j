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
package org.neo4j.kernel.ha.cluster;

import static org.neo4j.cluster.ClusterSettings.INSTANCE_ID;
import static org.neo4j.helpers.Functions.withDefaults;
import static org.neo4j.helpers.NamedThreadFactory.named;
import static org.neo4j.helpers.Uris.parameter;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.helpers.Functions;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Performs the internal switches from pending to slave/master, by listening for
 * ClusterMemberChangeEvents. When finished it will invoke {@link org.neo4j.cluster.member.ClusterMemberAvailability#memberIsAvailable(String, URI)} to announce
 * to the cluster it's new status.
 */
public class HighAvailabilityModeSwitcher implements HighAvailabilityMemberListener, BindingListener, Lifecycle
{
    public static final String MASTER = "master";
    public static final String SLAVE = "slave";
    public static final String INADDR_ANY = "0.0.0.0";

    private volatile URI masterHaURI;
    private volatile URI slaveHaURI;

    public static InstanceId getServerId( URI haUri )
    {
        // Get serverId parameter, default to -1 if it is missing, and parse to integer
        return INSTANCE_ID.apply( withDefaults(
                Functions.<URI, String>constant( "-1" ), parameter( "serverId" ) ).apply( haUri ));
    }

    private URI availableMasterId;

    private final SwitchToSlave switchToSlave;
    private final SwitchToMaster switchToMaster;
    private final Election election;
    private final ClusterMemberAvailability clusterMemberAvailability;
    private final StringLogger msgLog;

    private LifeSupport haCommunicationLife;

    private ScheduledExecutorService modeSwitcherExecutor;
    private volatile URI me;
    private volatile Future<?> modeSwitcherFuture;
    private volatile HighAvailabilityMemberState currentTargetState;

    public HighAvailabilityModeSwitcher( SwitchToSlave switchToSlave,
                                         SwitchToMaster switchToMaster,
                                         Election election,
                                         ClusterMemberAvailability clusterMemberAvailability,
                                         StringLogger msgLog)
    {
        this.switchToSlave = switchToSlave;
        this.switchToMaster = switchToMaster;
        this.election = election;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.msgLog = msgLog;
        this.haCommunicationLife = new LifeSupport();
    }

    @Override
    public void listeningAt( URI myUri )
    {
        me = myUri;
    }

    @Override
    public synchronized void init() throws Throwable
    {
        modeSwitcherExecutor = Executors.newSingleThreadScheduledExecutor( named( "HA Mode switcher" ) );

        haCommunicationLife.init();
    }

    @Override
    public synchronized void start() throws Throwable
    {
        haCommunicationLife.start();
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        haCommunicationLife.stop();
    }

    @Override
    public synchronized void shutdown() throws Throwable
    {
        modeSwitcherExecutor.shutdown();

        modeSwitcherExecutor.awaitTermination( 60, TimeUnit.SECONDS );

        haCommunicationLife.shutdown();
    }

    @Override
    public void masterIsElected( HighAvailabilityMemberChangeEvent event )
    {
        if ( event.getNewState() == event.getOldState() && event.getOldState() == HighAvailabilityMemberState.MASTER )
        {
            clusterMemberAvailability.memberIsAvailable( MASTER, masterHaURI );
        }
        else
        {
            stateChanged( event );
        }
    }

    @Override
    public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
    {
        if ( event.getNewState() == event.getOldState() && event.getOldState() == HighAvailabilityMemberState.SLAVE )
        {
            clusterMemberAvailability.memberIsAvailable( SLAVE, slaveHaURI );
        }
        else
        {
            stateChanged( event );
        }
    }

    @Override
    public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
    {
        // ignored, we don't do any mode switching in slave available events
    }

    @Override
    public void instanceStops( HighAvailabilityMemberChangeEvent event )
    {
        stateChanged( event );
    }

    private void stateChanged( HighAvailabilityMemberChangeEvent event )
    {
        availableMasterId = event.getServerHaUri();
        if ( event.getNewState() == event.getOldState() )
        {
            return;
        }

        currentTargetState = event.getNewState();
        switch ( event.getNewState() )
        {
            case TO_MASTER:

                if ( event.getOldState().equals( HighAvailabilityMemberState.SLAVE ) )
                {
                    clusterMemberAvailability.memberIsUnavailable( SLAVE );
                }

                switchToMaster();
                break;
            case TO_SLAVE:
                switchToSlave();
                break;
            case PENDING:
                if ( event.getOldState().equals( HighAvailabilityMemberState.SLAVE ) )
                {
                    clusterMemberAvailability.memberIsUnavailable( SLAVE );
                }
                else if ( event.getOldState().equals( HighAvailabilityMemberState.MASTER ) )
                {
                    clusterMemberAvailability.memberIsUnavailable( MASTER );
                }

                startModeSwitching( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        haCommunicationLife.shutdown();
                        haCommunicationLife = new LifeSupport();
                    }
                });

                try
                {
                    modeSwitcherFuture.get(10, TimeUnit.SECONDS);
                }
                catch ( Exception e )
                {
                    // Ignore
                }

                break;
            default:
                // do nothing
        }
    }

    private void switchToMaster()
    {
        startModeSwitching( new Runnable()
        {
            @Override
            public void run()
            {
                if ( currentTargetState != HighAvailabilityMemberState.TO_MASTER )
                {
                    return;
                }

                haCommunicationLife.shutdown();
                haCommunicationLife = new LifeSupport();

                try
                {
                    masterHaURI = switchToMaster.switchToMaster( haCommunicationLife, me );
                }
                catch ( Throwable e )
                {
                    msgLog.logMessage( "Failed to switch to master", e );

                    // Since this master switch failed, elect someone else
                    election.demote( getServerId( me ) );

                    return;
                }
            }
        } );
    }

    private void switchToSlave()
    {
        // Do this with a scheduler, so that if it fails, it can retry later with an exponential backoff with max wait time.
        final URI masterUri = availableMasterId;
        final AtomicLong wait = new AtomicLong();
        startModeSwitching( new Runnable()
        {
            @Override
            public void run()
            {
                if (currentTargetState != HighAvailabilityMemberState.TO_SLAVE)
                {
                    return; // Already switched - this can happen if a second master becomes available while waiting
                }

                try
                {
                    haCommunicationLife.shutdown();
                    haCommunicationLife = new LifeSupport();

                    slaveHaURI = switchToSlave.switchToSlave(haCommunicationLife, me, masterUri);
                } catch (MismatchingStoreIdException e)
                {
                    // Try again immediately
                    run();
                } catch ( Throwable t )
                {
                    msgLog.logMessage( "Error while trying to switch to slave", t );

                    // Try again later
                    wait.set( (1 + wait.get()*2) ); // Exponential backoff
                    wait.set(Math.min(wait.get(), 5*60)); // Wait maximum 5 minutes

                    modeSwitcherFuture = modeSwitcherExecutor.schedule( this, wait.get(), TimeUnit.SECONDS );

                    msgLog.logMessage( "Attempting to switch to slave in "+wait.get()+"s");
                }
            }
        } );
    }

    private void startModeSwitching( Runnable switcher )
    {
        if ( modeSwitcherFuture != null )
        {
            // Cancel any delayed previous switching
            modeSwitcherFuture.cancel( false );
        }

        modeSwitcherFuture = modeSwitcherExecutor.submit( switcher );
    }
}
