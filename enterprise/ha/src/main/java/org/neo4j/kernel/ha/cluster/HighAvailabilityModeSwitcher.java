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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.function.Supplier;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Functions;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.ha.store.HighAvailabilityStoreFailureException;
import org.neo4j.kernel.ha.store.UnableToCopyStoreFromOldMasterException;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

import static org.neo4j.cluster.ClusterSettings.INSTANCE_ID;
import static org.neo4j.helpers.Functions.withDefaults;
import static org.neo4j.helpers.NamedThreadFactory.named;
import static org.neo4j.helpers.Uris.parameter;

/**
 * Performs the internal switches in various services from pending to slave/master, by listening for
 * {@link HighAvailabilityMemberChangeEvent}s. When finished it will invoke
 * {@link ClusterMemberAvailability#memberIsAvailable(String, URI, StoreId)} to announce it's new status to the
 * cluster.
 */
public class HighAvailabilityModeSwitcher
        implements HighAvailabilityMemberListener, ModeSwitcherNotifier, BindingListener, Lifecycle
{
    public static final String MASTER = "master";
    public static final String SLAVE = "slave";
    public static final String UNKNOWN = "UNKNOWN";

    public static final String INADDR_ANY = "0.0.0.0";

    private Iterable<ModeSwitcher> modeSwitchListeners = Listeners.newListeners();

    private volatile URI masterHaURI;
    private volatile URI slaveHaURI;
    private CancellationHandle cancellationHandle; // guarded by synchronized in startModeSwitching()

    public static InstanceId getServerId( URI haUri )
    {
        // Get serverId parameter, default to -1 if it is missing, and parse to integer
        return INSTANCE_ID.apply( withDefaults(
                Functions.<URI, String>constant( "-1" ), parameter( "serverId" ) ).apply( haUri ) );
    }

    private URI availableMasterId;

    private SwitchToSlave switchToSlave;
    private SwitchToMaster switchToMaster;
    private final Election election;
    private final ClusterMemberAvailability clusterMemberAvailability;
    private final ClusterClient clusterClient;
    private final Supplier<StoreId> storeIdSupplier;
    private final InstanceId instanceId;

    private final Log msgLog;
    private final Log userLog;

    private LifeSupport haCommunicationLife;

    private ScheduledExecutorService modeSwitcherExecutor;
    private volatile URI me;
    private volatile Future<?> modeSwitcherFuture;
    private volatile HighAvailabilityMemberState currentTargetState;
    private final AtomicBoolean canAskForElections = new AtomicBoolean( true );
    private final DataSourceManager neoStoreDataSourceSupplier;

    public HighAvailabilityModeSwitcher( SwitchToSlave switchToSlave,
                                         SwitchToMaster switchToMaster,
                                         Election election,
                                         ClusterMemberAvailability clusterMemberAvailability,
                                         ClusterClient clusterClient,
                                         Supplier<StoreId> storeIdSupplier,
                                         InstanceId instanceId, LogService logService,
                                         DataSourceManager neoStoreDataSourceSupplier )
    {
        this.switchToSlave = switchToSlave;
        this.switchToMaster = switchToMaster;
        this.election = election;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.clusterClient = clusterClient;
        this.storeIdSupplier = storeIdSupplier;
        this.instanceId = instanceId;
        this.msgLog = logService.getInternalLog( getClass() );
        this.userLog = logService.getUserLog( getClass() );
        this.neoStoreDataSourceSupplier = neoStoreDataSourceSupplier;
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
        modeSwitcherExecutor = createExecutor();

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

        switchToMaster.close();
        switchToMaster = null;
        switchToSlave = null;
    }

    @Override
    public void masterIsElected( HighAvailabilityMemberChangeEvent event )
    {
        if ( event.getNewState() == event.getOldState() && event.getOldState() == HighAvailabilityMemberState.MASTER )
        {
            clusterMemberAvailability.memberIsAvailable( MASTER, masterHaURI, storeIdSupplier.get() );
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
            clusterMemberAvailability.memberIsAvailable( SLAVE, slaveHaURI, storeIdSupplier.get() );
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

    @Override
    public void instanceDetached( HighAvailabilityMemberChangeEvent event )
    {
        switchToDetached();
    }

    @Override
    public void addModeSwitcher( ModeSwitcher modeSwitcher )
    {
        modeSwitchListeners = Listeners.addListener( modeSwitcher, modeSwitchListeners );
    }

    @Override
    public void removeModeSwitcher( ModeSwitcher modeSwitcher )
    {
        modeSwitchListeners = Listeners.removeListener( modeSwitcher, modeSwitchListeners );
    }

    public void forceElections()
    {
        if ( canAskForElections.compareAndSet( true, false ) )
        {
            clusterMemberAvailability.memberIsUnavailable( HighAvailabilityModeSwitcher.SLAVE );
            election.performRoleElections();
        }
    }

    private void stateChanged( HighAvailabilityMemberChangeEvent event )
    {
        if ( event.getNewState() == event.getOldState() )
        {
            /*
             * We get here if for example a new master becomes available while we are already switching. In that case
             * we don't change state but we must update with the new availableMasterId, but only if it is not null.
             */
            if ( event.getServerHaUri() != null )
            {
                availableMasterId = event.getServerHaUri();
            }
            return;
        }

        availableMasterId = event.getServerHaUri();

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

                switchToPending( event.getOldState() );
                break;
            default:
                // do nothing
        }
    }

    private void switchToMaster()
    {
        final CancellationHandle cancellationHandle = new CancellationHandle();
        startModeSwitching( new Runnable()
        {
            @Override
            public void run()
            {
                if ( currentTargetState != HighAvailabilityMemberState.TO_MASTER )
                {
                    return;
                }

                // We just got scheduled. Maybe we are already obsolete - test
                if ( cancellationHandle.cancellationRequested() )
                {
                    msgLog.info( "Switch to master cancelled on start." );
                    return;
                }

                Listeners.notifyListeners( modeSwitchListeners, new Listeners.Notification<ModeSwitcher>()
                {
                    @Override
                    public void notify( ModeSwitcher listener )
                    {
                        listener.switchToMaster();
                    }
                } );

                if ( cancellationHandle.cancellationRequested() )
                {
                    msgLog.info( "Switch to master cancelled before ha communication started." );
                    return;
                }

                haCommunicationLife.shutdown();
                haCommunicationLife = new LifeSupport();

                try
                {
                    masterHaURI = switchToMaster.switchToMaster( haCommunicationLife, me );
                    canAskForElections.set( true );
                }
                catch ( Throwable e )
                {
                    msgLog.error( "Failed to switch to master", e );
                    // Since this master switch failed, elect someone else
                    election.demote( instanceId );
                }
            }
        }, cancellationHandle );
    }

    private void switchToSlave()
    {
        // Do this with a scheduler, so that if it fails, it can retry later with an exponential backoff with max
        // wait time.
        /*
         * This is purely defensive and should never trigger. There was a race where the switch to slave task would
         * start after this instance was elected master and the task would constantly try to change as slave
         * for itself, never cancelling. This now should not be possible, since we cancel the task and wait for it
         * to complete, all in a single thread executor. However, this is a check worth doing because if this
         * condition slips through via some other code path it can cause trouble.
         */
        if ( getServerId( availableMasterId ).equals( instanceId ) )
        {
            msgLog.error( "I (" + me + ") tried to switch to slave for myself as master (" + availableMasterId + ")" );
            return;
        }
        final AtomicLong wait = new AtomicLong();
        final CancellationHandle cancellationHandle = new CancellationHandle();
        startModeSwitching( new Runnable()
        {
            @Override
            public void run()
            {
                if ( currentTargetState != HighAvailabilityMemberState.TO_SLAVE )
                {
                    return; // Already switched - this can happen if a second master becomes available while waiting
                }

                if ( cancellationHandle.cancellationRequested() )
                {
                    msgLog.info( "Switch to slave cancelled on start." );
                    return;
                }

                Listeners.notifyListeners( modeSwitchListeners, new Listeners.Notification<ModeSwitcher>()
                {
                    @Override
                    public void notify( ModeSwitcher listener )
                    {
                        listener.switchToSlave();
                    }
                } );

                try
                {
                    if ( cancellationHandle.cancellationRequested() )
                    {
                        msgLog.info( "Switch to slave cancelled before ha communication started." );
                        return;
                    }

                    haCommunicationLife.shutdown();
                    haCommunicationLife = new LifeSupport();

                    // it is important for availableMasterId to be re-read on every attempt so that
                    // slave switching would not result in an infinite loop with wrong/stale availableMasterId
                    URI resultingSlaveHaURI = switchToSlave.switchToSlave( haCommunicationLife, me, availableMasterId, cancellationHandle );
                    if ( resultingSlaveHaURI == null )
                    {
                        /*
                         * null slave uri means the task was cancelled. The task then must simply terminate and
                         * have no side effects.
                         */
                        msgLog.info( "Switch to slave is effectively cancelled" );
                    }
                    else
                    {
                        slaveHaURI = resultingSlaveHaURI;
                        canAskForElections.set( true );
                    }
                }
                catch ( HighAvailabilityStoreFailureException e )
                {
                    userLog.error( "UNABLE TO START UP AS SLAVE: %s", e.getMessage() );
                    msgLog.error( "Unable to start up as slave", e );

                    clusterMemberAvailability.memberIsUnavailable( SLAVE );
                    ClusterClient clusterClient = HighAvailabilityModeSwitcher.this.clusterClient;
                    try
                    {
                        // TODO I doubt this actually works
                        clusterClient.leave();
                        clusterClient.stop();
                        haCommunicationLife.shutdown();
                    }
                    catch ( Throwable t )
                    {
                        msgLog.error( "Unable to stop cluster client", t );
                    }

                    modeSwitcherExecutor.schedule( this, 5, TimeUnit.SECONDS );
                }
                catch ( MismatchingStoreIdException e )
                {
                    // Try again immediately, the place that threw it have already treated the db
                    // as branched and so a new attempt will have this slave copy a new store from master.
                    run();
                }
                catch ( Throwable t )
                {
                    msgLog.error( "Error while trying to switch to slave", t );

                    // Try again later
                    wait.set( (1 + wait.get() * 2) ); // Exponential backoff
                    wait.set( Math.min( wait.get(), 5 * 60 ) ); // Wait maximum 5 minutes

                    modeSwitcherFuture = modeSwitcherExecutor.schedule( this, wait.get(), TimeUnit.SECONDS );

                    msgLog.info( "Attempting to switch to slave in %ds", wait.get() );
                }
            }
        }, cancellationHandle );
    }

    private void switchToPending( final HighAvailabilityMemberState oldState )
    {
        msgLog.info( "I am %s, moving to pending", instanceId );

        startModeSwitching( new Runnable()
        {
            @Override
            public void run()
            {
                if ( cancellationHandle.cancellationRequested() )
                {
                    msgLog.info( "Switch to pending cancelled on start." );
                    return;
                }

                if ( oldState.equals( HighAvailabilityMemberState.SLAVE ) )
                {
                    clusterMemberAvailability.memberIsUnavailable( SLAVE );
                }
                else if ( oldState.equals( HighAvailabilityMemberState.MASTER ) )
                {
                    clusterMemberAvailability.memberIsUnavailable( MASTER );
                }

                Listeners.notifyListeners( modeSwitchListeners, new Listeners.Notification<ModeSwitcher>()
                {
                    @Override
                    public void notify( ModeSwitcher listener )
                    {
                        listener.switchToPending();
                    }
                } );
                neoStoreDataSourceSupplier.getDataSource().beforeModeSwitch();

                if ( cancellationHandle.cancellationRequested() )
                {
                    msgLog.info( "Switch to pending cancelled before ha communication shutdown." );
                    return;
                }

                haCommunicationLife.shutdown();
                haCommunicationLife = new LifeSupport();
            }
        }, new CancellationHandle() );

        try
        {
            modeSwitcherFuture.get( 10, TimeUnit.SECONDS );
        }
        catch ( Exception e )
        {
            msgLog.warn( "Exception received while waiting for switching to pending", e );
        }
    }

    private void switchToDetached()
    {
        msgLog.info( "I am %s, moving to detached", instanceId );

        startModeSwitching( new Runnable()
        {
            @Override
            public void run()
            {
                if ( cancellationHandle.cancellationRequested() )
                {
                    msgLog.info( "Switch to pending cancelled on start." );
                    return;
                }

                Listeners.notifyListeners( modeSwitchListeners, new Listeners.Notification<ModeSwitcher>()
                {
                    @Override
                    public void notify( ModeSwitcher listener )
                    {
                        listener.switchToSlave();
                    }
                } );
                neoStoreDataSourceSupplier.getDataSource().beforeModeSwitch();

                if ( cancellationHandle.cancellationRequested() )
                {
                    msgLog.info( "Switch to pending cancelled before ha communication shutdown." );
                    return;
                }

                haCommunicationLife.shutdown();
                haCommunicationLife = new LifeSupport();
            }
        }, new CancellationHandle() );

        try
        {
            modeSwitcherFuture.get( 10, TimeUnit.SECONDS );
        }
        catch ( Exception e )
        {
            msgLog.warn( "Exception received while waiting for switching to detached", e );
        }
    }

    private synchronized void startModeSwitching( Runnable switcher, CancellationHandle cancellationHandle )
    {
        if ( modeSwitcherFuture != null )
        {
            // Cancel any delayed previous switching
            this.cancellationHandle.cancel();
            // Wait for it to actually stop what it was doing
            try
            {
                modeSwitcherFuture.get();
            }
            catch ( UnableToCopyStoreFromOldMasterException e )
            {
                throw e;
            }
            catch ( Exception e )
            {
                msgLog.warn( "Got exception from cancelled task", e );
            }
        }

        this.cancellationHandle = cancellationHandle;
        modeSwitcherFuture = modeSwitcherExecutor.submit( switcher );
    }

    ScheduledExecutorService createExecutor()
    {
        return Executors.newSingleThreadScheduledExecutor( named( "HA Mode switcher" ) );
    }

    private static class CancellationHandle implements CancellationRequest
    {
        private volatile boolean cancelled = false;

        @Override
        public boolean cancellationRequested()
        {
            return cancelled;
        }

        public void cancel()
        {
            assert !cancelled : "Should not cancel on the same request twice";
            cancelled = true;
        }
    }
}
