/*
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
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Functions;
import org.neo4j.kernel.ha.store.HighAvailabilityStoreFailureException;
import org.neo4j.kernel.ha.store.InconsistentlyUpgradedClusterException;
import org.neo4j.kernel.ha.store.UnableToCopyStoreFromOldMasterException;
import org.neo4j.kernel.ha.store.UnavailableMembersException;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.cluster.ClusterSettings.INSTANCE_ID;
import static org.neo4j.helpers.Functions.withDefaults;
import static org.neo4j.helpers.NamedThreadFactory.named;
import static org.neo4j.helpers.Uris.parameter;

/**
 * Performs the internal switches from pending to slave/master, by listening for
 * {@link HighAvailabilityMemberChangeEvent}s. When finished it will invoke
 * {@link ClusterMemberAvailability#memberIsAvailable(String, URI, StoreId)} to announce it's new status to the
 * cluster.
 */
public class HighAvailabilityModeSwitcher implements HighAvailabilityMemberListener, BindingListener, Lifecycle
{

    public static final String MASTER = "master";
    public static final String SLAVE = "slave";
    public static final String UNKNOWN = "UNKNOWN";

    public static final String INADDR_ANY = "0.0.0.0";

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
    private final InstanceId instanceId;
    private final DependencyResolver dependencyResolver;

    private final StringLogger msgLog;
    private final ConsoleLogger consoleLog;

    private LifeSupport haCommunicationLife;

    private ScheduledExecutorService modeSwitcherExecutor;
    private volatile URI me;
    private volatile Future<?> modeSwitcherFuture;
    private volatile HighAvailabilityMemberState currentTargetState;
    private final AtomicBoolean canAskForElections = new AtomicBoolean( true );

    public HighAvailabilityModeSwitcher( SwitchToSlave switchToSlave,
                                         SwitchToMaster switchToMaster,
                                         Election election,
                                         ClusterMemberAvailability clusterMemberAvailability,
                                         DependencyResolver dependencyResolver,
                                         InstanceId instanceId, Logging logging )
    {
        this.switchToSlave = switchToSlave;
        this.switchToMaster = switchToMaster;
        this.election = election;
        this.clusterMemberAvailability = clusterMemberAvailability;
        this.instanceId = instanceId;
        this.msgLog = logging.getMessagesLog( getClass() );
        this.consoleLog = logging.getConsoleLog( getClass() );
        this.haCommunicationLife = new LifeSupport();
        this.dependencyResolver = dependencyResolver;
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
            clusterMemberAvailability.memberIsAvailable( MASTER, masterHaURI, resolveStoreId() );
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
            clusterMemberAvailability.memberIsAvailable( SLAVE, slaveHaURI, resolveStoreId() );
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
                if ( event.getOldState().equals( HighAvailabilityMemberState.SLAVE ) )
                {
                    clusterMemberAvailability.memberIsUnavailable( SLAVE );
                }
                else if ( event.getOldState().equals( HighAvailabilityMemberState.MASTER ) )
                {
                    clusterMemberAvailability.memberIsUnavailable( MASTER );
                }

                switchToPending();
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
                // We just got scheduled. Maybe we are already obsolete - test
                if ( cancellationHandle.cancellationRequested() )
                {
                    msgLog.info( "Switch to master cancelled in the beginning of switching to master." );
                    return;
                }

                if ( currentTargetState != HighAvailabilityMemberState.TO_MASTER )
                {
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
                    msgLog.logMessage( "Failed to switch to master", e );
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
            msgLog.error( "I (" + me + ") tried to switch to slave for myself as master (" + availableMasterId + ")"  );
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

                try
                {
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
                    consoleLog.error( "UNABLE TO START UP AS SLAVE: " + e.getMessage() );
                    msgLog.error( "Unable to start up as slave", e );

                    clusterMemberAvailability.memberIsUnavailable( SLAVE );
                    ClusterClient clusterClient = dependencyResolver.resolveDependency( ClusterClient.class );
                    try
                    {
                        // TODO I doubt this actually works. Shuts down with no recovery or restart
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
                    msgLog.logMessage( "Error while trying to switch to slave", t );

                    // Try again later
                    wait.set( (1 + wait.get() * 2) ); // Exponential backoff
                    wait.set( Math.min( wait.get(), 5 * 60 ) ); // Wait maximum 5 minutes

                    modeSwitcherFuture = modeSwitcherExecutor.schedule( this, wait.get(), TimeUnit.SECONDS );

                    msgLog.logMessage( "Attempting to switch to slave in " + wait.get() + "s" );
                }
            }
        }, cancellationHandle );
    }

    private void switchToPending()
    {
        msgLog.logMessage( "I am " + instanceId + ", moving to pending" );

        startModeSwitching( new Runnable()
        {
            @Override
            public void run()
            {
                haCommunicationLife.shutdown();
                haCommunicationLife = new LifeSupport();
            }
        }, new CancellationHandle() );

        try
        {
            modeSwitcherFuture.get( 10, TimeUnit.SECONDS );
        }
        catch ( Exception ignored )
        {
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
            catch ( UnableToCopyStoreFromOldMasterException | InconsistentlyUpgradedClusterException |
                    UnavailableMembersException e )
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

    private StoreId resolveStoreId()
    {
        return dependencyResolver.resolveDependency( StoreId.class );
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
