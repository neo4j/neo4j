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
package org.neo4j.kernel.ha;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.CappedOperation;
import org.neo4j.kernel.impl.util.Condition;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static java.lang.System.currentTimeMillis;

public class UpdatePuller implements Lifecycle, TransactionObligationFulfiller
{
    private final HighAvailabilityMemberStateMachine memberStateMachine;
    private final Master master;
    private final RequestContextFactory requestContextFactory;
    private final AvailabilityGuard availabilityGuard;
    private final LastUpdateTime lastUpdateTime;
    private final Config config;
    private final JobScheduler scheduler;
    private final StringLogger logger;
    private final CappedOperation<Pair<String, ? extends Exception>> cappedLogger;
    private final UpdatePullerHighAvailabilityMemberListener listener;
    private UpdatePullingThread updatePuller;
    private TransactionIdStore transactionIdStore;
    private final DependencyResolver resolver;

    public UpdatePuller( HighAvailabilityMemberStateMachine memberStateMachine, Master master,
                         RequestContextFactory requestContextFactory, AvailabilityGuard availabilityGuard,
                         LastUpdateTime lastUpdateTime, Config config, JobScheduler scheduler, final StringLogger log,
                         DependencyResolver resolver )

    {
        this.memberStateMachine = memberStateMachine;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.availabilityGuard = availabilityGuard;
        this.lastUpdateTime = lastUpdateTime;
        this.config = config;
        this.scheduler = scheduler;
        this.logger = log;
        this.resolver = resolver;
        this.cappedLogger = new CappedOperation<Pair<String, ? extends Exception>>(
                CappedOperation.count( 10 ) )
        {
            @Override
            protected void triggered( Pair<String, ? extends Exception> event )
            {
                logger.warn( event.first(), event.other() );
            }
        };

        this.listener = new UpdatePullerHighAvailabilityMemberListener( config.get( ClusterSettings.server_id ) );
    }

    public void pullUpdates() throws InterruptedException
    {
        if ( !updatePuller.isActive() )
        {
            return;
        }

        final int ticket = updatePuller.poke();
        await( new Condition()
        {
            @Override
            public boolean evaluate()
            {
                return updatePuller.current() >= ticket;
            }
        } );
    }

    private void await( Condition condition ) throws InterruptedException
    {
        while ( !condition.evaluate() )
        {
            if ( !updatePuller.isActive() )
            {
                throw new InterruptedException( "Update puller has been halted:" + updatePuller );
            }

            Thread.sleep( 1 );
        }
    }

    /**
     * Triggers pulling of updates up until at least {@code toTxId} if no pulling is currently happening
     * and returns immediately.
     * @return {@link Future} which will block on {@link Future#get()} until {@code toTxId} has been applied.
     */
    @Override
    public void pullUpdates( final long toTxId ) throws InterruptedException
    {
        if ( !updatePuller.isActive() )
        {
            throw new IllegalStateException( "Update puller not active " + updatePuller );
        }

        Condition condition = new Condition()
        {
            @Override
            public boolean evaluate()
            {
                /**
                 * We need to await last *closed* transaction id, not last *committed* transaction id since
                 * right after leaving this method we might read records off of disk, and they had better
                 * be up to date, otherwise we read stale data.
                 */
                return transactionIdStore.getLastClosedTransactionId() >= toTxId;
            }
        };
        if ( condition.evaluate() )
        {   // We're already there
            return;
        }

        updatePuller.poke();
        await( condition );
    }

    private void startUpdatePuller()
    {
        updatePuller = new UpdatePullingThread( new UpdatePullingThread.Operation()
        {
            @Override
            public void perform()
            {
                doPullUpdates();
            }
        } );
        updatePuller.start();
    }

    private void doPullUpdates()
    {
        if ( availabilityGuard.isAvailable( 5000 ) )
        {
            try
            {
                RequestContext context = requestContextFactory.newRequestContext();
                master.pullUpdates( context );
            }
            catch ( ComException e )
            {
                cappedLogger.event( Pair.of( "Pull updates failed due to network error.", e ) );
            }
            catch ( Exception e )
            {
                logger.error( "Pull updates failed", e );
            }
            lastUpdateTime.setLastUpdateTime( currentTimeMillis() );
        }
    }

    @Override
    public void init() throws Throwable
    {
        startUpdatePuller();
        startIntervalTrigger();
    }

    private void startIntervalTrigger()
    {
        long pullInterval = config.get( HaSettings.pull_interval );
        if ( pullInterval > 0 )
        {
            scheduler.scheduleRecurring( JobScheduler.Group.pullUpdates, new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        pullUpdates();
                    }
                    catch ( InterruptedException e )
                    {
                        logger.error( "Pull updates failed", e );
                    }
                }
            }, pullInterval, pullInterval, TimeUnit.MILLISECONDS );
        }
    }

    @Override
    public void start() throws Throwable
    {
        memberStateMachine.addHighAvailabilityMemberListener( listener );
    }

    @Override
    public void stop() throws Throwable
    {
        updatePuller.pause( true );
        memberStateMachine.removeHighAvailabilityMemberListener( listener );
    }

    @Override
    public void shutdown() throws Throwable
    {
        updatePuller.halt();
        while ( updatePuller.getState() != Thread.State.TERMINATED )
        {
            Thread.sleep( 1 );
            Thread.yield();
        }
    }

    private class UpdatePullerHighAvailabilityMemberListener extends HighAvailabilityMemberListener.Adapter
    {
        private final InstanceId myInstanceId;

        private UpdatePullerHighAvailabilityMemberListener( InstanceId myInstanceId )
        {
            this.myInstanceId = myInstanceId;
        }

        @Override
        public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            if ( event.getInstanceId().equals( myInstanceId ) )
            {
                // I'm the master, no need to pull updates
                updatePuller.pause( true );
            }
        }

        @Override
        public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            if ( event.getInstanceId().equals( myInstanceId ) )
            {
                // I'm a slave, let the transactions stream in

                // Pull out the transaction id store at this very moment, because we receive this event
                // when joining a cluster or switching to a new master and there might have been a store copy
                // just now where there has been a new transaction id store created.
                transactionIdStore = resolver.resolveDependency( TransactionIdStore.class );
                updatePuller.pause( false );
            }
        }
    }
}
