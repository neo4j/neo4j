/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.com.ComException;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.util.CappedOperation;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class UpdatePuller implements Lifecycle
{
    private final HighAvailabilityMemberStateMachine memberStateMachine;
    private final HaXaDataSourceManager xaDataSourceManager;
    private final Master master;
    private final RequestContextFactory requestContextFactory;
    private final AbstractTransactionManager txManager;
    private final AvailabilityGuard availabilityGuard;
    private final LastUpdateTime lastUpdateTime;
    private final Config config;
    private final JobScheduler scheduler;
    private final StringLogger logger;
    private final CappedOperation<Pair<String, ? extends Exception>> cappedLogger;
    private volatile boolean pullUpdates = false;
    private final UpdatePullerHighAvailabilityMemberListener listener;

    public UpdatePuller( HighAvailabilityMemberStateMachine memberStateMachine, HaXaDataSourceManager xaDataSourceManager, Master master,
                         RequestContextFactory requestContextFactory, AbstractTransactionManager txManager,
                         AvailabilityGuard availabilityGuard, LastUpdateTime lastUpdateTime, Config config,
                         JobScheduler scheduler, final StringLogger logger )
    {
        this.memberStateMachine = memberStateMachine;
        this.xaDataSourceManager = xaDataSourceManager;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.txManager = txManager;
        this.availabilityGuard = availabilityGuard;
        this.lastUpdateTime = lastUpdateTime;
        this.config = config;
        this.scheduler = scheduler;
        this.logger = logger;
        this.cappedLogger = new CappedOperation<Pair<String, ? extends Exception>>(
                CappedOperation.count( 10 ))
        {
            @Override
            protected void triggered( Pair<String, ? extends Exception> event )
            {
                logger.warn( event.first(), event.other() );
            }
        };

        listener = new UpdatePullerHighAvailabilityMemberListener( config.get( ClusterSettings.server_id ) );
    }

    public void pullUpdates()
    {
        if ( availabilityGuard.isAvailable( 5000 ) )
        {
            xaDataSourceManager.applyTransactions(
                    master.pullUpdates( requestContextFactory.newRequestContext( txManager.getEventIdentifier() ) ) );
        }
        lastUpdateTime.setLastUpdateTime( System.currentTimeMillis() );
    }

    @Override
    public void init() throws Throwable
    {
        long pullInterval = config.get( HaSettings.pull_interval );
        if ( pullInterval > 0 )
        {
            scheduler.scheduleRecurring( JobScheduler.Group.pullUpdates, new Runnable()
            {
                @Override
                public void run()
                {
                    if ( !pullUpdates )
                    {
                        return;
                    }
                    try
                    {
                        pullUpdates();
                    }
                    catch ( ComException e )
                    {
                        cappedLogger.event( Pair.of( "Pull updates failed due to network error.", e ) );
                    }
                    catch ( Exception e )
                    {
                        logger.logMessage( "Pull updates failed", e );
                    }
                }
            }, pullInterval, pullInterval, TimeUnit.MILLISECONDS );
        }
        this.pullUpdates = false;
    }

    @Override
    public void start() throws Throwable
    {
        this.pullUpdates = true;
        memberStateMachine.addHighAvailabilityMemberListener( listener );
    }

    @Override
    public void stop() throws Throwable
    {
        this.pullUpdates = false;
        memberStateMachine.removeHighAvailabilityMemberListener( listener );
    }

    @Override
    public void shutdown() throws Throwable
    {
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
                pullUpdates = false;
            }
        }

        @Override
        public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            if ( event.getInstanceId().equals( myInstanceId ) )
            {
                pullUpdates = true;
            }
        }
    }
}
