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
package org.neo4j.kernel.ha;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.neo4j.com.ComException;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.util.CappedOperation;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class UpdatePuller implements Lifecycle
{
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
    private boolean pullUpdates = false;
    private ScheduledThreadPoolExecutor updatePuller;

    public UpdatePuller( HaXaDataSourceManager xaDataSourceManager, Master master,
                         RequestContextFactory requestContextFactory, AbstractTransactionManager txManager,
                         AvailabilityGuard availabilityGuard, LastUpdateTime lastUpdateTime, Config config,
                         JobScheduler scheduler, final StringLogger logger )
    {
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
        if ( pullInterval > 0 && updatePuller == null )
        {
            updatePuller = new ScheduledThreadPoolExecutor( 1 );
            updatePuller.scheduleWithFixedDelay( new Runnable()
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
    }

    @Override
    public void stop() throws Throwable
    {
        this.pullUpdates = false;
    }

    @Override
    public void shutdown() throws Throwable
    {
        if ( updatePuller != null )
        {
            try
            {
                /*
                * Be gentle, interrupting running threads could leave the
                * file channels in a bad shape.
                */
                this.updatePuller.shutdown();
                this.updatePuller.awaitTermination( 5, TimeUnit.SECONDS );
            }
            catch ( InterruptedException e )
            {
                logger.logMessage(
                        "Got exception while waiting for update puller termination", e, true );
            }
        }
    }
}
