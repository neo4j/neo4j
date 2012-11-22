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

package org.neo4j.kernel.ha;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.neo4j.com.ComException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class UpdatePuller implements Lifecycle
{
    private final HaXaDataSourceManager xaDataSourceManager;
    private final Master master;
    private final RequestContextFactory requestContextFactory;
    private final AbstractTransactionManager txManager;
    private final InstanceAccessGuard accessGuard;
    private final LastUpdateTime lastUpdateTime;
    private final Config config;
    private final StringLogger logger;
    private boolean pullUpdates = false;
    private ScheduledThreadPoolExecutor updatePuller;

    public UpdatePuller( HaXaDataSourceManager xaDataSourceManager, Master master,
                         RequestContextFactory requestContextFactory, AbstractTransactionManager txManager,
                         InstanceAccessGuard accessGuard, LastUpdateTime lastUpdateTime, Config config, StringLogger logger )
    {
        this.xaDataSourceManager = xaDataSourceManager;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.txManager = txManager;
        this.accessGuard = accessGuard;
        this.lastUpdateTime = lastUpdateTime;
        this.config = config;
        this.logger = logger;
    }

    public void pullUpdates()
    {
        if ( accessGuard.await( 5000 ) )
        {
            xaDataSourceManager.applyTransactions(
                    master.pullUpdates( requestContextFactory.newRequestContext( txManager.getEventIdentifier() ) ) );
            lastUpdateTime.setLastUpdateTime( System.currentTimeMillis() );
        }
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
                        // Ignore
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
