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

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

public class HaKernelPanicHandler implements KernelEventHandler, AvailabilityGuard.AvailabilityRequirement
{
    private final XaDataSourceManager dataSourceManager;
    private final TxManager txManager;
    private final AtomicInteger epoch = new AtomicInteger();
    private final AvailabilityGuard availabilityGuard;
    private final DelegateInvocationHandler<Master> masterDelegateInvocationHandler;
    private final StringLogger logger;

    public HaKernelPanicHandler( XaDataSourceManager dataSourceManager, TxManager txManager,
                                 AvailabilityGuard availabilityGuard, Logging logging,
                                 DelegateInvocationHandler<Master> masterDelegateInvocationHandler )
    {
        this.dataSourceManager = dataSourceManager;
        this.txManager = txManager;
        this.availabilityGuard = availabilityGuard;
        this.logger = logging.getMessagesLog( getClass() );
        this.masterDelegateInvocationHandler = masterDelegateInvocationHandler;
        availabilityGuard.grant(this);
    }

    @Override
    public void beforeShutdown()
    {
    }

    @Override
    public void kernelPanic( ErrorState error )
    {
        if ( error == ErrorState.TX_MANAGER_NOT_OK )
        {
            try
            {
                int myEpoch = epoch.get();
                synchronized ( dataSourceManager )
                {
                    if ( myEpoch != epoch.get() )
                    {
                        return;
                    }

                    logger.info( "Recovering from HA kernel panic" );
                    epoch.incrementAndGet();
                    availabilityGuard.deny(this);
                    try
                    {
                        txManager.stop();
                        dataSourceManager.stop();
                        dataSourceManager.start();
                        txManager.start();
                        txManager.doRecovery();
                        masterDelegateInvocationHandler.harden();
                    }
                    finally
                    {
                        availabilityGuard.grant(this);
                        logger.info( "Done recovering from HA kernel panic" );
                    }
                }
            }
            catch ( Throwable t )
            {
                String msg = "Error while handling HA kernel panic";
                logger.warn( msg, t );
                throw new RuntimeException( msg, t );
            }
        }
        else if ( error == ErrorState.STORAGE_MEDIA_FULL )
        {
            // Fatal error - Permanently unavailable
            availabilityGuard.shutdown();
        }
    }

    @Override
    public Object getResource()
    {
        return null;
    }

    @Override
    public ExecutionOrder orderComparedTo( KernelEventHandler other )
    {
        return ExecutionOrder.DOESNT_MATTER;
    }

    @Override
    public String description()
    {
        return getClass().getSimpleName();
    }
}
