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

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;

public class HaRecovery implements AvailabilityGuard.AvailabilityRequirement
{
    private final XaDataSourceManager dataSourceManager;
    private final StringLogger logger;
    private final AvailabilityGuard availabilityGuard;
    private final AbstractTransactionManager txManager;
    private final DelegateInvocationHandler<Master> masterDelegateInvocationHandler;

    private final AtomicInteger epoch = new AtomicInteger();

    public HaRecovery( XaDataSourceManager dataSourceManager, StringLogger logger, AvailabilityGuard availabilityGuard,
                       AbstractTransactionManager txManager, DelegateInvocationHandler<Master> masterDelegateInvocationHandler )
    {
        this.dataSourceManager = dataSourceManager;
        this.logger = logger;
        this.availabilityGuard = availabilityGuard;
        this.txManager = txManager;
        this.masterDelegateInvocationHandler = masterDelegateInvocationHandler;

        availabilityGuard.grant(this);
    }

    public void recover()
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
                availabilityGuard.deny( this );
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
                    availabilityGuard.grant( this );
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

    @Override
    public String description()
    {
        return getClass().getSimpleName();
    }
}
