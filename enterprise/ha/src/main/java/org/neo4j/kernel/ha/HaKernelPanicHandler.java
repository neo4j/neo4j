/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;

public class HaKernelPanicHandler implements KernelEventHandler
{
    private final XaDataSourceManager dataSourceManager;
    private final TxManager txManager;
    private final AtomicInteger epoch = new AtomicInteger();
    private final InstanceAccessGuard accessGuard;

    public HaKernelPanicHandler( XaDataSourceManager dataSourceManager, TxManager txManager,
            InstanceAccessGuard accessGuard )
    {
        this.dataSourceManager = dataSourceManager;
        this.txManager = txManager;
        this.accessGuard = accessGuard;
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
                        return;
                    
                    accessGuard.enter();
                    try
                    {
                        txManager.stop();
                        dataSourceManager.stop();
                        dataSourceManager.start();
                        txManager.start();
                        txManager.doRecovery();
                        epoch.incrementAndGet();
                    }
                    finally
                    {
                        accessGuard.exit();
                    }
                }
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( "error while handling kernel panic for TX_MANAGER_NOT_OK", t );
            }
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
}
