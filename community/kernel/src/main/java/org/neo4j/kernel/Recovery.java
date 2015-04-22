/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.log.LogRecoveryCheck;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * This is the process of doing a recovery on the transaction log and store, and is executed
 * at startup of {@link org.neo4j.kernel.NeoStoreDataSource}.
 */
public class Recovery extends LifecycleAdapter
{
    public interface Monitor
    {
        void recoveryRequired( long recoveredLogVersion );

        void logRecovered();

        void recoveryCompleted();
    }

    public interface SPI
    {
        void forceEverything();
        long getCurrentLogVersion();
        Visitor<LogVersionedStoreChannel, IOException> getRecoverer();
        LogVersionedStoreChannel getLogFile( long recoveryVersion ) throws IOException;
    }

    private final SPI spi;
    private final Monitor monitor;

    private boolean recoveredLog = false;

    public Recovery(SPI spi, Monitor monitor)
    {
        this.spi = spi;
        this.monitor = monitor;
    }

    @Override
    public void init() throws Throwable
    {
        long recoveryVersion = spi.getCurrentLogVersion();
        try ( LogVersionedStoreChannel toRecover = spi.getLogFile( recoveryVersion ))
        {
            if ( LogRecoveryCheck.recoveryRequired( toRecover ) )
            {   // There's already data in here, which means recovery will need to be performed.
                monitor.recoveryRequired( toRecover.getVersion() );
                spi.getRecoverer().visit( toRecover );
                recoveredLog = true;
                monitor.logRecovered();

                // intentionally keep it open since we're continuing using the underlying channel for the writer below
                spi.forceEverything();
            }
        }
    }

    @Override
    public void start() throws Throwable
    {
        // This is here as by now all other services have reacted to the recovery process
        if (recoveredLog)
            monitor.recoveryCompleted();
    }
}
