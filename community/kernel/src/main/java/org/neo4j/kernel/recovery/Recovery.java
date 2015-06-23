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
package org.neo4j.kernel.recovery;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
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
        void recoveryRequired( LogPosition recoveryPosition );

        void logRecovered( LogPosition endPosition );

        void recoveryCompleted();
    }

    public interface SPI
    {
        void forceEverything();

        Visitor<LogVersionedStoreChannel,IOException> getRecoverer();

        Iterator<LogVersionedStoreChannel> getLogFiles( long fromVersion ) throws IOException;

        LogPosition getPositionToRecoverFrom() throws IOException;
    }

    private final SPI spi;
    private final Monitor monitor;

    private boolean recoveredLog = false;

    public Recovery( SPI spi, Monitor monitor )
    {
        this.spi = spi;
        this.monitor = monitor;
    }

    @Override
    public void init() throws Throwable
    {
        LogPosition recoveryPosition = spi.getPositionToRecoverFrom();
        if ( LogPosition.UNSPECIFIED.equals( recoveryPosition ) )
        {
            return;
        }
        Iterator<LogVersionedStoreChannel> logFiles = spi.getLogFiles( recoveryPosition.getLogVersion() );
        monitor.recoveryRequired( recoveryPosition );
        Visitor<LogVersionedStoreChannel,IOException> recoverer = spi.getRecoverer();
        while ( logFiles.hasNext() )
        {
            try ( LogVersionedStoreChannel toRecover = logFiles.next() )
            {
                toRecover.position( recoveryPosition.getByteOffset() );
                recoverer.visit( toRecover );
                recoveryPosition = LogPosition.start( recoveryPosition.getLogVersion() + 1 );
            }
        }
        recoveredLog = true;
        monitor.logRecovered( recoveryPosition );
        spi.forceEverything();
    }

    @Override
    public void start() throws Throwable
    {
        // This is here as by now all other services have reacted to the recovery process
        if ( recoveredLog )
        {
            monitor.recoveryCompleted();
        }
    }
}
