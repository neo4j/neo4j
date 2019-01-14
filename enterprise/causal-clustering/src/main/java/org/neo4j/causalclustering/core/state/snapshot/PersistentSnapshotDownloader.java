/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.snapshot;

import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.Log;

class PersistentSnapshotDownloader implements Runnable
{
    static final String OPERATION_NAME = "download of snapshot";

    private final CommandApplicationProcess applicationProcess;
    private final CatchupAddressProvider addressProvider;
    private final CoreStateDownloader downloader;
    private final Log log;
    private final TimeoutStrategy.Timeout timeout;
    private final Supplier<DatabaseHealth> dbHealth;
    private volatile State state;
    private volatile boolean keepRunning;

    PersistentSnapshotDownloader( CatchupAddressProvider addressProvider, CommandApplicationProcess applicationProcess,
            CoreStateDownloader downloader, Log log, TimeoutStrategy.Timeout pauseStrategy, Supplier<DatabaseHealth> dbHealth )
    {
        this.applicationProcess = applicationProcess;
        this.addressProvider = addressProvider;
        this.downloader = downloader;
        this.log = log;
        this.timeout = pauseStrategy;
        this.dbHealth = dbHealth;
        this.state = State.INITIATED;
        this.keepRunning = true;
    }

    private enum State
    {
        INITIATED,
        RUNNING,
        COMPLETED
    }

    @Override
    public void run()
    {
        if ( !moveToRunningState() )
        {
            return;
        }

        try
        {
            applicationProcess.pauseApplier( OPERATION_NAME );
            while ( keepRunning && !downloader.downloadSnapshot( addressProvider ) )
            {
                Thread.sleep( timeout.getMillis() );
                timeout.increment();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            log.warn( "Persistent snapshot downloader was interrupted" );
        }
        catch ( DatabaseShutdownException e )
        {
            log.warn( "Store copy aborted due to shut down", e );
        }
        catch ( Throwable e )
        {
            log.error( "Unrecoverable error during store copy", e );
            dbHealth.get().panic( e );
        }
        finally
        {
            applicationProcess.resumeApplier( OPERATION_NAME );
            state = State.COMPLETED;
        }
    }

    private synchronized boolean moveToRunningState()
    {
        if ( state != State.INITIATED )
        {
            return false;
        }
        else
        {
            state = State.RUNNING;
            return true;
        }
    }

    void stop() throws InterruptedException
    {
        this.keepRunning = false;

        while ( !hasCompleted() )
        {
            Thread.sleep( 100 );
        }
    }

    boolean hasCompleted()
    {
        return state == State.COMPLETED;
    }
}
