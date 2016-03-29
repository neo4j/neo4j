/*
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
package org.neo4j.coreedge.server.core;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.catchup.storecopy.edge.state.StateFetcher;
import org.neo4j.coreedge.discovery.CoreServerSelectionException;
import org.neo4j.coreedge.raft.RaftStateMachine;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogCompactedException;
import org.neo4j.coreedge.raft.replication.tx.ConstantTimeRetryStrategy;
import org.neo4j.coreedge.raft.replication.tx.RetryStrategy;
import org.neo4j.coreedge.raft.state.StateMachineApplier;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.edge.CoreServerSelectionStrategy;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CoreStateMachine extends LifecycleAdapter implements RaftStateMachine
{
    private final StateMachineApplier stateMachineApplier;
    private final LocalDatabase localDatabase;
    private final CoreServerSelectionStrategy selectionStrategy;
    private final StoreFetcher storeFetcher;
    private final StateFetcher stateFetcher;
    private final RaftLog raftLog;
    private final Log log;

    public CoreStateMachine(
            StateMachineApplier stateMachineApplier,
            LocalDatabase localDatabase,
            CoreServerSelectionStrategy selectionStrategy,
            StoreFetcher storeFetcher,
            StateFetcher stateFetcher,
            LogProvider logProvider,
            RaftLog raftLog )
    {
        this.stateMachineApplier = stateMachineApplier;
        this.localDatabase = localDatabase;
        this.selectionStrategy = selectionStrategy;
        this.storeFetcher = storeFetcher;
        this.stateFetcher = stateFetcher;
        this.log = logProvider.getLog( getClass() );
        this.raftLog = raftLog;
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void notifyCommitted( long commitIndex )
    {
        stateMachineApplier.notifyCommitted( commitIndex );
    }

    public void compact()
    {
        try
        {
            raftLog.prune( stateMachineApplier.lastFlushed() );
        }
        catch ( IOException e )
        {
            // TODO panic?
            throw new RuntimeException( e );
        }
        catch ( RaftLogCompactedException e )
        {
            log.warn( "Log already pruned?", e );
        }
    }

    @Override
    public synchronized void downloadSnapshot()
    {
        try
        {
            downloadSnapshot( selectionStrategy.coreServer() );
        }
        catch ( CoreServerSelectionException e )
        {
            log.error( "Failed to download snapshot", e );
        }
    }

    public synchronized void downloadSnapshot( AdvertisedSocketAddress source )
    {
        RetryStrategy.Timeout timeout = new ConstantTimeRetryStrategy( 10, TimeUnit.SECONDS ).newTimeout();

        localDatabase.stop();
        while ( true )
        {
            try
            {
                performDownload( source );
                localDatabase.start();
                break;
            }
            catch ( CoreServerSelectionException | StoreCopyFailedException ex )
            {
                ex.printStackTrace();
                log.info( ex.getMessage() + ", retrying in %d ms.", timeout.getMillis() );
                try
                {
                    Thread.sleep( timeout.getMillis() );
                    timeout.increment();
                }
                catch ( InterruptedException e )
                {
                    log.warn( "Snapshot download interrupted" );
                    break;
                }
            }
            catch ( IOException e )
            {
                localDatabase.panic( e );
                break;
            }
        }
    }

    private void performDownload( AdvertisedSocketAddress source ) throws CoreServerSelectionException, StoreCopyFailedException
    {
        log.info( "Server starting, connecting to core server at %s", source.toString() );

        localDatabase.copyStoreFrom( source, storeFetcher );
        stateFetcher.copyRaftState( source, stateMachineApplier.get() );
    }
}
