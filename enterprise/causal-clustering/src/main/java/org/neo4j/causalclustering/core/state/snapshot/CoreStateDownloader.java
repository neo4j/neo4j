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
package org.neo4j.causalclustering.core.state.snapshot;

import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreFetcher;
import org.neo4j.causalclustering.core.state.CoreState;
import org.neo4j.causalclustering.readreplica.CopyStoreSafely;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS;

public class CoreStateDownloader
{
    private final FileSystemAbstraction fs;
    private final LocalDatabase localDatabase;
    private final Lifecycle startStopOnStoreCopy;
    private final StoreFetcher storeFetcher;
    private final CatchUpClient catchUpClient;
    private final Log log;
    private final CopiedStoreRecovery copiedStoreRecovery;

    public CoreStateDownloader( FileSystemAbstraction fs, LocalDatabase localDatabase, Lifecycle startStopOnStoreCopy,
            StoreFetcher storeFetcher, CatchUpClient catchUpClient, LogProvider logProvider,
            CopiedStoreRecovery copiedStoreRecovery )
    {
        this.fs = fs;
        this.localDatabase = localDatabase;
        this.startStopOnStoreCopy = startStopOnStoreCopy;
        this.storeFetcher = storeFetcher;
        this.catchUpClient = catchUpClient;
        this.log = logProvider.getLog( getClass() );
        this.copiedStoreRecovery = copiedStoreRecovery;
    }

    public synchronized void downloadSnapshot( MemberId source, CoreState coreState ) throws StoreCopyFailedException
    {
        // TODO: Think about recovery scenarios.
        try
        {
            /* Extract some key properties before shutting it down. */
            boolean isEmptyStore = localDatabase.isEmpty();
            StoreId localStoreId = localDatabase.storeId();

            StoreId remoteStoreId = storeFetcher.getStoreIdOf( source );
            if ( !isEmptyStore && !remoteStoreId.equals( localStoreId ) )
            {
                throw new StoreCopyFailedException( "StoreId mismatch and not empty" );
            }

            startStopOnStoreCopy.stop();
            localDatabase.stop();

            log.info( "Downloading snapshot from core server at %s", source );

            /* The core snapshot must be copied before the store, because the store has a dependency on
             * the state of the state machines. The store will thus be at or ahead of the state machines,
             * in consensus log index, and application of commands will bring them in sync. Any such commands
             * that carry transactions will thus be ignored by the transaction/token state machines, since they
             * are ahead, and the correct decisions for their applicability have already been taken as encapsulated
             * in the copied store. */

            CoreSnapshot coreSnapshot = catchUpClient.makeBlockingRequest( source, new CoreSnapshotRequest(),
                    new CatchUpResponseAdaptor<CoreSnapshot>()
                    {
                        @Override
                        public void onCoreSnapshot( CompletableFuture<CoreSnapshot> signal, CoreSnapshot response )
                        {
                            signal.complete( response );
                        }
                    } );

            if ( isEmptyStore )
            {
                new CopyStoreSafely( fs, localDatabase, copiedStoreRecovery, log ).
                        copyWholeStoreFrom( source, remoteStoreId, storeFetcher );
            }
            else
            {
                CatchupResult catchupResult =
                        storeFetcher.tryCatchingUp( source, localStoreId, localDatabase.storeDir() );

                if ( catchupResult == E_TRANSACTION_PRUNED )
                {
                    localDatabase.delete();
                    new CopyStoreSafely( fs, localDatabase, copiedStoreRecovery, log ).
                        copyWholeStoreFrom( source, localStoreId, storeFetcher );
                }
                else if ( catchupResult != SUCCESS )
                {
                    throw new StoreCopyFailedException( "Failed to download store: " + catchupResult );
                }
            }

            /* We install the snapshot after the store has been downloaded,
             * so that we are not left with a state ahead of the store. */
            coreState.installSnapshot( coreSnapshot );
            log.info( "Core snapshot installed: " + coreSnapshot );

            /* Starting the database will invoke the commit process factory in
             * the EnterpriseCoreEditionModule, which has important side-effects. */
            log.info( "Restarting local database", source );
            localDatabase.start();
            startStopOnStoreCopy.start();
            log.info( "Local database started", source );
        }
        catch ( StoreCopyFailedException e )
        {
            throw e;
        }
        catch ( Throwable e )
        {
            throw new StoreCopyFailedException( e );
        }
    }
}
