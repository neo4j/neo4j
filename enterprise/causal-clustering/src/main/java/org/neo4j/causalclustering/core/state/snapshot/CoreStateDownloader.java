/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;

public class CoreStateDownloader
{
    private final LocalDatabase localDatabase;
    private final Lifecycle startStopOnStoreCopy;
    private final RemoteStore remoteStore;
    private final CatchUpClient catchUpClient;
    private final Log log;
    private final StoreCopyProcess storeCopyProcess;
    private final CoreStateMachines coreStateMachines;
    private final CoreSnapshotService snapshotService;
    private final TopologyService topologyService;

    public CoreStateDownloader( LocalDatabase localDatabase, Lifecycle startStopOnStoreCopy,
            RemoteStore remoteStore, CatchUpClient catchUpClient, LogProvider logProvider,
            StoreCopyProcess storeCopyProcess, CoreStateMachines coreStateMachines,
            CoreSnapshotService snapshotService, TopologyService topologyService )
    {
        this.localDatabase = localDatabase;
        this.startStopOnStoreCopy = startStopOnStoreCopy;
        this.remoteStore = remoteStore;
        this.catchUpClient = catchUpClient;
        this.log = logProvider.getLog( getClass() );
        this.storeCopyProcess = storeCopyProcess;
        this.coreStateMachines = coreStateMachines;
        this.snapshotService = snapshotService;
        this.topologyService = topologyService;
    }

    void downloadSnapshot( MemberId source ) throws StoreCopyFailedException
    {
        try
        {
            /* Extract some key properties before shutting it down. */
            boolean isEmptyStore = localDatabase.isEmpty();

            if ( !isEmptyStore )
            {
                /* make sure it's recovered before we start messing with catchup */
                localDatabase.start();
                localDatabase.stop();
            }

            AdvertisedSocketAddress fromAddress = topologyService.findCatchupAddress( source ).orElseThrow( () -> new TopologyLookupException( source ));
            StoreId remoteStoreId = remoteStore.getStoreId( fromAddress );
            if ( !isEmptyStore && !remoteStoreId.equals( localDatabase.storeId() ) )
            {
                throw new StoreCopyFailedException( "StoreId mismatch and not empty" );
            }

            startStopOnStoreCopy.stop();
            localDatabase.stopForStoreCopy();

            log.info( "Downloading snapshot from core server at %s", source );

            /* The core snapshot must be copied before the store, because the store has a dependency on
             * the state of the state machines. The store will thus be at or ahead of the state machines,
             * in consensus log index, and application of commands will bring them in sync. Any such commands
             * that carry transactions will thus be ignored by the transaction/token state machines, since they
             * are ahead, and the correct decisions for their applicability have already been taken as encapsulated
             * in the copied store. */

            CoreSnapshot coreSnapshot = catchUpClient.makeBlockingRequest( fromAddress, new CoreSnapshotRequest(),
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
                storeCopyProcess.replaceWithStoreFrom( fromAddress, remoteStoreId );
            }
            else
            {
                StoreId localStoreId = localDatabase.storeId();
                CatchupResult catchupResult = remoteStore.tryCatchingUp( fromAddress, localStoreId, localDatabase
                        .storeDir(), false );

                if ( catchupResult == E_TRANSACTION_PRUNED )
                {
                    log.info( format( "Failed to pull transactions from %s (%s). They may have been pruned away", source, fromAddress ) );
                    localDatabase.delete();

                    storeCopyProcess.replaceWithStoreFrom( fromAddress, localStoreId );
                }
                else if ( catchupResult != SUCCESS_END_OF_STREAM )
                {
                    throw new StoreCopyFailedException( "Failed to download store: " + catchupResult );
                }
            }

            /* We install the snapshot after the store has been downloaded,
             * so that we are not left with a state ahead of the store. */
            snapshotService.installSnapshot( coreSnapshot );
            log.info( "Core snapshot installed: " + coreSnapshot );

            /* Starting the database will invoke the commit process factory in
             * the EnterpriseCoreEditionModule, which has important side-effects. */
            log.info( "Starting local database" );
            localDatabase.start();
            coreStateMachines.installCommitProcess( localDatabase.getCommitProcess() );
            startStopOnStoreCopy.start();
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
