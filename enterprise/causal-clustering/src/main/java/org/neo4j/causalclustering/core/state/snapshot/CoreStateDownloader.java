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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.CommitStateHelper;
import org.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import org.neo4j.causalclustering.helper.Suspendable;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;

public class CoreStateDownloader
{
    private final LocalDatabase localDatabase;
    private final Suspendable suspendOnStoreCopy;
    private final RemoteStore remoteStore;
    private final CatchUpClient catchUpClient;
    private final Log log;
    private final StoreCopyProcess storeCopyProcess;
    private final CoreStateMachines coreStateMachines;
    private final CoreSnapshotService snapshotService;
    private CommitStateHelper commitStateHelper;

    public CoreStateDownloader( LocalDatabase localDatabase, Suspendable suspendOnStoreCopy, RemoteStore remoteStore,
                                CatchUpClient catchUpClient, LogProvider logProvider, StoreCopyProcess storeCopyProcess,
                                CoreStateMachines coreStateMachines, CoreSnapshotService snapshotService,
                                CommitStateHelper commitStateHelper )
    {
        this.localDatabase = localDatabase;
        this.suspendOnStoreCopy = suspendOnStoreCopy;
        this.remoteStore = remoteStore;
        this.catchUpClient = catchUpClient;
        this.log = logProvider.getLog( getClass() );
        this.storeCopyProcess = storeCopyProcess;
        this.coreStateMachines = coreStateMachines;
        this.snapshotService = snapshotService;
        this.commitStateHelper = commitStateHelper;
    }

    /**
     * Tries to catchup this instance by downloading a snapshot. A snapshot consists of both the
     * comparatively small state of the cluster state machines as well as the database store. The
     * store is however caught up using two different approach. If it is possible to catchup by
     * pulling transactions, then this will be sufficient, but if the store is lagging too far
     * behind then a complete store copy will be attempted.
     *
     * @param addressProvider Provider of addresses to catchup from.
     * @return True if the operation succeeded, and false otherwise.
     * @throws LifecycleException A major database component failed to start or stop.
     * @throws IOException An issue with I/O.
     * @throws DatabaseShutdownException The database is shutting down.
     */
    boolean downloadSnapshot( CatchupAddressProvider addressProvider )
            throws LifecycleException, IOException, DatabaseShutdownException
    {
        /* Extract some key properties before shutting it down. */
        boolean isEmptyStore = localDatabase.isEmpty();

        /*
         *  There is no reason to try to recover if there are no transaction logs and in fact it is
         *  also problematic for the initial transaction pull during the snapshot download because the
         *  kernel will create a transaction log with a header where previous index points to the same
         *  index as that written down into the metadata store. This is problematic because we have no
         *  guarantee that there are later transactions and we need at least one transaction in
         *  the log to figure out the Raft log index (see {@link RecoverConsensusLogIndex}).
         */
        if ( commitStateHelper.hasTxLogs( localDatabase.storeDir() ) )
        {
            log.info( "Recovering local database" );
            ensure( localDatabase::start, "start local database" );
            ensure( localDatabase::stop, "stop local database" );
        }

        AdvertisedSocketAddress primary;
        StoreId remoteStoreId;
        try
        {
            primary = addressProvider.primary();
            remoteStoreId = remoteStore.getStoreId( primary );
        }
        catch ( CatchupAddressResolutionException | StoreIdDownloadFailedException e )
        {
            log.warn( "Store copy failed", e );
            return false;
        }

        if ( !isEmptyStore && !remoteStoreId.equals( localDatabase.storeId() ) )
        {
            log.error( "Store copy failed due to store ID mismatch" );
            return false;
        }

        ensure( suspendOnStoreCopy::disable, "disable auxiliary services before store copy" );
        ensure( localDatabase::stopForStoreCopy, "stop local database for store copy" );

        log.info( "Downloading snapshot from core server at %s", primary );

        /* The core snapshot must be copied before the store, because the store has a dependency on
         * the state of the state machines. The store will thus be at or ahead of the state machines,
         * in consensus log index, and application of commands will bring them in sync. Any such commands
         * that carry transactions will thus be ignored by the transaction/token state machines, since they
         * are ahead, and the correct decisions for their applicability have already been taken as encapsulated
         * in the copied store. */

        CoreSnapshot coreSnapshot;
        try
        {
            coreSnapshot = catchUpClient.makeBlockingRequest( primary, new CoreSnapshotRequest(),
                    new CatchUpResponseAdaptor<CoreSnapshot>()
                    {
                        @Override
                        public void onCoreSnapshot( CompletableFuture<CoreSnapshot> signal, CoreSnapshot response )
                        {
                            signal.complete( response );
                        }
                    } );
        }
        catch ( CatchUpClientException e )
        {
            log.warn( "Store copy failed", e );
            return false;
        }

        if ( !isEmptyStore )
        {
            StoreId localStoreId = localDatabase.storeId();
            CatchupResult catchupResult;
            try
            {
                catchupResult = remoteStore.tryCatchingUp( primary, localStoreId, localDatabase.storeDir(), false, false );
            }
            catch ( StoreCopyFailedException e )
            {
                log.warn( "Failed to catch up", e );
                return false;
            }

            if ( catchupResult == E_TRANSACTION_PRUNED )
            {
                log.warn( format( "Failed to pull transactions from (%s). They may have been pruned away", primary ) );
                localDatabase.delete();
                isEmptyStore = true;
            }
            else if ( catchupResult != SUCCESS_END_OF_STREAM )
            {
                log.warn( format( "Unexpected catchup operation result %s from %s", catchupResult, primary ) );
                return false;
            }
        }

        if ( isEmptyStore )
        {
            try
            {
                storeCopyProcess.replaceWithStoreFrom( addressProvider, remoteStoreId );
            }
            catch ( StoreCopyFailedException e )
            {
                log.warn( "Failed to copy and replace store", e );
                return false;
            }
        }

        /* We install the snapshot after the store has been downloaded,
         * so that we are not left with a state ahead of the store. */
        snapshotService.installSnapshot( coreSnapshot );
        log.info( "Core snapshot installed: " + coreSnapshot );

        /* Starting the database will invoke the commit process factory in
         * the EnterpriseCoreEditionModule, which has important side-effects. */
        log.info( "Starting local database" );
        ensure( localDatabase::start, "start local database after store copy" );

        coreStateMachines.installCommitProcess( localDatabase.getCommitProcess() );
        ensure( suspendOnStoreCopy::enable, "enable auxiliary services after store copy" );

        return true;
    }

    public interface LifecycleAction
    {
        void perform() throws Throwable;
    }

    private static void ensure( LifecycleAction action, String operation )
    {
        try
        {
            action.perform();
        }
        catch ( Throwable cause )
        {
            throw new LifecycleException( "Failed to " + operation, cause );
        }
    }
}
