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
package org.neo4j.coreedge.core.state.snapshot;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.catchup.CatchUpClient;
import org.neo4j.coreedge.catchup.CatchUpResponseAdaptor;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.catchup.storecopy.StoreFetcher;
import org.neo4j.coreedge.core.state.CoreState;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MINUTES;

public class CoreStateDownloader
{
    private final LocalDatabase localDatabase;
    private final StoreFetcher storeFetcher;
    private final CatchUpClient catchUpClient;
    private final Log log;

    public CoreStateDownloader( LocalDatabase localDatabase, StoreFetcher storeFetcher, CatchUpClient catchUpClient,
                                LogProvider logProvider )
    {
        this.localDatabase = localDatabase;
        this.storeFetcher = storeFetcher;
        this.catchUpClient = catchUpClient;
        this.log = logProvider.getLog( getClass() );
    }

    public synchronized void downloadSnapshot( MemberId source, StoreId storeId, CoreState coreState )
            throws InterruptedException, StoreCopyFailedException
    {
        try
        {
            localDatabase.stop();
            log.info( "Downloading snapshot from core server at %s", source );

            /* The core snapshot must be copied before the store, because the store has a dependency on
             * the state of the state machines. The store will thus be at or ahead of the state machines,
             * in consensus log index, and application of commands will bring them in sync. Any such commands
             * that carry transactions will thus be ignored by the transaction/token state machines, since they
             * are ahead, and the correct decisions for their applicability have already been taken as encapsulated
             * in the copied store. */

            CoreSnapshot coreSnapshot = catchUpClient.makeBlockingRequest( source, new CoreSnapshotRequest(),
                    1, MINUTES, new CatchUpResponseAdaptor<CoreSnapshot>()
                    {
                        @Override
                        public void onCoreSnapshot( CompletableFuture<CoreSnapshot> signal, CoreSnapshot response )
                        {
                            signal.complete( response );
                        }
                    } );

            localDatabase.bringUpToDateOrReplaceStoreFrom( source, storeId, storeFetcher ); // this deletes the current store
            log.info( "Replaced store with one downloaded from %s", source );

            /* We install the snapshot after the store has been downloaded,
             * so that we are not left with a state ahead of the store. */
            coreState.installSnapshot( coreSnapshot );
            log.info( "Core snapshot installed: " + coreSnapshot );

            /* Starting the database will invoke the commit process factory in
             * the EnterpriseCoreEditionModule, which has important side-effects. */
            log.info( "Restarting local database", source );
            localDatabase.start();
            log.info( "Restarted local database", source );
        }
        catch ( Throwable e )
        {
            localDatabase.panic( e );
            throw new StoreCopyFailedException( e );
        }
    }
}
