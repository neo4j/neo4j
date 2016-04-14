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
package org.neo4j.coreedge.raft.state;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.catchup.storecopy.CoreClient;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MINUTES;

public class CoreStateDownloader
{
    private final LocalDatabase localDatabase;
    private final StoreFetcher storeFetcher;
    private final CoreClient coreClient;
    private final Log log;

    public CoreStateDownloader( LocalDatabase localDatabase, StoreFetcher storeFetcher, CoreClient coreClient, LogProvider logProvider )
    {
        this.localDatabase = localDatabase;
        this.storeFetcher = storeFetcher;
        this.coreClient = coreClient;
        this.log = logProvider.getLog( getClass() );
    }

    synchronized void downloadSnapshot( AdvertisedSocketAddress source, CoreState coreState ) throws InterruptedException, StoreCopyFailedException
    {
        localDatabase.stop();

        try
        {
            log.info( "Downloading snapshot from core server at %s", source );

            /* The core snapshot must be copied before the store, because the store has a dependency on
             * the state of the state machines. The store will thus be at or ahead of the state machines,
             * in consensus log index, and application of commands will bring them in sync. Any such commands
             * that carry transactions will thus be ignored by the transaction/token state machines, since they
             * are ahead, and the correct decisions for their applicability have already been taken as encapsulated
             * in the copied store. */

            CompletableFuture<CoreSnapshot> snapshotFuture = coreClient.requestCoreSnapshot( source );

            CoreSnapshot coreSnapshot;
            try
            {
                coreSnapshot = snapshotFuture.get( 1, MINUTES ); // TODO: Configurable timeout.
            }
            catch ( TimeoutException e )
            {
                throw new StoreCopyFailedException( e );
            }

            localDatabase.copyStoreFrom( source, storeFetcher ); // this deletes the current store

            /* We install the snapshot after the store has been downloaded,
             * so that we are not left with a state ahead of the store. */
            coreState.installSnapshot( coreSnapshot );

            /* Starting the database will invoke the commit process factory in
             * the EnterpriseCoreEditionModule, which has important side-effects. */
            localDatabase.start();
        }
        catch ( IOException | ExecutionException e )
        {
            localDatabase.panic( e );
            throw new StoreCopyFailedException( e );
        }
    }
}
