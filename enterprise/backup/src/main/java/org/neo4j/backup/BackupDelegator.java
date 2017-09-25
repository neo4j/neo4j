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
package org.neo4j.backup;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StreamingTransactionsFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Simplifies the process of performing a backup over the transaction protocol by wrapping all the necessary classes
 * and delegating methods to the correct instances.
 */
public class BackupDelegator extends LifecycleAdapter
{
    private final RemoteStore remoteStore;
    private final CatchUpClient catchUpClient;
    private final StoreCopyClient storeCopyClient;
    private final ClearIdService clearIdService;

    BackupDelegator( RemoteStore remoteStore, CatchUpClient catchUpClient, StoreCopyClient storeCopyClient, ClearIdService clearIdService )
    {
        this.remoteStore = remoteStore;
        this.catchUpClient = catchUpClient;
        this.storeCopyClient = storeCopyClient;
        this.clearIdService = clearIdService;
    }

    void copy( AdvertisedSocketAddress fromAddress, StoreId expectedStoreId, File destDir ) throws StoreCopyFailedException
    {
        try
        {
            remoteStore.copy( fromAddress, expectedStoreId, destDir );
        }
        catch ( StreamingTransactionsFailedException | StoreCopyFailedException e )
        {
            throw Exceptions.launderedException( StoreCopyFailedException.class, e );
        }
    }

    CatchupResult tryCatchingUp( AdvertisedSocketAddress fromAddress, StoreId expectedStoreId, File storeDir ) throws StoreCopyFailedException
    {
        try
        {
            return remoteStore.tryCatchingUp( fromAddress, expectedStoreId, storeDir );
        }
        catch ( IOException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    @Override
    public void start()
    {
        catchUpClient.start();
    }

    @Override
    public void stop()
    {
        catchUpClient.stop();
    }

    public StoreId fetchStoreId( AdvertisedSocketAddress fromAddress ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.fetchStoreId( fromAddress );
    }

    public void clearIdFiles( FileSystemAbstraction fileSystem, File targetDirectory )
    {
        clearIdService.clearIdFiles( fileSystem, targetDirectory );
    }
}
