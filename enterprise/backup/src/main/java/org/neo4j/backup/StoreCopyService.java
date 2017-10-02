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

import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreams;
import org.neo4j.causalclustering.catchup.storecopy.StreamToDisk;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.monitoring.Monitors;

class StoreCopyService
{
    private final File storeDir;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final PageCache pageCache;
    private final StoreCopyClient storeCopyClient;
    private final AdvertisedSocketAddress fromAddress;

    StoreCopyService( File storeDir, FileSystemAbstraction fileSystemAbstraction, PageCache pageCache, StoreCopyClient storeCopyClient,
            AdvertisedSocketAddress fromAddress )
    {
        this.storeDir = storeDir;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.pageCache = pageCache;
        this.storeCopyClient = storeCopyClient;
        this.fromAddress = fromAddress;
    }

    long retrieveStore( StoreFileStreams storeFileStreams, StoreId expectedStoreId ) throws StoreCopyFailedException
    {
        long latestTxNumber = storeCopyClient.copyStoreFiles( fromAddress, expectedStoreId, storeFileStreams );
        try
        {
            storeFileStreams.close();
        }
        catch ( Exception e )
        {
            throw Exceptions.launderedException(e);
        }
        return latestTxNumber;
    }

    StoreFileStreams constructStoreFileStreams()
    {
        Monitors monitors = new Monitors();
        try
        {
            return new StreamToDisk( storeDir, fileSystemAbstraction, pageCache, monitors );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
