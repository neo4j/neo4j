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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class StoreCopyProcess
{
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final LocalDatabase localDatabase;
    private final CopiedStoreRecovery copiedStoreRecovery;
    private final Log log;
    private final RemoteStore remoteStore;

    public StoreCopyProcess( FileSystemAbstraction fs, PageCache pageCache, LocalDatabase localDatabase,
            CopiedStoreRecovery copiedStoreRecovery, RemoteStore remoteStore, LogProvider logProvider )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.localDatabase = localDatabase;
        this.copiedStoreRecovery = copiedStoreRecovery;
        this.remoteStore = remoteStore;
        this.log = logProvider.getLog( getClass() );
    }

    public void replaceWithStoreFrom( AdvertisedSocketAddress source, StoreId expectedStoreId )
            throws IOException, StoreCopyFailedException, StreamingTransactionsFailedException
    {
        try ( TemporaryStoreDirectory tempStore = new TemporaryStoreDirectory( fs, pageCache, localDatabase.storeDir() ) )
        {
            remoteStore.copy( source, expectedStoreId, tempStore.storeDir() );
            copiedStoreRecovery.recoverCopiedStore( tempStore.storeDir() );
            localDatabase.replaceWith( tempStore.storeDir() );
        }
        log.info( "Replaced store with one downloaded from %s", source );
    }
}
