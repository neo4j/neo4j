/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.identity.StoreId;
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

    public void replaceWithStoreFrom( CatchupAddressProvider addressProvider, StoreId expectedStoreId )
            throws IOException, StoreCopyFailedException, DatabaseShutdownException
    {
        try ( TemporaryStoreDirectory tempStore = new TemporaryStoreDirectory( fs, pageCache, localDatabase.storeDir() ) )
        {
            remoteStore.copy( addressProvider, expectedStoreId, tempStore.storeDir(), false );
            copiedStoreRecovery.recoverCopiedStore( tempStore.storeDir() );
            localDatabase.replaceWith( tempStore.storeDir() );
        }
        log.info( "Replaced store successfully" );
    }
}
