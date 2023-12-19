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

import java.io.File;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class SimpleCatchupClient implements AutoCloseable
{
    private final GraphDatabaseAPI graphDb;
    private final FileSystemAbstraction fsa;
    private final CatchUpClient catchUpClient;
    private final TestCatchupServer catchupServer;

    private final AdvertisedSocketAddress from;
    private final StoreId correctStoreId;
    private final StreamToDiskProvider streamToDiskProvider;
    private final PageCache clientPageCache;
    private final Log log;
    private final LogProvider logProvider;

    SimpleCatchupClient( GraphDatabaseAPI graphDb, FileSystemAbstraction fileSystemAbstraction, CatchUpClient catchUpClient,
            TestCatchupServer catchupServer, File temporaryDirectory, LogProvider logProvider )
    {
        this.graphDb = graphDb;
        this.fsa = fileSystemAbstraction;
        this.catchUpClient = catchUpClient;
        this.catchupServer = catchupServer;

        from = getCatchupServerAddress();
        correctStoreId = getStoreIdFromKernelStoreId( graphDb );
        clientPageCache = createPageCache();
        streamToDiskProvider = new StreamToDiskProvider( temporaryDirectory, fsa, clientPageCache, new Monitors() );
        log = logProvider.getLog( SimpleCatchupClient.class );
        this.logProvider = logProvider;
    }

    private PageCache createPageCache()
    {
        return StandalonePageCacheFactory.createPageCache( fsa );
    }

    PrepareStoreCopyResponse requestListOfFilesFromServer() throws CatchUpClientException
    {
        return requestListOfFilesFromServer( correctStoreId );
    }

    PrepareStoreCopyResponse requestListOfFilesFromServer( StoreId expectedStoreId ) throws CatchUpClientException
    {
        return catchUpClient.makeBlockingRequest( from, new PrepareStoreCopyRequest( expectedStoreId ),
                StoreCopyResponseAdaptors.prepareStoreCopyAdaptor( streamToDiskProvider, logProvider.getLog( SimpleCatchupClient.class ) ) );
    }

    StoreCopyFinishedResponse requestIndividualFile( File file ) throws CatchUpClientException
    {
        return requestIndividualFile( file, correctStoreId );
    }

    StoreCopyFinishedResponse requestIndividualFile( File file, StoreId expectedStoreId ) throws CatchUpClientException
    {
        long lastTransactionId = getCheckPointer( graphDb ).lastCheckPointedTransactionId();
        GetStoreFileRequest storeFileRequest = new GetStoreFileRequest( expectedStoreId, file, lastTransactionId );
        return catchUpClient.makeBlockingRequest( from, storeFileRequest, StoreCopyResponseAdaptors.filesCopyAdaptor( streamToDiskProvider, log ) );
    }

    private StoreId getStoreIdFromKernelStoreId( GraphDatabaseAPI graphDb )
    {
        org.neo4j.kernel.impl.store.StoreId storeId = graphDb.storeId();
        return new StoreId( storeId.getCreationTime(), storeId.getRandomId(), storeId.getUpgradeTime(), storeId.getUpgradeId() );
    }

    private AdvertisedSocketAddress getCatchupServerAddress()
    {
        return new AdvertisedSocketAddress( "localhost", catchupServer.address().getPort() );
    }

    StoreCopyFinishedResponse requestIndexSnapshot( long indexId ) throws CatchUpClientException
    {
        long lastCheckPointedTransactionId = getCheckPointer( graphDb ).lastCheckPointedTransactionId();
        StoreId storeId = getStoreIdFromKernelStoreId( graphDb );
        GetIndexFilesRequest request = new GetIndexFilesRequest( storeId, indexId, lastCheckPointedTransactionId );
        return catchUpClient.makeBlockingRequest( from, request, StoreCopyResponseAdaptors.filesCopyAdaptor( streamToDiskProvider, log ) );
    }

    @Override
    public void close() throws Exception
    {
        IOUtils.closeAll( clientPageCache );
    }

    private static CheckPointer getCheckPointer( GraphDatabaseAPI graphDb )
    {
        return graphDb.getDependencyResolver().resolveDependency( CheckPointer.class );
    }
}
