/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.EventId;
import org.neo4j.causalclustering.messaging.LoggingEventHandlerProvider;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
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
    private LoggingEventHandlerProvider eventHandler;

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
        streamToDiskProvider = new StreamToDiskProvider( temporaryDirectory, fsa, new Monitors() );
        eventHandler = new LoggingEventHandlerProvider( logProvider.getLog( SimpleCatchupClient.class ) );
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
        EventId id = EventId.create();
        return catchUpClient.makeBlockingRequest( from, new PrepareStoreCopyRequest( expectedStoreId, id.toString() ),
                StoreCopyResponseAdaptors.prepareStoreCopyAdaptor( streamToDiskProvider, eventHandler.eventHandler( id ) ) );
    }

    StoreCopyFinishedResponse requestIndividualFile( File file ) throws CatchUpClientException
    {
        return requestIndividualFile( file, correctStoreId );
    }

    StoreCopyFinishedResponse requestIndividualFile( File file, StoreId expectedStoreId ) throws CatchUpClientException
    {
        long lastTransactionId = getCheckPointer( graphDb ).lastCheckPointedTransactionId();
        EventId id = EventId.create();
        GetStoreFileRequest storeFileRequest = new GetStoreFileRequest( expectedStoreId, file, lastTransactionId, id.toString() );
        return catchUpClient.makeBlockingRequest( from, storeFileRequest,
                StoreCopyResponseAdaptors.filesCopyAdaptor( streamToDiskProvider, eventHandler.eventHandler( id ) ) );
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
        EventId id = EventId.create();
        GetIndexFilesRequest request = new GetIndexFilesRequest( storeId, indexId, lastCheckPointedTransactionId, id.toString() );
        return catchUpClient.makeBlockingRequest( from, request,
                StoreCopyResponseAdaptors.filesCopyAdaptor( streamToDiskProvider, eventHandler.eventHandler( id ) ) );
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
