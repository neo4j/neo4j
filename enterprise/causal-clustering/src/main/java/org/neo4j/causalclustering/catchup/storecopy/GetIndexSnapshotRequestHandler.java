/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.causalclustering.catchup.storecopy.DataSourceChecks.hasSameStoreId;
import static org.neo4j.causalclustering.catchup.storecopy.DataSourceChecks.isTransactionWithinReach;
import static org.neo4j.io.fs.FileUtils.relativePath;

public class GetIndexSnapshotRequestHandler extends SimpleChannelInboundHandler<GetIndexFilesRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<NeoStoreDataSource> dataSource;
    private final Supplier<CheckPointer> checkpointerSupplier;
    private final StoreFileStreamingProtocol storeFileStreamingProtocol;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;

    public GetIndexSnapshotRequestHandler( CatchupServerProtocol protocol, Supplier<NeoStoreDataSource> dataSource,
            Supplier<CheckPointer> checkpointerSupplier, StoreFileStreamingProtocol storeFileStreamingProtocol, PageCache pageCache, FileSystemAbstraction fs )
    {
        this.protocol = protocol;
        this.dataSource = dataSource;
        this.checkpointerSupplier = checkpointerSupplier;
        this.storeFileStreamingProtocol = storeFileStreamingProtocol;
        this.pageCache = pageCache;
        this.fs = fs;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetIndexFilesRequest snapshotRequest ) throws IOException
    {
        CloseablesListener closeablesListener = new CloseablesListener();
        NeoStoreDataSource neoStoreDataSource = dataSource.get();
        if ( !hasSameStoreId( snapshotRequest.expectedStoreId(), neoStoreDataSource ) )
        {
            storeFileStreamingProtocol.end( ctx, StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH );
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
        }
        else if ( !isTransactionWithinReach( snapshotRequest.requiredTransactionId(), checkpointerSupplier.get() ) )
        {
            storeFileStreamingProtocol.end( ctx, StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND );
        }
        else
        {
            StoreCopyFinishedResponse.Status status = StoreCopyFinishedResponse.Status.E_UNKNOWN;
            File storeDir = neoStoreDataSource.getStoreDir();
            ResourceIterator<StoreFileMetadata> resourceIterator =
                    neoStoreDataSource.getNeoStoreFileListing().getNeoStoreFileIndexListing().getSnapshot( snapshotRequest.descriptor() );
            try
            {
                closeablesListener.add( resourceIterator );
                while ( resourceIterator.hasNext() )
                {
                    StoreFileMetadata storeFileMetadata = resourceIterator.next();
                    File file = storeFileMetadata.file();
                    String relativePath = relativePath( storeDir, file );
                    int recordSize = storeFileMetadata.recordSize();
                    storeFileStreamingProtocol.stream( ctx, new StoreResource( file, relativePath, recordSize, pageCache, fs ) );
                }
                status = StoreCopyFinishedResponse.Status.SUCCESS;
            }
            finally
            {
                storeFileStreamingProtocol.end( ctx, status ).addListener( closeablesListener );
                protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
            }
        }
    }
}
