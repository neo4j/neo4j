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
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.messaging.StoreCopyRequest;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static java.lang.String.format;
import static org.neo4j.causalclustering.catchup.storecopy.DataSourceChecks.hasSameStoreId;
import static org.neo4j.causalclustering.catchup.storecopy.DataSourceChecks.isTransactionWithinReach;
import static org.neo4j.io.fs.FileUtils.relativePath;

public abstract class StoreCopyRequestHandler<T extends StoreCopyRequest> extends SimpleChannelInboundHandler<T>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<NeoStoreDataSource> dataSource;
    private final Supplier<CheckPointer> checkpointerSupplier;
    private final StoreFileStreamingProtocol storeFileStreamingProtocol;

    private final FileSystemAbstraction fs;
    private final Log log;

    StoreCopyRequestHandler( CatchupServerProtocol protocol, Supplier<NeoStoreDataSource> dataSource, Supplier<CheckPointer> checkpointerSupplier,
            StoreFileStreamingProtocol storeFileStreamingProtocol, FileSystemAbstraction fs, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.dataSource = dataSource;
        this.checkpointerSupplier = checkpointerSupplier;
        this.storeFileStreamingProtocol = storeFileStreamingProtocol;
        this.fs = fs;
        this.log = logProvider.getLog( StoreCopyRequestHandler.class );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, T request ) throws Exception
    {
        log.debug( "Handling request %s", request );
        StoreCopyFinishedResponse.Status responseStatus = StoreCopyFinishedResponse.Status.E_UNKNOWN;
        try
        {
            NeoStoreDataSource neoStoreDataSource = dataSource.get();
            if ( !hasSameStoreId( request.expectedStoreId(), neoStoreDataSource ) )
            {
                responseStatus = StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH;
            }
            else if ( !isTransactionWithinReach( request.requiredTransactionId(), checkpointerSupplier.get() ) )
            {
                responseStatus = StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND;
            }
            else
            {
                File storeDir = neoStoreDataSource.getStoreDir();
                try ( ResourceIterator<StoreFileMetadata> resourceIterator = files( request, neoStoreDataSource ) )
                {
                    while ( resourceIterator.hasNext() )
                    {
                        StoreFileMetadata storeFileMetadata = resourceIterator.next();
                        storeFileStreamingProtocol.stream( ctx,
                                new StoreResource( storeFileMetadata.file(), relativePath( storeDir, storeFileMetadata.file() ), storeFileMetadata.recordSize(),
                                        fs ) );
                    }
                }
                responseStatus = StoreCopyFinishedResponse.Status.SUCCESS;
            }
        }
        finally
        {
            storeFileStreamingProtocol.end( ctx, responseStatus );
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
        }
    }

    abstract ResourceIterator<StoreFileMetadata> files( T request, NeoStoreDataSource neoStoreDataSource ) throws IOException;

    private static Iterator<StoreFileMetadata> onlyOne( List<StoreFileMetadata> files, String description )
    {
        if ( files.size() != 1 )
        {
            throw new IllegalStateException( format( "Expected exactly one file '%s'. Got %d", description, files.size() ) );
        }
        return files.iterator();
    }

    private static Predicate<StoreFileMetadata> matchesRequested( String fileName )
    {
        return f -> f.file().getName().equals( fileName );
    }

    public static class GetStoreFileRequestHandler extends StoreCopyRequestHandler<GetStoreFileRequest>
    {
        public GetStoreFileRequestHandler( CatchupServerProtocol protocol, Supplier<NeoStoreDataSource> dataSource, Supplier<CheckPointer> checkpointerSupplier,
                StoreFileStreamingProtocol storeFileStreamingProtocol, FileSystemAbstraction fs, LogProvider logProvider )
        {
            super( protocol, dataSource, checkpointerSupplier, storeFileStreamingProtocol, fs, logProvider );
        }

        @Override
        ResourceIterator<StoreFileMetadata> files( GetStoreFileRequest request, NeoStoreDataSource neoStoreDataSource ) throws IOException
        {
            try ( ResourceIterator<StoreFileMetadata> resourceIterator = neoStoreDataSource.listStoreFiles( false ) )
            {
                String fileName = request.file().getName();
                return Iterators.asResourceIterator(
                        onlyOne( resourceIterator.stream().filter( matchesRequested( fileName ) ).collect( Collectors.toList() ), fileName ) );
            }
        }
    }

    public static class GetIndexSnapshotRequestHandler extends StoreCopyRequestHandler<GetIndexFilesRequest>
    {
        public GetIndexSnapshotRequestHandler( CatchupServerProtocol protocol, Supplier<NeoStoreDataSource> dataSource,
                Supplier<CheckPointer> checkpointerSupplier, StoreFileStreamingProtocol storeFileStreamingProtocol,
                FileSystemAbstraction fs, LogProvider logProvider )
        {
            super( protocol, dataSource, checkpointerSupplier, storeFileStreamingProtocol, fs, logProvider );
        }

        @Override
        ResourceIterator<StoreFileMetadata> files( GetIndexFilesRequest request, NeoStoreDataSource neoStoreDataSource ) throws IOException
        {
            return neoStoreDataSource.getNeoStoreFileListing().getNeoStoreFileIndexListing().getSnapshot( request.indexId() );
        }
    }
}
