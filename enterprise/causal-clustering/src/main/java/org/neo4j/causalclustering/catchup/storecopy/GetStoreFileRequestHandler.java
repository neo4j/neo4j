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
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static java.lang.String.format;
import static org.neo4j.causalclustering.catchup.storecopy.DataSourceChecks.hasSameStoreId;
import static org.neo4j.causalclustering.catchup.storecopy.DataSourceChecks.isTransactionWithinReach;
import static org.neo4j.io.fs.FileUtils.relativePath;

public class GetStoreFileRequestHandler extends SimpleChannelInboundHandler<GetStoreFileRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<NeoStoreDataSource> dataSource;
    private final Supplier<CheckPointer> checkpointerSupplier;
    private final StoreFileStreamingProtocol storeFileStreamingProtocol;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Log log;

    public GetStoreFileRequestHandler( CatchupServerProtocol protocol, Supplier<NeoStoreDataSource> dataSource, Supplier<CheckPointer> checkpointerSupplier,
            StoreFileStreamingProtocol storeFileStreamingProtocol, PageCache pageCache, FileSystemAbstraction fs, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.dataSource = dataSource;
        this.checkpointerSupplier = checkpointerSupplier;
        this.storeFileStreamingProtocol = storeFileStreamingProtocol;
        this.pageCache = pageCache;
        this.fs = fs;
        this.log = logProvider.getLog( GetStoreFileRequestHandler.class );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetStoreFileRequest fileRequest ) throws Exception
    {
        log.debug( "Requesting file %s", fileRequest.file() );
        NeoStoreDataSource neoStoreDataSource = dataSource.get();
        if ( !hasSameStoreId( fileRequest.expectedStoreId(), neoStoreDataSource ) )
        {
            storeFileStreamingProtocol.end( ctx, StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH );
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
        }
        else if ( !isTransactionWithinReach( fileRequest.requiredTransactionId(), checkpointerSupplier.get() ) )
        {
            storeFileStreamingProtocol.end( ctx, StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND );
        }
        else
        {
            File storeDir = neoStoreDataSource.getStoreDir();
            StoreFileMetadata storeFileMetadata = findFile( fileRequest.file().getName() );
            storeFileStreamingProtocol.stream( ctx,
                    new StoreResource( storeFileMetadata.file(), relativePath( storeDir, storeFileMetadata.file() ), storeFileMetadata.recordSize(), pageCache,
                            fs ) );
            storeFileStreamingProtocol.end( ctx, StoreCopyFinishedResponse.Status.SUCCESS );
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
        }
    }

    private StoreFileMetadata findFile( String fileName ) throws IOException
    {
        try ( ResourceIterator<StoreFileMetadata> resourceIterator = dataSource.get().listStoreFiles( false ) )
        {
            return onlyOne( resourceIterator.stream().filter( matchesRequested( fileName ) ).collect( Collectors.toList() ), fileName );
        }
    }

    private StoreFileMetadata onlyOne( List<StoreFileMetadata> files, String description )
    {
        if ( files.size() != 1 )
        {
            throw new IllegalStateException( format( "Expected exactly one file '%s'. Got %d", description, files.size() ) );
        }
        return files.get( 0 );
    }

    private static Predicate<StoreFileMetadata> matchesRequested( String fileName )
    {
        return f -> f.file().getName().equals( fileName );
    }
}
