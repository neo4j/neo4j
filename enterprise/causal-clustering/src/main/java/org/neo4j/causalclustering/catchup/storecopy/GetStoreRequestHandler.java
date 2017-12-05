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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.causalclustering.catchup.CatchupServerProtocol.State;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;
import static org.neo4j.io.fs.FileUtils.relativePath;

public class GetStoreRequestHandler extends SimpleChannelInboundHandler<GetStoreRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<NeoStoreDataSource> dataSource;
    private final Supplier<CheckPointer> checkPointerSupplier;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final Log log;
    private final StoreCopyCheckPointMutex mutex;

    public GetStoreRequestHandler( CatchupServerProtocol protocol, Supplier<NeoStoreDataSource> dataSource, Supplier<CheckPointer> checkPointerSupplier,
            FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider, StoreCopyCheckPointMutex mutex )
    {
        this.protocol = protocol;
        this.dataSource = dataSource;
        this.checkPointerSupplier = checkPointerSupplier;
        this.fs = fs;
        this.pageCache = pageCache;
        this.mutex = mutex;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetStoreRequest msg ) throws Exception
    {
        if ( !msg.expectedStoreId().equalToKernelStoreId( dataSource.get().getStoreId() ) )
        {
            endStoreCopy( E_STORE_ID_MISMATCH, ctx, -1 );
        }
        else
        {
            queueStoreCopy( ctx );
        }
        protocol.expect( State.MESSAGE_TYPE );
    }

    /**
     * This queues all the file sending operations on the outgoing pipeline, including
     * chunking {@link FileSender} handlers. Note that we do not block here.
     */
    private void queueStoreCopy( ChannelHandlerContext ctx ) throws IOException
    {
        CheckPointer checkPointer = checkPointerSupplier.get();

        // The checkpoint-lock will be released when the entire store copy has finished.
        Resource checkPointLock = mutex.storeCopy( () -> checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Store copy" ) ) );
        long lastCheckPointedTx = checkPointer.lastCheckPointedTransactionId();

        ResourceIterator<StoreFileMetadata> files = dataSource.get().listStoreFiles( false );
        while ( files.hasNext() )
        {
            StoreFileMetadata fileMetadata = files.next();
            File file = fileMetadata.file();
            log.debug( "Sending file " + file );

            ctx.writeAndFlush( ResponseMessageType.FILE );
            ctx.writeAndFlush( new FileHeader( relativePath( dataSource.get().getStoreDir(), file ), fileMetadata.recordSize() ) );

            ReadableByteChannel channel = getReadableChannel( file );
            ctx.writeAndFlush( new FileSender( channel ) );
        }

        ChannelFuture completion = endStoreCopy( SUCCESS, ctx, lastCheckPointedTx );
        completion.addListener( f -> checkPointLock.close() );
    }

    private ReadableByteChannel getReadableChannel( File file ) throws IOException
    {
        ReadableByteChannel channel;
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping( file );
        if ( existingMapping.isPresent() )
        {
            try ( PagedFile pagedFile = existingMapping.get() )
            {

                channel = pagedFile.openReadableByteChannel();
            }
        }
        else
        {
            channel = fs.open( file, "r" );
        }
        return channel;
    }

    private ChannelFuture endStoreCopy( Status status, ChannelHandlerContext ctx, long lastCommittedTxBeforeStoreCopy )
    {
        ctx.write( ResponseMessageType.STORE_COPY_FINISHED );
        return ctx.writeAndFlush( new StoreCopyFinishedResponse( status, lastCommittedTxBeforeStoreCopy ) );
    }
}
