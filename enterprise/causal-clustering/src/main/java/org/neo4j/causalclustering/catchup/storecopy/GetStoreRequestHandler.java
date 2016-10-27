/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import io.netty.handler.stream.ChunkedNioStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.causalclustering.catchup.CatchupServerProtocol.State;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;

public class GetStoreRequestHandler extends SimpleChannelInboundHandler<GetStoreRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<NeoStoreDataSource> dataSource;

    private Supplier<CheckPointer> checkPointerSupplier;

    public GetStoreRequestHandler( CatchupServerProtocol protocol,
                                   Supplier<NeoStoreDataSource> dataSource,
                                   Supplier<CheckPointer> checkPointerSupplier )
    {
        this.protocol = protocol;
        this.dataSource = dataSource;
        this.checkPointerSupplier = checkPointerSupplier;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, GetStoreRequest msg ) throws Exception
    {
        if ( !msg.expectedStoreId().equalToKernelStoreId( dataSource.get().getStoreId() ) )
        {
            endStoreCopy( SUCCESS, ctx, -1 );
        }
        else
        {
            long lastCheckPointedTx = checkPointerSupplier.get().tryCheckPoint( new SimpleTriggerInfo( "Store copy" ) );
            sendFiles( ctx );
            endStoreCopy( SUCCESS, ctx, lastCheckPointedTx );
        }
        protocol.expect( State.MESSAGE_TYPE );
    }

    private void sendFiles( ChannelHandlerContext ctx ) throws IOException
    {
        try ( ResourceIterator<StoreFileMetadata> files = dataSource.get().listStoreFiles( false ) )
        {
            while ( files.hasNext() )
            {
                sendFile( ctx, files.next().file() );
            }
        }
    }

    private void sendFile( ChannelHandlerContext ctx, File file ) throws FileNotFoundException
    {
        ctx.writeAndFlush( ResponseMessageType.FILE );
        long initialLength = file.length();
        ctx.writeAndFlush( new FileHeader( file.getName(), initialLength ) );
        ctx.writeAndFlush( new ChunkedNioStream( new LimitedLengthReadableByteChannel(
                new FileInputStream( file ).getChannel(), initialLength ) ) );
    }

    private void endStoreCopy( Status status, ChannelHandlerContext ctx, long lastCommittedTxBeforeStoreCopy )
    {
        ctx.write( ResponseMessageType.STORE_COPY_FINISHED );
        ctx.writeAndFlush( new StoreCopyFinishedResponse( status, lastCommittedTxBeforeStoreCopy ) );
    }
}
