/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.storecopy.core;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.stream.ChunkedNioStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.CatchupServerProtocol;
import org.neo4j.coreedge.catchup.ResponseMessageType;
import org.neo4j.coreedge.catchup.storecopy.FileHeader;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.coreedge.catchup.storecopy.edge.GetStoreRequest;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;

import static org.neo4j.coreedge.catchup.CatchupServerProtocol.NextMessage;

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
        long lastCheckPointedTx = checkPointerSupplier.get().tryCheckPoint(new SimpleTriggerInfo("Store copy"));
        sendFiles( ctx );
        endStoreCopy( ctx, lastCheckPointedTx );
        protocol.expect( NextMessage.MESSAGE_TYPE );
    }

    private void sendFiles( ChannelHandlerContext ctx ) throws IOException
    {
        ResourceIterator<File> files = dataSource.get().listStoreFiles( false );
        while ( files.hasNext() )
        {
            sendFile( ctx, files.next() );
        }
    }

    private void sendFile( ChannelHandlerContext ctx, File file ) throws FileNotFoundException
    {
        ctx.writeAndFlush( ResponseMessageType.FILE );
        ctx.writeAndFlush( new FileHeader( file.getName(), file.length() ) );
        ctx.writeAndFlush( new ChunkedNioStream( new FileInputStream( file ).getChannel() ) );
    }

    private void endStoreCopy( ChannelHandlerContext ctx, long lastCommittedTxBeforeStoreCopy )
    {
        ctx.write( ResponseMessageType.STORY_COPY_FINISHED );
        ctx.writeAndFlush( new StoreCopyFinishedResponse( lastCommittedTxBeforeStoreCopy ) );
    }
}
