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

import java.io.File;
import java.util.function.Supplier;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.causalclustering.catchup.CatchupServerProtocol.State;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;
import static org.neo4j.io.fs.FileUtils.relativePath;

public class GetStoreRequestHandler extends SimpleChannelInboundHandler<GetStoreRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<NeoStoreDataSource> dataSource;
    private final Supplier<CheckPointer> checkPointerSupplier;
    private final FileSystemAbstraction fs;
    private final Log log;

    public GetStoreRequestHandler( CatchupServerProtocol protocol, Supplier<NeoStoreDataSource> dataSource,
                                   Supplier<CheckPointer> checkPointerSupplier, FileSystemAbstraction fs,
                                   LogProvider logProvider )
    {
        this.protocol = protocol;
        this.dataSource = dataSource;
        this.checkPointerSupplier = checkPointerSupplier;
        this.fs = fs;
        this.log = logProvider.getLog( getClass() );
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
            try ( ResourceIterator<StoreFileMetadata> files = dataSource.get().listStoreFiles( false ) )
            {
                while ( files.hasNext() )
                {
                    File file = files.next().file();
                    log.debug( "Sending file " + file );

                    ctx.writeAndFlush( ResponseMessageType.FILE );
                    ctx.writeAndFlush( new FileHeader( relativePath( dataSource.get().getStoreDir(), file ) ) );
                    ctx.writeAndFlush( new FileSender( fs.open( file, "r" ) ) );
                }
            }
            endStoreCopy( SUCCESS, ctx, lastCheckPointedTx );
        }
        protocol.expect( State.MESSAGE_TYPE );
    }

    private void endStoreCopy( Status status, ChannelHandlerContext ctx, long lastCommittedTxBeforeStoreCopy )
    {
        ctx.write( ResponseMessageType.STORE_COPY_FINISHED );
        ctx.writeAndFlush( new StoreCopyFinishedResponse( status, lastCommittedTxBeforeStoreCopy ) );
    }
}
