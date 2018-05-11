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
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;

import static org.neo4j.causalclustering.catchup.storecopy.DataSourceChecks.hasSameStoreId;

public class PrepareStoreCopyRequestHandler extends SimpleChannelInboundHandler<PrepareStoreCopyRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<CheckPointer> checkPointerSupplier;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider;
    private final Supplier<NeoStoreDataSource> dataSourceSupplier;
    private final StoreFileStreamingProtocol streamingProtocol = new StoreFileStreamingProtocol();

    public PrepareStoreCopyRequestHandler( CatchupServerProtocol catchupServerProtocol, Supplier<CheckPointer> checkPointerSupplier,
            StoreCopyCheckPointMutex storeCopyCheckPointMutex, Supplier<NeoStoreDataSource> dataSourceSupplier,
            PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider )
    {
        this.protocol = catchupServerProtocol;
        this.checkPointerSupplier = checkPointerSupplier;
        this.storeCopyCheckPointMutex = storeCopyCheckPointMutex;
        this.prepareStoreCopyFilesProvider = prepareStoreCopyFilesProvider;
        this.dataSourceSupplier = dataSourceSupplier;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext channelHandlerContext, PrepareStoreCopyRequest prepareStoreCopyRequest ) throws IOException
    {
        CloseablesListener closeablesListener = new CloseablesListener();
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_LISTING_STORE );
        try
        {
            NeoStoreDataSource neoStoreDataSource = dataSourceSupplier.get();
            if ( !hasSameStoreId( prepareStoreCopyRequest.getStoreId(), neoStoreDataSource ) )
            {
                channelHandlerContext.write( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE );
                response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH );
            }
            else
            {
                CheckPointer checkPointer = checkPointerSupplier.get();
                closeablesListener.add( tryCheckpointAndAcquireMutex( checkPointer ) );
                PrepareStoreCopyFiles prepareStoreCopyFiles =
                        closeablesListener.add( prepareStoreCopyFilesProvider.prepareStoreCopyFiles( neoStoreDataSource ) );

                StoreResource[] nonReplayable = prepareStoreCopyFiles.getAtomicFilesSnapshot();
                for ( StoreResource storeResource : nonReplayable )
                {
                    streamingProtocol.stream( channelHandlerContext, storeResource );
                }
                channelHandlerContext.write( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE );
                response = createSuccessfulResponse( checkPointer, prepareStoreCopyFiles );
            }
        }
        finally
        {
            channelHandlerContext.writeAndFlush( response ).addListener( closeablesListener );
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
        }
    }

    private PrepareStoreCopyResponse createSuccessfulResponse( CheckPointer checkPointer, PrepareStoreCopyFiles prepareStoreCopyFiles ) throws IOException
    {
        PrimitiveLongSet indexIds = prepareStoreCopyFiles.getNonAtomicIndexIds();
        File[] files = prepareStoreCopyFiles.listReplayableFiles();
        long lastCommittedTxId = checkPointer.lastCheckPointedTransactionId();
        return PrepareStoreCopyResponse.success( files, indexIds, lastCommittedTxId );
    }

    private Resource tryCheckpointAndAcquireMutex( CheckPointer checkPointer ) throws IOException
    {
        return storeCopyCheckPointMutex.storeCopy( () -> checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Store copy" ) ) );
    }
}
