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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.eclipse.collections.api.set.primitive.LongSet;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.messaging.EventHandler;
import org.neo4j.causalclustering.messaging.EventHandlerProvider;
import org.neo4j.causalclustering.messaging.EventId;
import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;

import static org.neo4j.causalclustering.catchup.storecopy.DataSourceChecks.hasSameStoreId;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Begin;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.End;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Error;
import static org.neo4j.causalclustering.messaging.EventHandler.Param.param;

public class PrepareStoreCopyRequestHandler extends SimpleChannelInboundHandler<PrepareStoreCopyRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<CheckPointer> checkPointerSupplier;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider;
    private final Supplier<NeoStoreDataSource> dataSourceSupplier;
    private EventHandlerProvider eventHandlerProvider;
    private final StoreFileStreamingProtocol streamingProtocol = new StoreFileStreamingProtocol();

    public PrepareStoreCopyRequestHandler( CatchupServerProtocol catchupServerProtocol, Supplier<CheckPointer> checkPointerSupplier,
            StoreCopyCheckPointMutex storeCopyCheckPointMutex, Supplier<NeoStoreDataSource> dataSourceSupplier,
            PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider, EventHandlerProvider eventHandlerProvider )
    {
        this.protocol = catchupServerProtocol;
        this.checkPointerSupplier = checkPointerSupplier;
        this.storeCopyCheckPointMutex = storeCopyCheckPointMutex;
        this.prepareStoreCopyFilesProvider = prepareStoreCopyFilesProvider;
        this.dataSourceSupplier = dataSourceSupplier;
        this.eventHandlerProvider = eventHandlerProvider;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext channelHandlerContext, PrepareStoreCopyRequest prepareStoreCopyRequest ) throws IOException
    {
        EventHandler eventHandler = eventHandlerProvider.eventHandler( EventId.from( prepareStoreCopyRequest.messageId() ) );
        eventHandler.on( Begin, param( "Request", prepareStoreCopyRequest ) );
        CloseablesListener closeablesListener = new CloseablesListener();
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_LISTING_STORE );
        try
        {
            NeoStoreDataSource neoStoreDataSource = dataSourceSupplier.get();
            if ( !hasSameStoreId( prepareStoreCopyRequest.getStoreId(), neoStoreDataSource ) )
            {
                channelHandlerContext.write( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE );
                eventHandler.on( Error, "Store Ids does not match" );
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
            eventHandler.on( End, param( "Response", response ) );
            channelHandlerContext.writeAndFlush( response ).addListener( closeablesListener );
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
        }
    }

    private PrepareStoreCopyResponse createSuccessfulResponse( CheckPointer checkPointer, PrepareStoreCopyFiles prepareStoreCopyFiles ) throws IOException
    {
        LongSet indexIds = prepareStoreCopyFiles.getNonAtomicIndexIds();
        File[] files = prepareStoreCopyFiles.listReplayableFiles();
        long lastCommittedTxId = checkPointer.lastCheckPointedTransactionId();
        return PrepareStoreCopyResponse.success( files, indexIds, lastCommittedTxId );
    }

    private Resource tryCheckpointAndAcquireMutex( CheckPointer checkPointer ) throws IOException
    {
        return storeCopyCheckPointMutex.storeCopy( () -> checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Store copy" ) ) );
    }
}
