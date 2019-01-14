/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrepareStoreCopyRequestHandlerTest
{
    private static final StoreId STORE_ID_MATCHING = new StoreId( 1, 2, 3, 4 );
    private static final StoreId STORE_ID_MISMATCHING = new StoreId( 5000, 6000, 7000, 8000 );
    private final ChannelHandlerContext channelHandlerContext = mock( ChannelHandlerContext.class );
    private EmbeddedChannel embeddedChannel;

    private static final CheckPointer checkPointer = mock( CheckPointer.class );
    private static final NeoStoreDataSource neoStoreDataSource = mock( NeoStoreDataSource.class );
    private CatchupServerProtocol catchupServerProtocol;
    private final PrepareStoreCopyFiles prepareStoreCopyFiles = mock( PrepareStoreCopyFiles.class );

    @Before
    public void setup()
    {
        StoreCopyCheckPointMutex storeCopyCheckPointMutex = new StoreCopyCheckPointMutex();
        PrepareStoreCopyRequestHandler subject = createHandler( storeCopyCheckPointMutex );
        embeddedChannel = new EmbeddedChannel( subject );
    }

    private PrepareStoreCopyRequestHandler createHandler( StoreCopyCheckPointMutex storeCopyCheckPointMutex )
    {
        catchupServerProtocol = new CatchupServerProtocol();
        catchupServerProtocol.expect( CatchupServerProtocol.State.PREPARE_STORE_COPY );
        Supplier<CheckPointer> checkPointerSupplier = () -> checkPointer;
        Supplier<NeoStoreDataSource> dataSourceSupplier = () -> neoStoreDataSource;
        when( neoStoreDataSource.getStoreId() ).thenReturn( new org.neo4j.kernel.impl.store.StoreId( 1, 2, 5, 3, 4 ) );

        PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider = mock( PrepareStoreCopyFilesProvider.class );
        when( prepareStoreCopyFilesProvider.prepareStoreCopyFiles( any() ) ).thenReturn( prepareStoreCopyFiles );

        return new PrepareStoreCopyRequestHandler( catchupServerProtocol, checkPointerSupplier, storeCopyCheckPointMutex, dataSourceSupplier,
                prepareStoreCopyFilesProvider );
    }

    @Test
    public void shouldGiveErrorResponseIfStoreMismatch()
    {
        // given store id doesn't match

        // when PrepareStoreCopyRequest is written to channel
        embeddedChannel.writeInbound( new PrepareStoreCopyRequest( STORE_ID_MISMATCHING ) );

        // then there is a store id mismatch message
        assertEquals( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE, embeddedChannel.readOutbound() );
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH );
        assertEquals( response, embeddedChannel.readOutbound() );

        // and the expected message type is reset back to message type
        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    public void shouldGetSuccessfulResponseFromPrepareStoreCopyRequest() throws Exception
    {
        // given storeId matches
        PrimitiveLongSet indexIds = Primitive.longSet();
        indexIds.add( 1 );
        File[] files = new File[]{new File( "file" )};
        long lastCheckpoint = 1;

        configureProvidedStoreCopyFiles( new StoreResource[0], files, indexIds, lastCheckpoint );

        // when store listing is requested
        embeddedChannel.writeInbound( channelHandlerContext, new PrepareStoreCopyRequest( STORE_ID_MATCHING ) );

        // and the contents of the store listing response is sent
        assertEquals( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE, embeddedChannel.readOutbound() );
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.success( files, indexIds, lastCheckpoint );
        assertEquals( response, embeddedChannel.readOutbound() );

        // and the protocol is reset to expect any message type after listing has been transmitted
        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    public void shouldRetainLockWhileStreaming() throws Exception
    {
        // given
        ChannelPromise channelPromise = embeddedChannel.newPromise();
        ChannelHandlerContext channelHandlerContext = mock( ChannelHandlerContext.class );
        when( channelHandlerContext.writeAndFlush( any( PrepareStoreCopyResponse.class ) ) ).thenReturn( channelPromise );

        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        PrepareStoreCopyRequestHandler subjectHandler = createHandler( new StoreCopyCheckPointMutex( lock ) );

        // and
        PrimitiveLongSet indexIds = Primitive.longSet();
        indexIds.add( 42 );
        File[] files = new File[]{new File( "file" )};
        long lastCheckpoint = 1;
        configureProvidedStoreCopyFiles( new StoreResource[0], files, indexIds, lastCheckpoint );

        // when
        subjectHandler.channelRead0( channelHandlerContext, new PrepareStoreCopyRequest( STORE_ID_MATCHING ) );

        // then
        assertEquals( 1, lock.getReadLockCount() );

        // when
        channelPromise.setSuccess();

        //then
        assertEquals( 0, lock.getReadLockCount() );
    }

    private void configureProvidedStoreCopyFiles( StoreResource[] atomicFiles, File[] files, PrimitiveLongSet indexIds, long lastCommitedTx )
            throws IOException
    {
        when( prepareStoreCopyFiles.getAtomicFilesSnapshot() ).thenReturn( atomicFiles );
        when( prepareStoreCopyFiles.getNonAtomicIndexIds() ).thenReturn( indexIds );
        when( prepareStoreCopyFiles.listReplayableFiles() ).thenReturn( files );
        when( checkPointer.lastCheckPointedTransactionId() ).thenReturn( lastCommitedTx );
    }
}
