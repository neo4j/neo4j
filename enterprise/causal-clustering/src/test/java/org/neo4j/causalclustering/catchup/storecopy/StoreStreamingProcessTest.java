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
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.neo4j.cursor.RawCursor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;
import static org.neo4j.kernel.impl.util.Cursors.rawCursorOf;

public class StoreStreamingProcessTest
{
    // mocks
    private final StoreFileStreamingProtocol protocol = mock( StoreFileStreamingProtocol.class );
    private final CheckPointer checkPointer = mock( CheckPointer.class );
    private final StoreResourceStreamFactory resourceStream = mock( StoreResourceStreamFactory.class );
    private final ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
    private final Supplier<CheckPointer> checkPointerSupplier = () -> checkPointer;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private StoreCopyCheckPointMutex mutex = new StoreCopyCheckPointMutex( lock );

    @Test
    public void shouldPerformSuccessfulStoreCopyProcess() throws Exception
    {
        // given
        StoreStreamingProcess process = new StoreStreamingProcess( protocol, checkPointerSupplier, mutex, resourceStream );

        // mocked behaviour
        ImmediateEventExecutor eventExecutor = ImmediateEventExecutor.INSTANCE;
        Promise<Void> completionPromise = eventExecutor.newPromise();
        long lastCheckpointedTxId = 1000L;
        RawCursor<StoreResource,IOException> resources = rawCursorOf();

        when( checkPointer.tryCheckPoint( any() ) ).thenReturn( lastCheckpointedTxId );
        when( checkPointer.lastCheckPointedTransactionId() ).thenReturn( lastCheckpointedTxId );
        when( protocol.end( ctx, SUCCESS ) ).thenReturn( completionPromise );
        when( resourceStream.create() ).thenReturn( resources );

        // when
        process.perform( ctx );

        // then
        InOrder inOrder = Mockito.inOrder( protocol, checkPointer );
        inOrder.verify( checkPointer ).tryCheckPoint( any() );
        inOrder.verify( protocol ).end( ctx, SUCCESS );
        inOrder.verifyNoMoreInteractions();

        assertEquals( 1, lock.getReadLockCount() );

        // when
        completionPromise.setSuccess( null );

        // then
        assertEquals( 0, lock.getReadLockCount() );
    }

    @Test
    public void shouldSignalFailure()
    {
        // given
        StoreStreamingProcess process = new StoreStreamingProcess( protocol, checkPointerSupplier, mutex, resourceStream );

        // when
        process.fail( ctx, E_STORE_ID_MISMATCH );

        // then
        verify( protocol ).end( ctx, E_STORE_ID_MISMATCH );
    }
}
