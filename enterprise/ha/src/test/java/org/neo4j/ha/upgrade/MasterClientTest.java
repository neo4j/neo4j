/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.ha.upgrade;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.com.StoreIdTestFactory;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker.Dependencies;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.MasterClient320;
import org.neo4j.kernel.ha.com.master.ConversationManager;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterImpl.Monitor;
import org.neo4j.kernel.ha.com.master.MasterImplTest;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Commands;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.com.storecopy.ResponseUnpacker.NO_OP_RESPONSE_UNPACKER;
import static org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import static org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker.DEFAULT_BATCH_SIZE;
import static org.neo4j.kernel.impl.transaction.command.Commands.createNode;

public class MasterClientTest
{
    private static final String MASTER_SERVER_HOST = "localhost";
    private static final int MASTER_SERVER_PORT = 9191;
    private static final int CHUNK_SIZE = 1024;
    private static final int TIMEOUT = 2000;
    private static final int TX_LOG_COUNT = 10;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    @Rule
    public final LifeRule life = new LifeRule( true );
    private final Monitors monitors = new Monitors();
    private final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader =
            new VersionAwareLogEntryReader<>();

    @Test( expected = MismatchingStoreIdException.class )
    public void newClientsShouldNotIgnoreStoreIdDifferences() throws Throwable
    {
        // Given
        MasterImpl.SPI masterImplSPI =
                MasterImplTest.mockedSpi( StoreIdTestFactory.newStoreIdForCurrentVersion( 1, 2, 3, 4 ) );
        when( masterImplSPI.getTransactionChecksum( anyLong() ) ).thenReturn( 5L );

        newMasterServer( masterImplSPI );

        StoreId storeId = StoreIdTestFactory.newStoreIdForCurrentVersion( 5, 6, 7, 8 );
        MasterClient masterClient = newMasterClient320( storeId );

        // When
        masterClient.handshake( 1, storeId );
    }

    @Test
    public void clientShouldReadAndApplyTransactionLogsOnNewLockSessionRequest() throws Throwable
    {
        // Given
        MasterImpl master = spy( newMasterImpl( mockMasterImplSpiWith( StoreId.DEFAULT ) ) );
        doReturn( voidResponseWithTransactionLogs() ).when( master ).newLockSession( any( RequestContext.class ) );

        newMasterServer( master );

        Dependencies deps = mock( Dependencies.class );
        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );
        when( deps.commitProcess() ).thenReturn( commitProcess );
        when( deps.logService() ).thenReturn( NullLogService.getInstance() );
        when( deps.versionContextSupplier() ).thenReturn( EmptyVersionContextSupplier.EMPTY );
        KernelTransactions transactions = mock( KernelTransactions.class );
        when( deps.kernelTransactions() ).thenReturn( transactions );

        ResponseUnpacker unpacker = life.add(
                new TransactionCommittingResponseUnpacker( deps, DEFAULT_BATCH_SIZE, 0 ) );

        MasterClient masterClient = newMasterClient320( StoreId.DEFAULT, unpacker );

        // When
        masterClient.newLockSession( new RequestContext( 1, 2, 3, 4, 5 ) );

        // Then
        verify( commitProcess ).commit( any( TransactionToApply.class ),
                any( CommitEvent.class ), any( TransactionApplicationMode.class ) );
    }

    @Test
    public void endLockSessionDoesNotUnpackResponse() throws Throwable
    {
        StoreId storeId = new StoreId( 1, 2, 3, 4, 5 );
        long txChecksum = 123;
        long lastAppliedTx = 5;

        ResponseUnpacker responseUnpacker = mock( ResponseUnpacker.class );
        MasterImpl.SPI masterImplSPI = MasterImplTest.mockedSpi( storeId );
        when( masterImplSPI.packTransactionObligationResponse( any( RequestContext.class ), ArgumentMatchers.any() ) )
                .thenReturn( Response.empty() );
        when( masterImplSPI.getTransactionChecksum( anyLong() ) ).thenReturn( txChecksum );

        newMasterServer( masterImplSPI );

        MasterClient client = newMasterClient320( storeId, responseUnpacker );

        HandshakeResult handshakeResult;
        try ( Response<HandshakeResult> handshakeResponse = client.handshake( 1, storeId ) )
        {
            handshakeResult = handshakeResponse.response();
        }
        verify( responseUnpacker ).unpackResponse( any( Response.class ), any( TxHandler.class ) );
        reset( responseUnpacker );

        RequestContext context = new RequestContext( handshakeResult.epoch(), 1, 1, lastAppliedTx, txChecksum );

        client.endLockSession( context, false );
        verify( responseUnpacker, never() ).unpackResponse( any( Response.class ), any( TxHandler.class ) );
    }

    private static MasterImpl.SPI mockMasterImplSpiWith( StoreId storeId )
    {
        return when( mock( MasterImpl.SPI.class ).storeId() ).thenReturn( storeId ).getMock();
    }

    private MasterServer newMasterServer( MasterImpl.SPI masterImplSPI )
    {
        MasterImpl masterImpl = new MasterImpl( masterImplSPI, mock(
                ConversationManager.class ), mock( Monitor.class ), masterConfig() );

        return newMasterServer( masterImpl );
    }

    private static MasterImpl newMasterImpl( MasterImpl.SPI masterImplSPI )
    {
        return new MasterImpl( masterImplSPI, mock(
                ConversationManager.class ), mock( Monitor.class ), masterConfig() );
    }

    private MasterServer newMasterServer( MasterImpl masterImpl )
    {
        return life.add( new MasterServer( masterImpl, NullLogProvider.getInstance(),
                masterServerConfiguration(),
                mock( TxChecksumVerifier.class ),
                monitors.newMonitor( ByteCounterMonitor.class, MasterClient.class ),
                monitors.newMonitor( RequestMonitor.class, MasterClient.class ), mock(
                ConversationManager.class ), logEntryReader ) );
    }

    private MasterClient newMasterClient320( StoreId storeId )
    {
        return newMasterClient320( storeId, NO_OP_RESPONSE_UNPACKER );
    }

    private MasterClient newMasterClient320( StoreId storeId, ResponseUnpacker responseUnpacker )
    {
        return life.add( new MasterClient320( MASTER_SERVER_HOST, MASTER_SERVER_PORT, null, NullLogProvider.getInstance(),
                storeId, TIMEOUT, TIMEOUT, 1, CHUNK_SIZE, responseUnpacker,
                monitors.newMonitor( ByteCounterMonitor.class, MasterClient320.class ),
                monitors.newMonitor( RequestMonitor.class, MasterClient320.class ), logEntryReader ) );
    }

    private static Response<Void> voidResponseWithTransactionLogs()
    {
        return new TransactionStreamResponse<>( null, StoreId.DEFAULT, visitor ->
        {
            for ( int i = 1; i <= TX_LOG_COUNT; i++ )
            {
                visitor.visit( committedTransactionRepresentation( i ) );
            }
        }, ResourceReleaser.NO_OP );
    }

    private static CommittedTransactionRepresentation committedTransactionRepresentation( int id )
    {
        return new CommittedTransactionRepresentation(
                new LogEntryStart( id, id, id, id - 1, new byte[]{}, LogPosition.UNSPECIFIED ),
                Commands.transactionRepresentation( createNode( 0 ) ),
                new LogEntryCommit( id, id ) );
    }

    private static Config masterConfig()
    {
        return Config.defaults( ClusterSettings.server_id, "1" );
    }

    private static Server.Configuration masterServerConfiguration()
    {
        return new Server.Configuration()
        {
            @Override
            public long getOldChannelThreshold()
            {
                return -1;
            }

            @Override
            public int getMaxConcurrentTransactions()
            {
                return 1;
            }

            @Override
            public int getChunkSize()
            {
                return CHUNK_SIZE;
            }

            @Override
            public HostnamePort getServerAddress()
            {
                return new HostnamePort( MASTER_SERVER_HOST, MASTER_SERVER_PORT );
            }
        };
    }
}
