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
package org.neo4j.ha.upgrade;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Random;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.DefaultUnpackerDependencies;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.ha.com.master.ConversationManager;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterImpl.Monitor;
import org.neo4j.kernel.ha.com.master.MasterImplTest;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.impl.api.BatchingTransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.CleanupRule;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.com.storecopy.ResponseUnpacker.NO_OP_RESPONSE_UNPACKER;
import static org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import static org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker.DEFAULT_BATCH_SIZE;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class MasterClientTest
{
    private static final String MASTER_SERVER_HOST = "localhost";
    private static final int MASTER_SERVER_PORT = 9191;
    private static final int CHUNK_SIZE = 1024;
    private static final int TIMEOUT = 2000;

    private static final int TX_LOG_COUNT = 10;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public final CleanupRule cleanupRule = new CleanupRule();
    private final Monitors monitors = new Monitors();

    @Test(expected = MismatchingStoreIdException.class)
    public void newClientsShouldNotIgnoreStoreIdDifferences() throws Throwable
    {
        // Given
        MasterImpl.SPI masterImplSPI = MasterImplTest.mockedSpi( new StoreId( 1, 2, 3, 4 ) );
        when( masterImplSPI.getTransactionChecksum( anyLong() ) ).thenReturn( 5L );

        cleanupRule.add( newMasterServer( masterImplSPI ) );

        StoreId storeId = new StoreId( 5, 6, 7, 8 );
        MasterClient214 masterClient214 = cleanupRule.add( newMasterClient214( storeId ) );

        // When
        masterClient214.handshake( 1, storeId );
    }

    @Test
    public void clientShouldReadAndApplyTransactionLogsOnNewLockSessionRequest() throws Throwable
    {
        // Given
        MasterImpl master = spy( newMasterImpl( mockMasterImplSpiWith( StoreId.DEFAULT ) ) );
        doReturn( voidResponseWithTransactionLogs() ).when( master ).newLockSession( any( RequestContext.class ) );

        cleanupRule.add( newMasterServer( master ) );

        final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
        TransactionAppender txAppender = mock( TransactionAppender.class );
        when( txAppender.append( any( TransactionRepresentation.class ), anyLong() ) )
                .thenAnswer( new Answer<Commitment>()
                {
                    @Override
                    public Commitment answer( InvocationOnMock invocation ) throws Throwable
                    {
                        return new FakeCommitment( (Long) invocation.getArguments()[1], txIdStore );
                    }
                } );
        final BatchingTransactionRepresentationStoreApplier txApplier =
                mock( BatchingTransactionRepresentationStoreApplier.class );
        final IndexUpdatesValidator indexUpdatesValidator = mock( IndexUpdatesValidator.class );
        when( indexUpdatesValidator.validate( any( TransactionRepresentation.class ) ) )
                .thenReturn( ValidatedIndexUpdates.NONE );

        final Dependencies deps = new Dependencies();
        KernelHealth health = mock( KernelHealth.class );
        when( health.isHealthy() ).thenReturn( true );
        deps.satisfyDependencies(
                mock( LogicalTransactionStore.class ),
                mock( LogFile.class ),
                mock( LogRotation.class),
                mock( KernelTransactions.class ),
                health,
                txAppender,
                txApplier,
                txIdStore,
                indexUpdatesValidator,
                NullLogService.getInstance()
        );

        TransactionCommittingResponseUnpacker.Dependencies dependencies = new DefaultUnpackerDependencies( deps, 0 )
        {
            @Override
            public BatchingTransactionRepresentationStoreApplier transactionRepresentationStoreApplier()
            {
                return txApplier;
            }

            @Override
            public IndexUpdatesValidator indexUpdatesValidator()
            {
                return indexUpdatesValidator;
            }
        };
        ResponseUnpacker unpacker = initAndStart( new TransactionCommittingResponseUnpacker( dependencies,
                DEFAULT_BATCH_SIZE) );

        MasterClient masterClient = cleanupRule.add( newMasterClient214( StoreId.DEFAULT, unpacker ) );

        // When
        masterClient.newLockSession( new RequestContext( 1, 2, 3, 4, 5 ) );

        // Then
        verify( txAppender, times( TX_LOG_COUNT ) ).append( any( TransactionRepresentation.class ), anyLong() );
        // we can't verify transactionCommitted since that's part of the TransactionAppender, which we have mocked
        verify( txApplier, times( TX_LOG_COUNT ) )
                .apply( any( TransactionRepresentation.class ), any( ValidatedIndexUpdates.class ),
                        any( LockGroup.class ), anyLong(), any( TransactionApplicationMode.class ) );
        verify( txIdStore, times( TX_LOG_COUNT ) ).transactionClosed( anyLong(), anyLong(), anyLong() );
    }

    @Test
    public void endLockSessionDoesNotUnpackResponse() throws Throwable
    {
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        long txChecksum = 123;
        long lastAppliedTx = 5;

        ResponseUnpacker responseUnpacker = mock( ResponseUnpacker.class );
        MasterImpl.SPI masterImplSPI = MasterImplTest.mockedSpi( storeId );
        when( masterImplSPI.packTransactionObligationResponse( any( RequestContext.class ), Matchers.anyObject() ) )
                .thenReturn( Response.empty() );
        when( masterImplSPI.getTransactionChecksum( anyLong() ) ).thenReturn( txChecksum );

        cleanupRule.add( newMasterServer( masterImplSPI ) );

        MasterClient214 client = cleanupRule.add( newMasterClient214( storeId, responseUnpacker ) );

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

    private MasterServer newMasterServer( MasterImpl.SPI masterImplSPI ) throws Throwable
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

    private MasterServer newMasterServer( MasterImpl masterImpl ) throws Throwable
    {
        return initAndStart( new MasterServer( masterImpl, NullLogProvider.getInstance(),
                masterServerConfiguration(),
                mock( TxChecksumVerifier.class ),
                monitors.newMonitor( ByteCounterMonitor.class, MasterClient.class ),
                monitors.newMonitor( RequestMonitor.class, MasterClient.class ), mock(
                ConversationManager.class ) ) );
    }

    private MasterClient214 newMasterClient214( StoreId storeId ) throws Throwable
    {
        return initAndStart(
                new MasterClient214( MASTER_SERVER_HOST, MASTER_SERVER_PORT, null, NullLogProvider.getInstance(),
                storeId, TIMEOUT, TIMEOUT, 1, CHUNK_SIZE, NO_OP_RESPONSE_UNPACKER,
                monitors.newMonitor( ByteCounterMonitor.class, MasterClient214.class ),
                monitors.newMonitor( RequestMonitor.class, MasterClient214.class ) ) );
    }

    private MasterClient214 newMasterClient214( StoreId storeId, ResponseUnpacker responseUnpacker ) throws Throwable
    {
        return initAndStart(
                new MasterClient214( MASTER_SERVER_HOST, MASTER_SERVER_PORT, null, NullLogProvider.getInstance(),
                storeId, TIMEOUT, TIMEOUT, 1, CHUNK_SIZE, responseUnpacker,
                monitors.newMonitor( ByteCounterMonitor.class, MasterClient214.class ),
                monitors.newMonitor( RequestMonitor.class, MasterClient214.class ) ) );
    }

    private static Response<Void> voidResponseWithTransactionLogs()
    {
        return new TransactionStreamResponse<>( null, StoreId.DEFAULT, new TransactionStream()
        {
            @Override
            public void accept( Visitor<CommittedTransactionRepresentation, IOException> visitor ) throws IOException
            {
                for ( int i = 1; i <= TX_LOG_COUNT; i++ )
                {
                    visitor.visit( committedTransactionRepresentation( i ) );
                }
            }
        }, ResourceReleaser.NO_OP );
    }

    private static CommittedTransactionRepresentation committedTransactionRepresentation( int id )
    {
        return new CommittedTransactionRepresentation(
                new LogEntryStart( id, id, id, id - 1, new byte[]{}, LogPosition.UNSPECIFIED ),
                new PhysicalTransactionRepresentation( asList( nodeCommand() ) ),
                new OnePhaseCommit( id, id ) );
    }

    private static Command nodeCommand()
    {
        int nodeId = new Random().nextInt();
        NodeRecord before = new NodeRecord( nodeId, false, -1, -1, false );
        NodeRecord after = new NodeRecord( nodeId, false, -1, -1, true );
        return new Command.NodeCommand().init( before, after );
    }

    private static <T extends Lifecycle> T initAndStart( T element ) throws Throwable
    {
        element.init();
        element.start();
        return element;
    }

    private static Config masterConfig()
    {
        return new Config( stringMap( ClusterSettings.server_id.name(), "1" ) );
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
