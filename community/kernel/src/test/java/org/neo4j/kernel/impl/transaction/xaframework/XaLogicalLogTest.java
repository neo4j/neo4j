/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.neo4j.kernel.impl.transaction.XidImpl.DEFAULT_SEED;
import static org.neo4j.kernel.impl.transaction.XidImpl.getNewGlobalId;
import static org.neo4j.kernel.impl.transaction.xaframework.ForceMode.forced;
import static org.neo4j.kernel.impl.transaction.xaframework.InjectedTransactionValidator.ALLOW_ALL;
import static org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriterv1.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies.NO_PRUNING;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.listeners.InvocationListener;
import org.mockito.listeners.MethodInvocationReport;
import org.mockito.stubbing.Answer;
import org.neo4j.helpers.Functions;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.StoreFileChannel;
import org.neo4j.kernel.impl.nioneo.xa.LogDeserializer;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReader;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandWriter;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandWriterFactory;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.util.Consumer;
import org.neo4j.kernel.impl.util.Cursor;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.FailureOutput;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class XaLogicalLogTest
{
    @Rule
    public final FailureOutput output = new FailureOutput();
    private static final byte[] RESOURCE_ID = new byte[]{0x00, (byte) 0x99, (byte) 0xcc};
    private long version;
    private int reads;

    @Test
    public void shouldRotateLogWithDoneRecordsOrEarlierTransactionsInTheRegionToCopy() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fs = ephemeralFs.get();
        XaTransactionFactory xaTf = mock( XaTransactionFactory.class );
        when( xaTf.getAndSetNewVersion() ).thenAnswer( new TxVersion( TxVersion.UPDATE_AND_GET ) );
        when( xaTf.getCurrentVersion() ).thenAnswer( new TxVersion( TxVersion.GET ) );
        File dir = TargetDirectory.forTest( fs, XaLogicalLogTest.class ).cleanDirectory( "log" );
        XaLogicalLog xaLogicalLog = new XaLogicalLog( new File( dir, "logical.log" ), mock( XaResourceManager.class ),
                                                      mock( XaCommandReaderFactory.class ),
                                                      mock( XaCommandWriterFactory.class ),
                                                      xaTf, fs, new Monitors(),
                                                      new SingleLoggingService( StringLogger.wrap( output.writer() ) ),
                                                      LogPruneStrategies.NO_PRUNING,
                                                      mock( TransactionStateFactory.class ),
                                                      mock( KernelHealth.class ),
                                                      25 * 1024 * 1024, ALLOW_ALL,
                                                      Functions.<List<LogEntry>>identity(),
                                                      Functions.<List<LogEntry>>identity() );
        xaLogicalLog.open();
        // first write a transaction that should not be copied during rotation, just to verify we don't copy everything
        int tx0 = xaLogicalLog.start( new XidImpl( getNewGlobalId( DEFAULT_SEED, 0 ), RESOURCE_ID ), -1, 0, 0 );
        xaLogicalLog.writeStartEntry( tx0 );
        xaLogicalLog.commitOnePhase( tx0, 1, ForceMode.forced );
        xaLogicalLog.done( tx0 );

        // when
        int tx1 = xaLogicalLog.start( new XidImpl( getNewGlobalId( DEFAULT_SEED, 0 ), RESOURCE_ID ), -1, 0, 0 );
        int tx2 = xaLogicalLog.start( new XidImpl( getNewGlobalId( DEFAULT_SEED, 0 ), RESOURCE_ID ), -1, 0, 0 );
        int tx3 = xaLogicalLog.start( new XidImpl( getNewGlobalId( DEFAULT_SEED, 0 ), RESOURCE_ID ), -1, 0, 0 );
        // start and prepare tx1
        xaLogicalLog.writeStartEntry( tx1 );
        xaLogicalLog.prepare( tx1 );
        // start and prepare tx2
        xaLogicalLog.writeStartEntry( tx2 );
        xaLogicalLog.prepare( tx2 );
        // complete tx1
        xaLogicalLog.commitTwoPhase( tx1, 2, ForceMode.forced );
        xaLogicalLog.done( tx1 );
        // start tx1
        xaLogicalLog.writeStartEntry( tx3 );
        // rotate
        xaLogicalLog.rotate();

        // then
        xaLogicalLog.close();
        StoreChannel log = fs.open( new File( dir, "logical.log.2" ), "r" );
        ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE+ Xid.MAXBQUALSIZE * 10 );
        VersionAwareLogEntryReader.readLogHeader( buffer, log, false );
        LogDeserializer deserializer = new LogDeserializer( buffer, null );
        Cursor<LogEntry, IOException> cursor = deserializer.cursor( log );
        @SuppressWarnings("unchecked")
        Consumer<LogEntry, IOException> consumer = mock( Consumer.class );
        when( consumer.accept( any( LogEntry.class ) ) ).thenReturn( true );
        while ( cursor.next( consumer ) );
        ArgumentCaptor<LogEntry> entryCaptor = ArgumentCaptor.forClass( LogEntry.class );
        verify( consumer, times( 7 ) ).accept( entryCaptor.capture() );
        List<LogEntry> entries = entryCaptor.getAllValues();
        assertThat( entries.get( 0 ), isEntry( LogEntry.Start.class, tx2 ) );
        assertThat( entries.get( 1 ), isEntry( LogEntry.Prepare.class, tx2 ) );
        assertThat( entries.get( 2 ), isEntry( LogEntry.Start.class, tx1 ) );
        assertThat( entries.get( 3 ), isEntry( LogEntry.Prepare.class, tx1 ) );
        assertThat( entries.get( 4 ), isEntry( LogEntry.TwoPhaseCommit.class, tx1 ) );
        assertThat( entries.get( 5 ), isEntry( LogEntry.Done.class, tx1 ) );
        assertThat( entries.get( 6 ), isEntry( LogEntry.Start.class, tx3 ) );
    }

    private Matcher<? super LogEntry> isEntry( final Class<? extends LogEntry> entryType, final int txId )
    {
        return new TypeSafeMatcher<LogEntry>(entryType)
        {
            @Override
            protected boolean matchesSafely( LogEntry item )
            {
                return item.getIdentifier() == txId;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( entryType.getSimpleName() ).appendText( " entry with id " ).appendValue( txId );
            }
        };
    }

    @Test
    public void shouldNotReadExcessivelyFromTheFileChannelWhenRotatingLogWithNoOpenTransactions() throws Exception
    {
        // given
        XaTransactionFactory xaTf = mock( XaTransactionFactory.class );
        when( xaTf.getAndSetNewVersion() ).thenAnswer( new TxVersion( TxVersion.UPDATE_AND_GET ) );
        when( xaTf.getCurrentVersion() ).thenAnswer( new TxVersion( TxVersion.GET ) );
        // spy on the file system abstraction so that we can spy on the file channel for the logical log
        FileSystemAbstraction fs = spy( ephemeralFs.get() );
        File dir = TargetDirectory.forTest( fs, XaLogicalLogTest.class ).cleanDirectory( "log" );
        // -- when opening the logical log, spy on the file channel we return and count invocations to channel.read(*)
        when( fs.open( new File( dir, "logical.log.1" ), "rw" ) ).thenAnswer( new Answer<StoreChannel>()
        {
            @Override
            public StoreChannel answer( InvocationOnMock invocation ) throws Throwable
            {
                StoreFileChannel channel = (StoreFileChannel) invocation.callRealMethod();
                return mock( channel.getClass(), withSettings()
                        .spiedInstance( channel )
                        .name( "channel" )
                        .defaultAnswer( CALLS_REAL_METHODS )
                        .invocationListeners( new InvocationListener()
                        {
                            @Override
                            public void reportInvocation( MethodInvocationReport methodInvocationReport )
                            {
                                if ( methodInvocationReport.getInvocation().toString().startsWith( "channel.read(" ) )
                                {
                                    reads++;
                                }
                            }
                        } ) );
            }
        } );
        XaLogicalLog xaLogicalLog = new XaLogicalLog( new File( dir, "logical.log" ),
                                                      mock( XaResourceManager.class ),
                                                      mock( XaCommandReaderFactory.class ),
                                                      mock( XaCommandWriterFactory.class),
                                                      xaTf,
                                                      fs,
                                                      new Monitors(),
                                                      new SingleLoggingService( StringLogger.wrap( output.writer() ) ),
                                                      LogPruneStrategies.NO_PRUNING,
                                                      mock( TransactionStateFactory.class ),
                                                      mock( KernelHealth.class ),
                                                      25 * 1024 * 1024,
                                                      ALLOW_ALL, Functions.<List<LogEntry>>identity(),
                                                      Functions.<List<LogEntry>>identity());
        xaLogicalLog.open();
        // -- set the log up with 10 transactions (with no commands, just start and commit)
        for ( int txId = 1; txId <= 10; txId++ )
        {
            int identifier = xaLogicalLog.start( new XidImpl( getNewGlobalId( DEFAULT_SEED, 0 ), RESOURCE_ID ), -1, 0, 1337 );
            xaLogicalLog.writeStartEntry( identifier );
            xaLogicalLog.commitOnePhase( identifier, txId, ForceMode.forced );
            xaLogicalLog.done( identifier );
        }

        // when
        xaLogicalLog.rotate();

        // then
        assertThat( "should not read excessively from the logical log file channel", reads, lessThan( 10 ) );
    }

    @Test
    public void shouldRespectCustomLogRotationThreshold() throws Exception
    {
        // GIVEN
        long maxSize = 1000;
        ephemeralFs.get().mkdir( new File( "asd" ) );
        XaLogicalLog log = new XaLogicalLog( new File( "asd/log" ),
                mock( XaResourceManager.class ),
                XaCommandReaderFactory.DEFAULT,
                new XaCommandWriterFactory()
                {
                    @Override
                    public XaCommandWriter newInstance()
                    {
                        return new FixedSizeXaCommandWriter();
                    }
                },
                new VersionRespectingXaTransactionFactory(),
                ephemeralFs.get(),
                new Monitors(),
                new DevNullLoggingService(),
                NO_PRUNING,
                mock( TransactionStateFactory.class ),
                mock( KernelHealth.class ),
                maxSize,
                ALLOW_ALL, Functions.<List<LogEntry>>identity(), Functions.<List<LogEntry>>identity() );
        log.open();
        long initialLogVersion = log.getHighestLogVersion();

        // WHEN
        for ( int i = 0; i < 10; i++ )
        {
            int identifier = log.start( xid, -1, -1, 1337 );
            log.writeStartEntry( identifier );
            log.writeCommand( new FixedSizeXaCommand( 100 ), identifier );
            log.commitOnePhase( identifier, i+1, forced );
            log.done( identifier );
        }

        // THEN
        assertEquals( initialLogVersion + 1, log.getHighestLogVersion() );
    }

    @Test
    public void shouldDetermineHighestArchivedLogVersionFromFileNamesIfTheyArePresent() throws Exception
    {
        // Given
        int lowAndIncorrectLogVersion = 0;
        EphemeralFileSystemAbstraction fs = ephemeralFs.get();
        File dir = new File( "db" );
        fs.mkdir( dir );
        fs.create( new File( dir, "log.v100" ) ).close();
        fs.create( new File( dir, "log.v101" ) ).close();

        StoreChannel active = fs.create( new File( dir, "log.1" ) );
        ByteBuffer buff = ByteBuffer.allocate( 128 );
        writeLogHeader( buff, lowAndIncorrectLogVersion, 0 );
        active.write( buff );
        active.force( false );
        active.close();

        // When
        XaLogicalLog log = new XaLogicalLog( new File( dir, "log" ),
                mock( XaResourceManager.class ),
                XaCommandReaderFactory.DEFAULT,
                new XaCommandWriterFactory()
                {
                    @Override
                    public XaCommandWriter newInstance()
                    {
                        return new FixedSizeXaCommandWriter();
                    }
                },
                new VersionRespectingXaTransactionFactory(),
                ephemeralFs.get(),
                new Monitors(),
                new DevNullLoggingService(),
                NO_PRUNING,
                mock( TransactionStateFactory.class ), mock( KernelHealth.class ), 10,
                ALLOW_ALL, Functions.<List<LogEntry>>identity(), Functions.<List<LogEntry>>identity());
        log.open();
        log.rotate();

        // Then
        assertThat( fs.fileExists( new File( dir, "log.v102" ) ), equalTo( true ) );
    }

    @Test
    public void shouldNotPrepareAfterKernelPanicHasHappened() throws Exception
    {
        // GIVEN
        File directory = TargetDirectory.forTest( getClass() ).
                cleanDirectory( "shouldNotPrepareAfterKernelPanicHasHappened" );
        Logging mockLogging = mock( Logging.class );
        when( mockLogging.getMessagesLog( Matchers.<Class>any() ) ).thenReturn( mock( StringLogger.class ) );
        KernelHealth health = new KernelHealth( mock( KernelPanicEventGenerator.class ), mockLogging );
        long maxSize = 1000;
        File logFile = new File( directory, "log" );

        RandomAccessFile forCheckingSize = null;
        XaLogicalLog log = null;
        try
        {
            forCheckingSize = new RandomAccessFile( logFile, "rw" );
            log = new XaLogicalLog( logFile,
                    mock( XaResourceManager.class ),
                    XaCommandReaderFactory.DEFAULT,
                    new XaCommandWriterFactory()
                    {
                        @Override
                        public XaCommandWriter newInstance()
                        {
                            return new FixedSizeXaCommandWriter();
                        }
                    },
                    new VersionRespectingXaTransactionFactory(),
                    new DefaultFileSystemAbstraction(),
                    new Monitors(),
                    new DevNullLoggingService(),
                    NO_PRUNING,
                    mock( TransactionStateFactory.class ), health, maxSize, ALLOW_ALL,
                    Functions.<List<LogEntry>>identity(), Functions.<List<LogEntry>>identity() );
            log.open();

            // When
            int identifier = log.start( new XidImpl( XidImpl.getNewGlobalId( DEFAULT_SEED, 1 ), NeoStoreXaDataSource.BRANCH_ID ), -1, -1, -1 );
            log.writeStartEntry( identifier );
            long sizeBeforePanic = forCheckingSize.getChannel().size();
            health.panic( new MockException() );
            try
            {
                log.prepare( identifier );
                fail(); // it should not go through ok
            }
            catch( XAException e )
            {
                assertEquals( MockException.class, e.getCause().getClass() );
            }

            // Then
            assertEquals( sizeBeforePanic, forCheckingSize.getChannel().size() );
        }
        finally
        {
            if ( log != null )
            {
                log.close();
            }
            if ( forCheckingSize != null )
            {
                forCheckingSize.close();
            }
        }
    }

    @Test
    public void shouldNotCommitOnePhaseAfterKernelPanicHasHappened() throws Exception
    {
        // GIVEN
        File directory = TargetDirectory.forTest( getClass() ).
                cleanDirectory( "shouldNotPrepareAfterKernelPanicHasHappened" );
        Logging mockLogging = mock( Logging.class );
        when( mockLogging.getMessagesLog( Matchers.<Class>any() ) ).thenReturn( mock( StringLogger.class ) );
        KernelHealth health = new KernelHealth( mock( KernelPanicEventGenerator.class ), mockLogging );
        long maxSize = 1000;
        File logFile = new File( directory, "log" );

        RandomAccessFile forCheckingSize = null;
        XaLogicalLog log = null;
        try
        {
            forCheckingSize = new RandomAccessFile( logFile, "rw" );
            log = new XaLogicalLog( logFile,
                    mock( XaResourceManager.class ),
                    XaCommandReaderFactory.DEFAULT,
                    new XaCommandWriterFactory()
                    {
                        @Override
                        public XaCommandWriter newInstance()
                        {
                            return new FixedSizeXaCommandWriter();
                        }
                    },
                    new VersionRespectingXaTransactionFactory(),
                    new DefaultFileSystemAbstraction(),
                    new Monitors(),
                    new DevNullLoggingService(),
                    NO_PRUNING,
                    mock( TransactionStateFactory.class ), health, maxSize, ALLOW_ALL,
                    Functions.<List<LogEntry>>identity(), Functions.<List<LogEntry>>identity()  );
            log.open();

            // When
            int identifier = log.start( new XidImpl( XidImpl.getNewGlobalId( DEFAULT_SEED, 1 ), NeoStoreXaDataSource.BRANCH_ID ), -1, -1, -1 );
            log.writeStartEntry( identifier );
            long sizeBeforePanic = forCheckingSize.getChannel().size();
            health.panic( new MockException() );
            try
            {
                log.commitOnePhase( identifier, 2, ForceMode.forced );
                fail(); // it should not go through ok
            }
            catch( XAException e )
            {
                assertEquals( MockException.class, e.getCause().getClass() );
            }

            // Then
            assertEquals( sizeBeforePanic, forCheckingSize.getChannel().size() );
        }
        finally
        {
            if ( log != null )
            {
                log.close();
            }
            if ( forCheckingSize != null )
            {
                forCheckingSize.close();
            }
        }
    }

    @Test
    public void shouldNotCommitTwoPhaseAfterKernelPanicHasHappened() throws Exception
    {
        // GIVEN
        File directory = TargetDirectory.forTest( getClass() ).
                cleanDirectory( "shouldNotPrepareAfterKernelPanicHasHappened" );
        Logging mockLogging = mock( Logging.class );
        when( mockLogging.getMessagesLog( Matchers.<Class>any() ) ).thenReturn( mock( StringLogger.class ) );
        KernelHealth health = new KernelHealth( mock( KernelPanicEventGenerator.class ), mockLogging );
        long maxSize = 1000;
        File logFile = new File( directory, "log" );

        RandomAccessFile forCheckingSize = null;
        XaLogicalLog log = null;
        try
        {
            forCheckingSize = new RandomAccessFile( logFile, "rw" );
            log = new XaLogicalLog( logFile,
                    mock( XaResourceManager.class ),
                    XaCommandReaderFactory.DEFAULT,
                    new XaCommandWriterFactory()
                    {
                        @Override
                        public XaCommandWriter newInstance()
                        {
                            return new FixedSizeXaCommandWriter();
                        }
                    },
                    new VersionRespectingXaTransactionFactory(),
                    new DefaultFileSystemAbstraction(),
                    new Monitors(),
                    new DevNullLoggingService(),
                    NO_PRUNING,
                    mock( TransactionStateFactory.class ), health, maxSize, ALLOW_ALL,
                    Functions.<List<LogEntry>>identity(), Functions.<List<LogEntry>>identity()  );
            log.open();

            // When
            int identifier = log.start( new XidImpl( XidImpl.getNewGlobalId( DEFAULT_SEED, 1 ), NeoStoreXaDataSource.BRANCH_ID ), -1, -1, -1);
            log.writeStartEntry( identifier );
            long sizeBeforePanic = forCheckingSize.getChannel().size();
            health.panic( new MockException() );
            try
            {
                log.commitTwoPhase( identifier, 2, ForceMode.forced );
                fail(); // it should not go through ok
            }
            catch( XAException e )
            {
                assertEquals( MockException.class, e.getCause().getClass() );
            }

            // Then
            assertEquals( sizeBeforePanic, forCheckingSize.getChannel().size() );
        }
        finally
        {
            if ( log != null )
            {
                log.close();
            }
            if ( forCheckingSize != null )
            {
                forCheckingSize.close();
            }
        }
    }

    @Test
    public void shouldMarkTheCurrentLogAsAppliedOnCloseOnlyAfterSuccessOnStoreFlush() throws Exception
    {
        // Given
        XaTransactionFactory xaTf = mock( XaTransactionFactory.class );
        doThrow( IOException.class ).when( xaTf ).flushAll();
        DefaultFileSystemAbstraction fileSystem = mock( DefaultFileSystemAbstraction.class );
        StoreFileChannel channel = mock( StoreFileChannel.class );
        when( channel.write( any( ByteBuffer.class ) ) ).thenReturn( 4 );
        when( channel.isOpen() ).thenReturn( true );
        when( fileSystem.open( any( File.class ), anyString() ) ).thenReturn( channel );
        when( fileSystem.listFiles( any( File.class ) ) ).thenReturn( new File[] {new File("db.tx.log.v1")} );

        XaLogicalLog theLog = new XaLogicalLog( new File("db.tx.log"),
                mock( XaResourceManager.class ),
                XaCommandReaderFactory.DEFAULT,
                new XaCommandWriterFactory()
                {
                    @Override
                    public XaCommandWriter newInstance()
                    {
                        return new FixedSizeXaCommandWriter();
                    }
                },
                xaTf,
                fileSystem,
                new Monitors(),
                new DevNullLoggingService(),
                NO_PRUNING,
                mock( TransactionStateFactory.class ), mock( KernelHealth.class ), 50000, ALLOW_ALL,
                Functions.<List<LogEntry>>identity(), Functions.<List<LogEntry>>identity()  );
        theLog.open();

        reset( channel );
        when( channel.write( any( ByteBuffer.class ) ) ).thenReturn( 4 );
        when( channel.isOpen() ).thenReturn( true );

        // When
        try
        {
            theLog.close();
        }
        catch( IOException e )
        {
            // like, totally expected
        }

        // Then
        verify( channel, times( 1 ) ).isOpen();
        verifyNoMoreInteractions( channel );
    }

    private static class FixedSizeXaCommand extends XaCommand
    {
        private final byte[] data;

        FixedSizeXaCommand( int payloadSize )
        {
            this.data = new byte[payloadSize-2/*2 bytes for describing which size will follow*/];
        }

        public byte[] getData()
        {
            return data;
        }
    }

    private static class FixedSizeXaCommandReader implements XaCommandReader
    {
        private ByteBuffer buffer;

        private FixedSizeXaCommandReader( ByteBuffer buffer )
        {
            this.buffer = buffer;
        }

        @Override
        public XaCommand read( ReadableByteChannel byteChannel ) throws IOException
        {
            short dataSize = IoPrimitiveUtils.readShort( byteChannel, buffer );
            IoPrimitiveUtils.readBytes( byteChannel, new byte[dataSize] );
            return new FixedSizeXaCommand( dataSize );
        }
    }

    private static class FixedSizeXaCommandWriter implements XaCommandWriter
    {

        @Override
        public void write( XaCommand command, LogBuffer buffer ) throws IOException
        {
            FixedSizeXaCommand fixed = (FixedSizeXaCommand) command;
            buffer.putShort( (short) (fixed.getData().length + 2) );
            buffer.put( fixed.getData() );
        }
    }

    private class TxVersion implements Answer<Object>
    {
        private final boolean update;
        public static final boolean UPDATE_AND_GET = true, GET = false;

        TxVersion( boolean update )
        {
            this.update = update;
        }

        @Override
        public Object answer( InvocationOnMock invocation ) throws Throwable
        {
            synchronized ( XaLogicalLogTest.this )
            {
                if ( update )
                {
                    version++;
                }
                return version;
            }
        }
    }

    private static class VersionRespectingXaTransactionFactory extends XaTransactionFactory
    {
        private long currentVersion = 0;

        @Override
        public XaTransaction create( long lastCommittedTxWhenTransactionStarted, TransactionState state)
        {
            return mock( XaTransaction.class );
        }

        @Override
        public void flushAll()
        {   // Nothing to flush
        }

        @Override
        public long getCurrentVersion()
        {
            return currentVersion;
        }

        @Override
        public long getAndSetNewVersion()
        {
            return ++currentVersion;
        }

        @Override
        public void setVersion( long version )
        {
            this.currentVersion = version;
        }

        @Override
        public long getLastCommittedTx()
        {
            return 0;
        }
    }

    public final @Rule EphemeralFileSystemRule ephemeralFs = new EphemeralFileSystemRule();
    public final Xid xid = new XidImpl( "global".getBytes(), "resource".getBytes() );

    private static class MockException extends RuntimeException
    {}
}
