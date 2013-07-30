/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import javax.transaction.xa.Xid;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.listeners.InvocationListener;
import org.mockito.listeners.MethodInvocationReport;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.FailureOutput;
import org.neo4j.test.TargetDirectory;

import static org.neo4j.kernel.impl.transaction.xaframework.ForceMode.forced;
import static org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies.NO_PRUNING;

public class XaLogicalLogTest
{
    @Rule
    public final FailureOutput output = new FailureOutput();
    private static final byte[] RESOURCE_ID = new byte[]{0x00, (byte) 0x99, (byte) 0xcc};
    private long version;
    private int reads;

    @Test
    public void shouldNotReadExcessivelyFromTheFileChannelWhenRotatingLogWithNoOpenTransactions() throws Exception
    {
        // given
        XaTransactionFactory xaTf = mock( XaTransactionFactory.class );
        when( xaTf.getAndSetNewVersion() ).thenAnswer( new TxVersion( TxVersion.UPDATE_AND_GET ) );
        when( xaTf.getCurrentVersion() ).thenAnswer( new TxVersion( TxVersion.GET ) );
        // spy on the file system abstraction so that we can spy on the file channel for the logical log
        FileSystemAbstraction fs = spy( ephemeralFs.get() );
        File dir = TargetDirectory.forTest( fs, XaLogicalLogTest.class ).directory( "log", true );
        // -- when opening the logical log, spy on the file channel we return and count invocations to channel.read(*)
        when( fs.open( new File( dir, "logical.log.1" ), "rw" ) ).thenAnswer( new Answer<FileChannel>()
        {
            @Override
            public FileChannel answer( InvocationOnMock invocation ) throws Throwable
            {
                FileChannel channel = (FileChannel) invocation.callRealMethod();
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
                                                      mock( XaCommandFactory.class ),
                                                      xaTf,
                                                      new DefaultLogBufferFactory(),
                                                      fs,
                                                      new SingleLoggingService( StringLogger.wrap( output.writer() ) ),
                                                      LogPruneStrategies.NO_PRUNING,
                                                      mock( TransactionStateFactory.class ),
                                                      25 * 1024 * 1024 );
        xaLogicalLog.open();
        // -- set the log up with 10 transactions (with no commands, just start and commit)
        for ( int txId = 1; txId <= 10; txId++ )
        {
            int identifier = xaLogicalLog.start( new XidImpl( XidImpl.getNewGlobalId(), RESOURCE_ID ), -1, 0 );
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
        XaLogicalLog log = new XaLogicalLog( new File( "log" ), 
                mock( XaResourceManager.class ),
                new FixedSizeXaCommandFactory(),
                new VersionRespectingXaTransactionFactory(),
                new DefaultLogBufferFactory(),
                ephemeralFs.get(),
                new DevNullLoggingService(),
                NO_PRUNING,
                mock( TransactionStateFactory.class ), maxSize );
        log.open();
        long initialLogVersion = log.getHighestLogVersion();
        
        // WHEN
        for ( int i = 0; i < 10; i++ )
        {
            int identifier = log.start( xid, -1, -1 );
            log.writeStartEntry( identifier );
            log.writeCommand( new FixedSizeXaCommand( 100 ), identifier );
            log.commitOnePhase( identifier, i+1, forced );
            log.done( identifier );
        }
        
        // THEN
        assertEquals( initialLogVersion+1, log.getHighestLogVersion() );
    }
    
    private static class FixedSizeXaCommand extends XaCommand
    {
        private byte[] data;

        FixedSizeXaCommand( int payloadSize )
        {
            this.data = new byte[payloadSize-2/*2 bytes for describing which size will follow*/];
        }
        
        @Override
        public void execute()
        {   // There's nothing to execute
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            buffer.putShort( (short) (data.length+2) );
            buffer.put( data );
        }
    }
    
    private static class FixedSizeXaCommandFactory extends XaCommandFactory
    {
        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) throws IOException
        {
            short dataSize = IoPrimitiveUtils.readShort( byteChannel, buffer );
            IoPrimitiveUtils.readBytes( byteChannel, new byte[dataSize] );
            return new FixedSizeXaCommand( dataSize );
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
        public XaTransaction create( int identifier, TransactionState state )
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
}
