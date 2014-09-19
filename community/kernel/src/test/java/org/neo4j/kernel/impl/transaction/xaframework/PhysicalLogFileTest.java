/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile.Monitor;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.xaframework.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.readLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.writeLogHeader;

public class PhysicalLogFileTest
{
    @Test
    public void shouldOpenInFreshDirectoryAndFinallyAddHeader() throws Exception
    {
        // GIVEN
        String name = "log";
        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        LifeSupport life = new LifeSupport();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), name, fs );
        life.add( new PhysicalLogFile( fs, logFiles, 1000, LogPruneStrategyFactory.NO_PRUNING,
                transactionIdStore, logVersionRepository, mock( Monitor.class ), logRotationControl,
                new TransactionMetadataCache( 10, 100 ), NO_RECOVERY_EXPECTED ));

        // WHEN
        life.start();
        life.shutdown();

        // THEN
        File file = new PhysicalLogFiles( directory.directory(), name, fs ).getLogFileForVersion( 1L );
        LogHeader header = readLogHeader( fs, file );
        assertEquals( 1L, header.logVersion );
        assertEquals( 5L, header.lastCommittedTxId );
    }

    @Test
    public void shouldWriteSomeDataIntoTheLog() throws Exception
    {
        // GIVEN
        String name = "log";
        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        LifeSupport life = new LifeSupport();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), name, fs );
        Monitor monitor = mock( Monitor.class );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 1000,
                LogPruneStrategyFactory.NO_PRUNING,
                transactionIdStore, logVersionRepository, monitor, logRotationControl,
                new TransactionMetadataCache( 10, 100 ), NO_RECOVERY_EXPECTED ) );

        // WHEN
        try
        {
            life.start();
            verify( monitor ).recoveryCompleted();

            WritableLogChannel writer = logFile.getWriter();
            LogPositionMarker positionMarker = new LogPositionMarker();
            writer.getCurrentPosition( positionMarker );
            int intValue = 45;
            long longValue = 4854587;
            writer.putInt( intValue );
            writer.putLong( longValue );
            writer.emptyBufferIntoChannelAndClearIt();
            writer.force();

            // THEN
            try ( ReadableLogChannel reader = logFile.getReader( positionMarker.newPosition() ) )
            {
                assertEquals( intValue, reader.getInt() );
                assertEquals( longValue, reader.getLong() );
            }
        }
        finally
        {
            life.shutdown();
        }
    }

    @Test
    public void shouldReadOlderLogs() throws Exception
    {
        // GIVEN
        String name = "log";
        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        LifeSupport life = new LifeSupport();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), name, fs );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 50, LogPruneStrategyFactory.NO_PRUNING,
                transactionIdStore, logVersionRepository, mock( Monitor.class ), logRotationControl,
                new TransactionMetadataCache( 10, 100 ), NO_RECOVERY_EXPECTED ) );

        // WHEN
        life.start();
        try
        {
            WritableLogChannel writer = logFile.getWriter();
            LogPositionMarker positionMarker = new LogPositionMarker();
            writer.getCurrentPosition( positionMarker );
            LogPosition position1 = positionMarker.newPosition();
            int intValue = 45;
            long longValue = 4854587;
            byte[] someBytes = someBytes( 40 );
            writer.putInt( intValue );
            writer.putLong( longValue );
            writer.put( someBytes, someBytes.length );
            writer.emptyBufferIntoChannelAndClearIt();
            writer.force();
            writer.getCurrentPosition( positionMarker );
            LogPosition position2 = positionMarker.newPosition();
            long longValue2 = 123456789L;
            writer.putLong( longValue2 );
            writer.put( someBytes, someBytes.length );
            writer.emptyBufferIntoChannelAndClearIt();
            writer.force();

            // THEN
            try ( ReadableLogChannel reader = logFile.getReader( position1 ) )
            {
                assertEquals( intValue, reader.getInt() );
                assertEquals( longValue, reader.getLong() );
                assertArrayEquals( someBytes, readBytes( reader, 40 ) );
            }
            try ( ReadableLogChannel reader = logFile.getReader( position2 ) )
            {
                assertEquals( longValue2, reader.getLong() );
                assertArrayEquals( someBytes, readBytes( reader, 40 ) );
            }
        }
        finally
        {
            life.shutdown();
        }
    }

    @Test
    public void shouldRecoverExistingData() throws Exception
    {
        String name = "log";
        File file = new File( directory.directory(), name + ".1" );
        final int logVersion = 1;
        writeSomeData( file, new Visitor<ByteBuffer, IOException>()
        {
            @Override
            public boolean visit( ByteBuffer buffer ) throws IOException
            {
                writeLogHeader( buffer, logVersion, 3 );
                buffer.clear();
                buffer.position( LOG_HEADER_SIZE );
                buffer.put( (byte) 2 );
                buffer.putInt( 23324 );
                return true;
            }
        } );

        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        LifeSupport life = new LifeSupport();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), name, fs );
        Monitor monitor = mock( Monitor.class );
        life.add( new PhysicalLogFile( fs, logFiles, 50, LogPruneStrategyFactory.NO_PRUNING,
                transactionIdStore, logVersionRepository, monitor, logRotationControl,
                new TransactionMetadataCache( 10, 100 ), new Visitor<ReadableVersionableLogChannel, IOException>()
                        {
                            @Override
                            public boolean visit( ReadableVersionableLogChannel element ) throws IOException
                            {
                                assertEquals( (byte) 2, element.get() );
                                assertEquals( 23324, element.getInt() );
                                try
                                {
                                    element.get();
                                    fail( "There should be no more" );
                                }
                                catch ( ReadPastEndException e )
                                {   // Good
                                }
                                return true;
                            }
                        } ) );
        try
        {
            life.start();
            InOrder order = inOrder( monitor );
            order.verify( monitor, times( 1 ) ).recoveryRequired( logVersion );
            order.verify( monitor, times( 1 ) ).recoveryCompleted();
        }
        finally
        {
            life.shutdown();
        }
    }

    private void writeSomeData( File file, Visitor<ByteBuffer, IOException> visitor ) throws IOException
    {
        try ( StoreChannel channel = fs.open( file, "rw" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( 1024 );
            visitor.visit( buffer );
            buffer.flip();
            channel.write( buffer );
        }
    }

    private byte[] readBytes( ReadableLogChannel reader, int length ) throws IOException
    {
        byte[] result = new byte[length];
        reader.get( result, length );
        return result;
    }

    private byte[] someBytes( int length )
    {
        byte[] result = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            result[i] = (byte) (i%5);
        }
        return result;
    }

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final LogVersionRepository logVersionRepository = new DeadSimpleLogVersionRepository( 1L );
    private final TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 5L );
    private static final Visitor<ReadableVersionableLogChannel, IOException> NO_RECOVERY_EXPECTED =
            new Visitor<ReadableVersionableLogChannel, IOException>()
            {
        @Override
        public boolean visit( ReadableVersionableLogChannel element ) throws IOException
        {
            fail( "No recovery expected" );
            return false;
        }
    };
}
