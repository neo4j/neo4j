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

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile.Monitor;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class PhysicalLogFileTest
{
    @Test
    public void shouldOpenInFreshDirectoryAndFinallyAddHeader() throws Exception
    {
        // GIVEN
        String name = "log";
        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        LogFile logFile = new PhysicalLogFile( fs, directory.directory(), name, 1000, LogPruneStrategies.NO_PRUNING,
                transactionIdStore, mock( Monitor.class ), logRotationControl );
        logFile.open( NO_RECOVERY_EXPECTED );

        // WHEN
        logFile.close();

        // THEN
        File file = new PhysicalLogFiles( directory.directory(), name, fs ).getHistoryFileName( 1L );
        long[] header = VersionAwareLogEntryReader.readLogHeader( fs, file );
        assertEquals( 1L, header[0] );
        assertEquals( 5L, header[1] );
    }

    @Test
    public void shouldWriteSomeDataIntoTheLog() throws Exception
    {
        // GIVEN
        String name = "log";
        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        LogFile logFile = new PhysicalLogFile( fs, directory.directory(), name, 1000, LogPruneStrategies.NO_PRUNING,
                transactionIdStore, mock( Monitor.class ), logRotationControl );
        logFile.open( NO_RECOVERY_EXPECTED );

        // WHEN
        WritableLogChannel writer = logFile.getWriter();
        LogPosition position = writer.getCurrentPosition();
        int intValue = 45;
        long longValue = 4854587;
        writer.putInt( intValue );
        writer.putLong( longValue );
        writer.force();

        // THEN
        try ( ReadableLogChannel reader = logFile.getReader( position ) )
        {
            assertEquals( intValue, reader.getInt() );
            assertEquals( longValue, reader.getLong() );
        }
        logFile.close();
    }

    @Test
    public void shouldReadOlderLogs() throws Exception
    {
        // GIVEN
        String name = "log";
        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        LogFile logFile = new PhysicalLogFile( fs, directory.directory(), name, 50, LogPruneStrategies.NO_PRUNING,
                transactionIdStore, mock( Monitor.class ), logRotationControl );
        logFile.open( NO_RECOVERY_EXPECTED );

        // WHEN
        WritableLogChannel writer = logFile.getWriter();
        LogPosition position1 = writer.getCurrentPosition();
        int intValue = 45;
        long longValue = 4854587;
        byte[] someBytes = someBytes( 40 );
        writer.putInt( intValue );
        writer.putLong( longValue );
        writer.put( someBytes, someBytes.length );
        writer.force();
        LogPosition position2 = writer.getCurrentPosition();
        long longValue2 = 123456789L;
        writer.putLong( longValue2 );
        writer.put( someBytes, someBytes.length );
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
        logFile.close();
    }

    @Test
    public void shouldRecoverExistingData() throws Exception
    {
        String name = "log";
        File file = new File( directory.directory(), name + ".1" );
        writeSomeData( file, new Visitor<ByteBuffer, IOException>()
        {
            @Override
            public boolean visit( ByteBuffer buffer ) throws IOException
            {
                VersionAwareLogEntryReader.writeLogHeader( buffer, 1, 3 );
                buffer.clear();
                buffer.position( VersionAwareLogEntryReader.LOG_HEADER_SIZE );
                buffer.put( (byte) 2 );
                buffer.putInt( 23324 );
                return true;
            }
        } );

        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        LogFile logFile = new PhysicalLogFile( fs, directory.directory(), name, 50, LogPruneStrategies.NO_PRUNING,
                transactionIdStore, mock( Monitor.class ), logRotationControl );
        logFile.open( new Visitor<ReadableLogChannel, IOException>()
        {
            @Override
            public boolean visit( ReadableLogChannel element ) throws IOException
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
        } );
        logFile.close();
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
    private final TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 1L, 5L );
    private static final Visitor<ReadableLogChannel, IOException> NO_RECOVERY_EXPECTED =
            new Visitor<ReadableLogChannel, IOException>()
    {
        @Override
        public boolean visit( ReadableLogChannel element ) throws IOException
        {
            fail( "No recovery expected" );
            return false;
        }
    };
}
