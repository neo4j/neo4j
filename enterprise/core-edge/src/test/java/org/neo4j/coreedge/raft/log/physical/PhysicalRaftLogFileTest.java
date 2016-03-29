/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.log.physical;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.DeadSimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.Monitor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

public class PhysicalRaftLogFileTest
{
    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final LogVersionRepository logVersionRepository = new DeadSimpleLogVersionRepository( 1L );
    private final TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 5L, 0, 0, 0 );


    @Test
    public void shouldOpenInFreshDirectoryAndFinallyAddHeader() throws Exception
    {
        // GIVEN
        String name = "log";
        LifeSupport life = new LifeSupport();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), name, fs );
        life.add( new PhysicalLogFile( fs, logFiles, 1000, transactionIdStore::getLastCommittedTransactionId,
                logVersionRepository, mock( Monitor.class ), new LogHeaderCache( 10 ) ) );

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
        LifeSupport life = new LifeSupport();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), name, fs );
        Monitor monitor = mock( Monitor.class );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 1000,
                transactionIdStore::getLastCommittedTransactionId, logVersionRepository, monitor,
                new LogHeaderCache( 10 ) ) );

        // WHEN
        try
        {
            life.start();

            FlushablePositionAwareChannel writer = logFile.getWriter();
            LogPositionMarker positionMarker = new LogPositionMarker();
            writer.getCurrentPosition( positionMarker );
            int intValue = 45;
            long longValue = 4854587;
            writer.putInt( intValue );
            writer.putLong( longValue );
            writer.prepareForFlush().flush();

            // THEN
            try ( ReadableClosableChannel reader = logFile.getReader( positionMarker.newPosition() ) )
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
        LifeSupport life = new LifeSupport();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), name, fs );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 50,
                transactionIdStore::getLastCommittedTransactionId, logVersionRepository, mock( Monitor.class ),
                new LogHeaderCache( 10 ) ) );

        // WHEN
        life.start();
        try
        {
            FlushablePositionAwareChannel writer = logFile.getWriter();
            LogPositionMarker positionMarker = new LogPositionMarker();
            writer.getCurrentPosition( positionMarker );
            LogPosition position1 = positionMarker.newPosition();
            int intValue = 45;
            long longValue = 4854587;
            byte[] someBytes = someBytes( 40 );
            writer.putInt( intValue );
            writer.putLong( longValue );
            writer.put( someBytes, someBytes.length );
            writer.prepareForFlush().flush();
            writer.getCurrentPosition( positionMarker );
            LogPosition position2 = positionMarker.newPosition();
            long longValue2 = 123456789L;
            writer.putLong( longValue2 );
            writer.put( someBytes, someBytes.length );
            writer.prepareForFlush().flush();

            // THEN
            try ( ReadableClosableChannel reader = logFile.getReader( position1 ) )
            {
                assertEquals( intValue, reader.getInt() );
                assertEquals( longValue, reader.getLong() );
                assertArrayEquals( someBytes, readBytes( reader, 40 ) );
            }
            try ( ReadableClosableChannel reader = logFile.getReader( position2 ) )
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
    public void shouldVisitLogFile() throws Exception
    {
        // GIVEN
        String name = "log";
        LifeSupport life = new LifeSupport();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), name, fs );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 50,
                transactionIdStore::getLastCommittedTransactionId, logVersionRepository, mock( Monitor.class ),
                new LogHeaderCache( 10 ) ) );
        life.start();
        FlushablePositionAwareChannel writer = logFile.getWriter();
        LogPositionMarker mark = new LogPositionMarker();
        writer.getCurrentPosition( mark );
        for ( int i = 0; i < 5; i++ )
        {
            writer.put( (byte)i );
        }
        writer.prepareForFlush();

        // WHEN/THEN
        final AtomicBoolean called = new AtomicBoolean();
        logFile.accept( ( position, channel ) -> {
            for ( int i = 0; i < 5; i++ )
            {
                assertEquals( (byte)i, channel.get() );
            }
            called.set( true );
            return true;
        }, mark.newPosition() );
        assertTrue( called.get() );
        life.shutdown();
    }

    private byte[] readBytes( ReadableClosableChannel reader, int length ) throws IOException
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
}
