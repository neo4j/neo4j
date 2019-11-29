/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.PositionableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.fs.ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;

@DbmsExtension
class VersionAwareLogEntryReaderIT
{
    // this offset includes log header and transaction that create node on test setup
    private static final long END_OF_DATA_OFFSET = CURRENT_FORMAT_LOG_HEADER_SIZE + 131L;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DatabaseManagementService managementService;
    private DatabaseLayout databaseLayout;
    private final VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader();

    @BeforeEach
    void setUp()
    {
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        createNode( database );
        databaseLayout = ((GraphDatabaseAPI) database).databaseLayout();
        managementService.shutdown();
    }

    @Test
    @EnabledOnOs( OS.LINUX )
    void readOnlyLogFilesWhileCommandsAreAvailable() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withLogEntryReader( entryReader )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            assertEquals( kibiBytes( 128 ), logFiles.getHighestLogFile().length() );
            LogPosition logPosition = entryReader.lastPosition();
            assertEquals( 0L, logPosition.getLogVersion() );
            // this position in a log file before 0's are actually starting
            assertEquals( END_OF_DATA_OFFSET, logPosition.getByteOffset() );
        }
    }

    @Test
    void correctlyResetPositionWhenEndOfCommandsReached() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withLogEntryReader( entryReader )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            LogPosition logPosition = entryReader.lastPosition();
            assertEquals( 0L, logPosition.getLogVersion() );
            // this position in a log file before 0's are actually starting
            assertEquals( END_OF_DATA_OFFSET, logPosition.getByteOffset() );

            for ( int i = 0; i < 10; i++ )
            {
                assertEquals( END_OF_DATA_OFFSET, getLastReadablePosition( logFiles ) );
            }
        }
    }

    @Test
    void correctlyResetPositionOfObservedZeroWhenChannelSwitchOnExactlyCheckedByte() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withLogEntryReader( entryReader )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            LogPositionMarker positionMarker = new LogPositionMarker();
            LogFile logFile = logFiles.getLogFile();
            long initialPosition = getLastReadablePosition( logFiles );
            long checkpointsEndDataOffset = DEFAULT_READ_AHEAD_SIZE + initialPosition;

            FlushablePositionAwareChecksumChannel writer = logFile.getWriter();
            TransactionLogWriter logWriter = new TransactionLogWriter( new LogEntryWriter( writer ) );
            do
            {
                logWriter.checkPoint( new LogPosition( 4, 5 ) );
                writer.getCurrentPosition( positionMarker );
            }
            while ( positionMarker.getByteOffset() <= checkpointsEndDataOffset );
            writer.prepareForFlush().flush();
            logFiles.getLogFile().rotate();
            fs.truncate( logFiles.getLogFileForVersion( 0 ), checkpointsEndDataOffset );

            try ( StoreChannel storeChannel = fs.write( logFiles.getLogFileForVersion( 1 ) ) )
            {
                storeChannel.position( CURRENT_FORMAT_LOG_HEADER_SIZE );
                storeChannel.write( ByteBuffer.wrap( new byte[]{0} ) );
            }

            try ( ReadableLogChannel logChannel = logFiles.getLogFile().getReader( new LogPosition( 0, initialPosition ) ) )
            {
                LogEntry logEntry = entryReader.readLogEntry( logChannel );
                // we reading expected checkpoint records
                assertThat( logEntry ).isInstanceOf( CheckPoint.class );
                CheckPoint checkPoint = (CheckPoint) logEntry;
                LogPosition logPosition = checkPoint.getLogPosition();
                assertEquals( 4, logPosition.getLogVersion() );
                assertEquals( 5, logPosition.getByteOffset() );

                // set positon to the end of the buffer to cause channel switch on next byte read
                ((PositionableChannel) logChannel).setCurrentPosition( checkpointsEndDataOffset );

                while ( entryReader.readLogEntry( logChannel ) != null )
                {
                    // read to the end
                }
                // channel should be switched now and position should be just after the header
                LogPosition position = entryReader.lastPosition();
                assertEquals( CURRENT_FORMAT_LOG_HEADER_SIZE, position.getByteOffset() );
                assertEquals( 1, position.getLogVersion() );
            }
        }
    }

    @Test
    @DisabledOnOs( OS.LINUX )
    void readTillTheEndOfNotPreallocatedFile() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withLogEntryReader( entryReader )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            LogPosition logPosition = entryReader.lastPosition();
            assertEquals( 0L, logPosition.getLogVersion() );
            assertEquals( logFiles.getHighestLogFile().length(), logPosition.getByteOffset() );
        }
    }

    private long getLastReadablePosition( LogFiles logFiles ) throws IOException
    {
        try ( ReadableLogChannel logChannel = logFiles.getLogFile().getReader( logFiles.extractHeader( 0 ).getStartPosition() ) )
        {
            while ( entryReader.readLogEntry( logChannel ) != null )
            {
                // read to the end
            }
            return entryReader.lastPosition().getByteOffset();
        }
    }

    private void createNode( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.createNode();
            transaction.commit();
        }
    }
}
