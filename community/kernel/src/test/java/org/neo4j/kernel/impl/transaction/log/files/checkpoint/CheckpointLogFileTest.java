/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.PanicEventGenerator;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent.NULL;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

@Neo4jLayoutExtension
@ExtendWith( LifeExtension.class )
class CheckpointLogFileTest
{
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private LifeSupport life;

    private final long rotationThreshold = ByteUnit.mebiBytes( 1 );
    private final DatabaseHealth databaseHealth = new DatabaseHealth( PanicEventGenerator.NO_OP, NullLog.getInstance() );
    private final LogVersionRepository logVersionRepository = new SimpleLogVersionRepository( 1L );
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore( 2L, 0, BASE_TX_COMMIT_TIMESTAMP, 0, 0 );
    private CheckpointFile checkpointFile;

    @BeforeEach
    void setUp() throws IOException
    {
        LogFiles logFiles = buildLogFiles();
        life.add( logFiles );
        life.start();
        checkpointFile = logFiles.getCheckpointFile();
    }

    @Test
    void failToWriteCheckpointAfterShutdown()
    {
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        assertDoesNotThrow( () -> checkpointAppender.checkPoint( NULL, new LogPosition( 1, 2 ), Instant.now(), "test" ) );

        life.shutdown();

        assertThrows( Exception.class, () -> checkpointAppender.checkPoint( NULL, new LogPosition( 1, 2 ), Instant.now(), "test" ) );
    }

    @Test
    void provideMatchedCheckpointFiles()
    {
        Path[] matchedFiles = checkpointFile.getMatchedFiles();
        assertThat( matchedFiles ).hasSize( 1 );
        assertEquals( matchedFiles[0], checkpointFile.getCurrentFile() );
    }

    @Test
    void latestCheckpointLookupLastCheckpoint() throws IOException
    {
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        assertThat( checkpointFile.findLatestCheckpoint() ).isEmpty();

        var firstLogPosition = new LogPosition( 1, 2 );
        checkpointAppender.checkPoint( NULL, firstLogPosition, Instant.now(), "test" );
        assertEquals( firstLogPosition, checkpointFile.findLatestCheckpoint().orElseThrow().getLogPosition() );

        var secondLogPosition = new LogPosition( 2, 3 );
        checkpointAppender.checkPoint( NULL, secondLogPosition, Instant.now(), "test" );
        assertEquals( secondLogPosition, checkpointFile.findLatestCheckpoint().orElseThrow().getLogPosition() );

        var thirdLogPosition = new LogPosition( 3, 4 );
        checkpointAppender.checkPoint( NULL, thirdLogPosition, Instant.now(), "test" );
        assertEquals( thirdLogPosition, checkpointFile.findLatestCheckpoint().orElseThrow().getLogPosition() );

        var checkpointInfos = checkpointFile.reachableCheckpoints();
        assertThat( checkpointInfos ).hasSize( 3 );
        assertThat( checkpointInfos.get( 0 ) ).hasFieldOrPropertyWithValue( "logPosition", firstLogPosition );
        assertThat( checkpointInfos.get( 1 ) ).hasFieldOrPropertyWithValue( "logPosition", secondLogPosition );
        assertThat( checkpointInfos.get( 2 ) ).hasFieldOrPropertyWithValue( "logPosition", thirdLogPosition );
    }

    @Test
    void findAllCheckpoints() throws IOException
    {
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        assertThat( checkpointFile.reachableCheckpoints() ).isEmpty();

        checkpointAppender.checkPoint( NULL, new LogPosition( 1, 2 ), Instant.now(), "test" );
        assertThat( checkpointFile.reachableCheckpoints() ).hasSize( 1 );

        checkpointAppender.checkPoint( NULL, new LogPosition( 2, 3 ), Instant.now(), "test" );
        assertThat( checkpointFile.reachableCheckpoints() ).hasSize( 2 );

        checkpointAppender.checkPoint( NULL, new LogPosition( 3, 4 ), Instant.now(), "test" );
        assertThat( checkpointFile.reachableCheckpoints() ).hasSize( 3 );
    }

    private LogFiles buildLogFiles() throws IOException
    {
        return LogFilesBuilder.builder( databaseLayout, fileSystem )
                .withRotationThreshold( rotationThreshold )
                .withTransactionIdStore( transactionIdStore )
                .withSeparateFilesForCheckpoint( true )
                .withDatabaseHealth( databaseHealth )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
    }
}
