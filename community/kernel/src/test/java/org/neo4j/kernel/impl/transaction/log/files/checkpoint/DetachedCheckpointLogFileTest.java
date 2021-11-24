/*
 * Copyright (c) "Neo4j"
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
import java.time.Instant;
import java.util.List;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.PanicEventGenerator;
import org.neo4j.storageengine.api.KernelVersionRepository;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent.NULL;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

@Neo4jLayoutExtension
@ExtendWith( LifeExtension.class )
class DetachedCheckpointLogFileTest
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
    private LogFiles logFiles;
    private final FakeKernelVersionProvider versionProvider = new FakeKernelVersionProvider();

    @BeforeEach
    void setUp() throws IOException
    {
        logFiles = buildLogFiles();
        life.add( logFiles );
        life.start();
        checkpointFile = logFiles.getCheckpointFile();
    }

    @Test
    void findLogTailShouldWorkForDetachedCheckpoints() throws IOException
    {
        LogPosition logPosition = new LogPosition( logVersionRepository.getCurrentLogVersion(), CURRENT_FORMAT_LOG_HEADER_SIZE );
        checkpointFile.getCheckpointAppender().checkPoint( NULL, logPosition, Instant.now(), "detached" );
        CheckpointInfo lastCheckPoint = buildLogFiles().getTailInformation().lastCheckPoint;
        assertThat( lastCheckPoint.getTransactionLogPosition() ).isEqualTo( logPosition );
    }

    @Test
    void findLatestCheckpointShouldWorkForDetachedCheckpoints() throws IOException
    {
        // Should find the detached checkpoint first
        LogPosition logPosition2 = new LogPosition( logVersionRepository.getCurrentLogVersion(), CURRENT_FORMAT_LOG_HEADER_SIZE );
        checkpointFile.getCheckpointAppender().checkPoint( NULL, logPosition2, Instant.now(), "detached" );
        assertThat( checkpointFile.findLatestCheckpoint().orElseThrow().getTransactionLogPosition() ).isEqualTo( logPosition2 );
    }

    @Test
    void shouldFindReachableCheckpointsForDetachedCheckpoints() throws IOException
    {
        assertThat( checkpointFile.reachableCheckpoints() ).isEmpty();
        assertThat( checkpointFile.getReachableDetachedCheckpoints() ).isEmpty();

        // Add detached checkpoints
        LogPosition logPosition = new LogPosition( 0, 3 );
        LogPosition logPosition1 = new LogPosition( 0, 4 );
        checkpointFile.getCheckpointAppender().checkPoint( NULL, logPosition, Instant.now(), "detached" );
        checkpointFile.getCheckpointAppender().checkPoint( NULL, logPosition1, Instant.now(), "detached" );

        List<CheckpointInfo> reachableCheckpoints = checkpointFile.reachableCheckpoints();
        assertThat( reachableCheckpoints.size() ).isEqualTo( 2 );
        assertThat( reachableCheckpoints.get( 0 ).getTransactionLogPosition() ).isEqualTo( logPosition );
        assertThat( reachableCheckpoints.get( 1 ).getTransactionLogPosition() ).isEqualTo( logPosition1 );
        List<CheckpointInfo> detachedCheckpoints = checkpointFile.getReachableDetachedCheckpoints();
        assertThat( detachedCheckpoints.size() ).isEqualTo( 2 );
        assertThat( detachedCheckpoints.get( 0 ).getTransactionLogPosition() ).isEqualTo( logPosition );
        assertThat( detachedCheckpoints.get( 1 ).getTransactionLogPosition() ).isEqualTo( logPosition1 );
    }

    private LogFiles buildLogFiles() throws IOException
    {
        return LogFilesBuilder.builder( databaseLayout, fileSystem )
                .withRotationThreshold( rotationThreshold )
                .withTransactionIdStore( transactionIdStore )
                .withDatabaseHealth( databaseHealth )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .withStoreId( StoreId.UNKNOWN )
                .withKernelVersionProvider( versionProvider )
                .build();
    }

    private static class FakeKernelVersionProvider implements KernelVersionRepository
    {
        volatile KernelVersion version = KernelVersion.LATEST;

        @Override
        public KernelVersion kernelVersion()
        {
            return version;
        }
    }
}
