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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_keep_threshold;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_rotation_threshold;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent.NULL;

@DbmsExtension( configurationCallback = "configure" )
public class CheckpointLogFileRotationIT
{
    @Inject
    private GraphDatabaseService database;
    @Inject
    private LogFiles logFiles;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( checkpoint_logical_log_rotation_threshold, kibiBytes( 1 ) ).setConfig( checkpoint_logical_log_keep_threshold, 100 );
    }

    @Test
    void rotateCheckpointLogFiles() throws IOException
    {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        LogPosition logPosition = new LogPosition( 1000, 12345 );
        var reason = "checkpoint for rotation test";
        for ( int i = 0; i < 105; i++ )
        {
            checkpointAppender.checkPoint( NULL, logPosition, Instant.now(), reason );
        }
        var matchedFiles = checkpointFile.getDetachedCheckpointFiles();
        assertThat( matchedFiles ).hasSize( 22 );
        for ( var fileWithCheckpoints : matchedFiles )
        {
            assertThat( fileWithCheckpoints ).satisfies( new Condition<>()
            {
                @Override
                public boolean matches( Path file )
                {
                    long length = file.toFile().length();
                    return length == kibiBytes( 1 ) || length == CURRENT_FORMAT_LOG_HEADER_SIZE;
                }
            } );
        }
    }

    @Test
    void doNotRotateWhileCheckpointsAreFitting() throws IOException
    {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        LogPosition logPosition = new LogPosition( 1000, 12345 );
        var reason = "checkpoint for rotation test";
        for ( int i = 0; i < 4; i++ )
        {
            checkpointAppender.checkPoint( NULL, logPosition, Instant.now(), reason );
        }
        assertThat( checkpointFile.getDetachedCheckpointFiles() ).hasSize( 1 );
    }

    @Test
    void afterRotationNewFileHaveHeader() throws IOException
    {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        LogPosition logPosition = new LogPosition( 1000, 12345 );
        var reason = "checkpoint for rotation test";
        for ( int i = 0; i < 5; i++ )
        {
            checkpointAppender.checkPoint( NULL, logPosition, Instant.now(), reason );
        }
        Path[] matchedFiles = checkpointFile.getDetachedCheckpointFiles();
        assertThat( matchedFiles ).hasSize( 2 );
        boolean headerFileFound = false;
        for ( Path matchedFile : matchedFiles )
        {
            if ( checkpointFile.getDetachedCheckpointLogFileVersion( matchedFile ) == 1 )
            {
                assertThat( matchedFile.toFile() ).hasSize( CURRENT_FORMAT_LOG_HEADER_SIZE );
                headerFileFound = true;
            }
        }
        assertTrue( headerFileFound );
    }
}
