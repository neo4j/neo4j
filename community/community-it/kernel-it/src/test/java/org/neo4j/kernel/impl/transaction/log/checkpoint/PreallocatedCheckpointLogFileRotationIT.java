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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import org.neo4j.kernel.impl.transaction.log.LogPosition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent.NULL;

@EnabledOnOs( OS.LINUX )
class PreallocatedCheckpointLogFileRotationIT extends CheckpointLogFileRotationIT
{
    protected boolean preallocateLogs()
    {
        return true;
    }

    protected long expectedNewFileSize()
    {
        return ROTATION_THRESHOLD;
    }

    @Test
    void writeCheckpointsIntoPreallocatedFile() throws IOException
    {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        LogPosition logPosition = new LogPosition( 1000, 12345 );
        var reason = "checkpoints in preallocated file";
        for ( int i = 0; i < 3; i++ )
        {
            checkpointAppender.checkPoint( NULL, logPosition, Instant.now(), reason );
        }
        var matchedFiles = checkpointFile.getDetachedCheckpointFiles();
        assertThat( matchedFiles ).hasSize( 1 );
    }

    @Test
    void writeCheckpointsIntoSeveralPreallocatedFiles() throws IOException
    {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        LogPosition logPosition = new LogPosition( 1000, 12345 );
        var reason = "checkpoint in preallocated file";
        for ( int i = 0; i < 32; i++ )
        {
            checkpointAppender.checkPoint( NULL, logPosition, Instant.now(), reason );
        }

        assertThat( checkpointFile.getDetachedCheckpointFiles() ).hasSize( 7 ).allMatch( this::sizeEqualsToPreallocatedFile );

        checkpointAppender.checkPoint( NULL, logPosition, Instant.now(), reason );
        assertThat( checkpointFile.getDetachedCheckpointFiles() ).hasSize( 7 ).allMatch( this::sizeEqualsToPreallocatedFile );

        checkpointAppender.checkPoint( NULL, logPosition, Instant.now(), reason );
        assertThat( checkpointFile.getDetachedCheckpointFiles() ).hasSize( 7 ).allMatch( this::sizeEqualsToPreallocatedFile );

        checkpointAppender.checkPoint( NULL, logPosition, Instant.now(), reason );
        assertThat( checkpointFile.getDetachedCheckpointFiles() ).hasSize( 8 ).allMatch( this::sizeEqualsToPreallocatedFile );
    }

    private boolean sizeEqualsToPreallocatedFile( Path path )
    {
        try
        {
            return Files.size( path ) == ROTATION_THRESHOLD;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
