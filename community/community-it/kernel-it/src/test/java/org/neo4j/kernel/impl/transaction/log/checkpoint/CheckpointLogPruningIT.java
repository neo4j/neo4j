/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_keep_threshold;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_rotation_threshold;
import static org.neo4j.io.ByteUnit.kibiBytes;

import java.io.IOException;
import java.nio.file.Path;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
public class CheckpointLogPruningIT {
    @Inject
    private GraphDatabaseService database;

    @Inject
    private LogFiles logFiles;

    @Inject
    private CheckPointer checkPointer;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(checkpoint_logical_log_rotation_threshold, kibiBytes(1))
                .setConfig(checkpoint_logical_log_keep_threshold, 2);
    }

    @Test
    void pruneObsoleteCheckpointLogFiles() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();

        var reason = "checkpoint for rotation test";
        for (int i = 0; i < 105; i++) {
            checkPointer.forceCheckPoint(new SimpleTriggerInfo(reason));
        }
        var matchedFiles = checkpointFile.getDetachedCheckpointFiles();
        assertThat(matchedFiles).hasSize(2);
        assertThat(matchedFiles)
                .areAtLeastOne(fileNameCondition("checkpoint.25"))
                .areAtLeastOne(fileNameCondition("checkpoint.26"));
    }

    @Test
    void doNotPruneFilesUntilConfigured() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();

        var reason = "checkpoint for rotation test";
        for (int i = 0; i < 6; i++) {
            checkPointer.forceCheckPoint(new SimpleTriggerInfo(reason));
        }
        var matchedFiles = checkpointFile.getDetachedCheckpointFiles();
        assertThat(matchedFiles).hasSize(2);
        assertThat(matchedFiles)
                .areAtLeastOne(fileNameCondition("checkpoint.0"))
                .areAtLeastOne(fileNameCondition("checkpoint.1"));
    }

    @Test
    void pruneAsSoonAsHaveAnyEligibleFiles() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();

        var reason = "checkpoint for rotation test";
        for (int i = 0; i < 10; i++) {
            checkPointer.forceCheckPoint(new SimpleTriggerInfo(reason));
        }
        var matchedFiles = checkpointFile.getDetachedCheckpointFiles();
        assertThat(matchedFiles).hasSize(2);
        assertThat(matchedFiles)
                .areAtLeastOne(fileNameCondition("checkpoint.1"))
                .areAtLeastOne(fileNameCondition("checkpoint.2"));
    }

    private static Condition<Path> fileNameCondition(String name) {
        return new Condition<>() {
            @Override
            public boolean matches(Path file) {
                return name.equals(file.getFileName().toString());
            }
        };
    }
}
