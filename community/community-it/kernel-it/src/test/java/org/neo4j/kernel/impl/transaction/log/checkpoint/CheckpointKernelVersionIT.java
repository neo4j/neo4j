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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionRepository;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public class CheckpointKernelVersionIT {
    @Inject
    private CheckPointer checkPointer;

    @Inject
    private KernelVersionRepository kernelVersionRepository;

    @Inject
    private LogFiles logFiles;

    @Test
    void checkPointRecordContainsDatabaseKernelVersion() throws IOException {
        // earlier version do not support new format of checkpoint commands; it's impossible to read them back, so we
        // cannot test them
        kernelVersionRepository.setKernelVersion(KernelVersion.V5_7);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("Forced " + kernelVersionRepository.kernelVersion()));

        final var checkpoint =
                logFiles.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertThat(checkpoint.kernelVersion())
                .as("kernel version from last checkpoint")
                .isEqualTo(kernelVersionRepository.kernelVersion());
    }

    @Test
    void checkPointRecordContainsAppendIndex() throws IOException {
        kernelVersionRepository.setKernelVersion(KernelVersion.V5_20);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("Forced " + kernelVersionRepository.kernelVersion()));

        final var checkpoint =
                logFiles.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertThat(checkpoint.appendIndex())
                .as("Append index should be positive number")
                .isGreaterThan(AppendIndexProvider.BASE_APPEND_INDEX);
    }

    @Test
    void checkPointInLegacy5_0Format() throws IOException {
        kernelVersionRepository.setKernelVersion(KernelVersion.V5_0);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("Legacy format."));

        final var checkpoint =
                logFiles.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertEquals(KernelVersion.V5_0, checkpoint.kernelVersion());
        assertEquals(UNKNOWN_CONSENSUS_INDEX, checkpoint.transactionId().consensusIndex());
    }
}
