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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public class CheckpointKernelVersionIT {
    @Inject
    private CheckPointer checkPointer;

    @Inject
    private MetadataProvider metadataProvider;

    @Inject
    private LogFiles logFiles;

    @Test
    void checkPointRecordContainsDatabaseKernelVersion() throws IOException {
        // we can't test any earlier version since those version do not support new format of checkpoint commands so its
        // impossible to read them back
        ((MetaDataStore) metadataProvider).setKernelVersion(KernelVersion.V5_0);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("Forced 5.0"));

        List<CheckpointInfo> checkpointInfos = logFiles.getCheckpointFile().reachableCheckpoints();
        assertThat(checkpointInfos).hasSize(1);
        assertThat(checkpointInfos.get(0).getVersion()).isEqualTo(KernelVersion.V5_0);
    }
}
