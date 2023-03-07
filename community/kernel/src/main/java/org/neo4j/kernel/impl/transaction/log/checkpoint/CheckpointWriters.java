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

import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.entry.CheckpointLogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.v50.DetachedCheckpointLogEntryWriterV5_0;
import org.neo4j.kernel.impl.transaction.log.entry.v56.DetachedCheckpointLogEntryWriterV5_6;

public final class CheckpointWriters {
    private final DetachedCheckpointLogEntryWriterV5_0 writer5_0;
    private final DetachedCheckpointLogEntryWriterV5_6 writer5_6;

    public CheckpointWriters(WritableChannel channel) {
        writer5_0 = new DetachedCheckpointLogEntryWriterV5_0(channel);
        writer5_6 = new DetachedCheckpointLogEntryWriterV5_6(channel);
    }

    public CheckpointLogEntryWriter writer(KernelVersion version) {
        return switch (version) {
            case V4_2, V4_3_D4, V4_4 -> throw new IllegalArgumentException(
                    "Unsupported kernel version for new checkpoint: " + version);
            case V5_0 -> writer5_0;
            default -> writer5_6;
        };
    }
}
