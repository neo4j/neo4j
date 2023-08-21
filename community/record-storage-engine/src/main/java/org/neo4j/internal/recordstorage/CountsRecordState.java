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
package org.neo4j.internal.recordstorage;

import static java.lang.StrictMath.toIntExact;

import java.util.Collection;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CountsDelta;
import org.neo4j.storageengine.api.StorageCommand;

/**
 * A {@link CountsDelta} with an additional capability of turning counts into {@link StorageCommand commands} for storage.
 */
public class CountsRecordState extends CountsDelta implements RecordState {
    private final LogCommandSerialization serialization;

    CountsRecordState(LogCommandSerialization serialization) {
        this.serialization = serialization;
    }

    @Override
    public void extractCommands(Collection<StorageCommand> target, MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(nodeCounts.size() * Command.NodeCountsCommand.SHALLOW_SIZE
                + relationshipCounts.size() * Command.RelationshipCountsCommand.SHALLOW_SIZE);

        nodeCounts.forEachKeyValue((labelId, count) -> {
            if (count != 0) {
                target.add(new Command.NodeCountsCommand(serialization, toIntExact(labelId), count));
            }
        });
        relationshipCounts.forEachKeyValue((k, mutableLong) -> {
            long count = mutableLong.longValue();
            if (count != 0) {
                target.add(new Command.RelationshipCountsCommand(
                        serialization, k.startLabelId(), k.typeId(), k.endLabelId(), count));
            }
        });
    }
}
