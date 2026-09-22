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
package org.neo4j.batchimport.api.input;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public record ResumableStateData(
        byte stepOrdinal,
        long badCollectedEntriesCount,
        long problemHandlerPosition,
        List<RelationshipsIrRangeData> relationshipsIrRanges,
        byte relationshipIdCountersSnapshot) {

    public ResumableStateData {
        relationshipsIrRanges = List.copyOf(relationshipsIrRanges);
    }

    public byte[] asByteArray() throws IOException {
        ByteArrayOutputStream checkpoint = new ByteArrayOutputStream();
        DataOutputStream checkpointStream = new DataOutputStream(checkpoint);
        writeDataToOutputStream(checkpointStream);
        return checkpoint.toByteArray();
    }

    private void writeDataToOutputStream(DataOutputStream checkpointStream) throws IOException {
        checkpointStream.writeByte(stepOrdinal);
        checkpointStream.writeLong(badCollectedEntriesCount);
        checkpointStream.writeLong(problemHandlerPosition);
        checkpointStream.writeInt(relationshipsIrRanges.size());
        for (RelationshipsIrRangeData relationshipsIrRange : relationshipsIrRanges) {
            relationshipsIrRange.writeDataToOutputStream(checkpointStream);
        }
        checkpointStream.writeByte(relationshipIdCountersSnapshot);
    }

    public static ResumableStateData fromInputStream(DataInputStream inputStream) throws IOException {
        byte stepOrdinal = inputStream.readByte();
        long collectorBadEntries = inputStream.readLong();
        long problemHandlerPosition = inputStream.readLong();
        int numRanges = inputStream.readInt();
        List<RelationshipsIrRangeData> relationshipsIrRanges = new ArrayList<>(numRanges);
        for (int i = 0; i < numRanges; i++) {
            relationshipsIrRanges.add(RelationshipsIrRangeData.fromInputStream(inputStream));
        }
        byte relationshipIdCountersSnapshot = inputStream.readByte();
        return new ResumableStateData(
                stepOrdinal,
                collectorBadEntries,
                problemHandlerPosition,
                relationshipsIrRanges,
                relationshipIdCountersSnapshot);
    }

    @SuppressWarnings("UnusedReturnValue")
    public static class ResumableStateDataBuilder {
        private final List<RelationshipsIrRangeData> relationshipsIrRanges = new ArrayList<>();
        private byte stepOrdinal;
        private long badCollectedEntriesCount;
        private long problemHandlerPosition;
        private byte relationshipIdCountersSnapshot;

        public ResumableStateDataBuilder setStepOrdinal(byte stepOrdinal) {
            this.stepOrdinal = stepOrdinal;
            return this;
        }

        public ResumableStateDataBuilder setBadCollectedEntriesCount(long badCollectedEntriesCount) {
            this.badCollectedEntriesCount = badCollectedEntriesCount;
            return this;
        }

        public ResumableStateDataBuilder setProblemHandlerPosition(long problemHandlerPosition) {
            this.problemHandlerPosition = problemHandlerPosition;
            return this;
        }

        public ResumableStateDataBuilder addRelationshipsIrRange(RelationshipsIrRangeData relationshipsIrRange) {
            this.relationshipsIrRanges.add(relationshipsIrRange);
            return this;
        }

        public ResumableStateDataBuilder setRelationshipIdCountersSnapshot(byte relationshipIdCountersSnapshot) {
            this.relationshipIdCountersSnapshot = relationshipIdCountersSnapshot;
            return this;
        }

        public ResumableStateData build() {
            return new ResumableStateData(
                    stepOrdinal,
                    badCollectedEntriesCount,
                    problemHandlerPosition,
                    relationshipsIrRanges,
                    relationshipIdCountersSnapshot);
        }
    }
}
