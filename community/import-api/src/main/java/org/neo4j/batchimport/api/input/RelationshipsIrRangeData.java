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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * Part of {@link ResumableStateData}.
 */
public record RelationshipsIrRangeData(
        Map<RelationshipsIRPhase, Boolean> hasRelationshipsInPhase,
        boolean initialPassDone,
        long nextIrPosition,
        boolean hasRelationships) {

    public RelationshipsIrRangeData {
        hasRelationshipsInPhase = Collections.unmodifiableMap(new EnumMap<>(hasRelationshipsInPhase));
    }

    void writeDataToOutputStream(DataOutputStream checkpointStream) throws IOException {
        checkpointStream.writeInt(hasRelationshipsInPhase.size());
        for (Map.Entry<RelationshipsIRPhase, Boolean> entry : hasRelationshipsInPhase.entrySet()) {
            checkpointStream.writeByte(entry.getKey().ordinal());
            checkpointStream.writeBoolean(entry.getValue());
        }
        checkpointStream.writeBoolean(initialPassDone);
        checkpointStream.writeLong(nextIrPosition);
        checkpointStream.writeBoolean(hasRelationships);
    }

    static RelationshipsIrRangeData fromInputStream(DataInputStream inputStream) throws IOException {
        int hasRelationshipsInPhaseSize = inputStream.readInt();
        EnumMap<RelationshipsIRPhase, Boolean> hasRelationshipsInPhase = new EnumMap<>(RelationshipsIRPhase.class);
        for (int i = 0; i < hasRelationshipsInPhaseSize; i++) {
            RelationshipsIRPhase phase = RelationshipsIRPhase.values()[inputStream.readByte()];
            hasRelationshipsInPhase.put(phase, inputStream.readBoolean());
        }
        boolean initialPassDone = inputStream.readBoolean();
        long nextIrPosition = inputStream.readLong();
        boolean hasRelationships = inputStream.readBoolean();
        return new RelationshipsIrRangeData(hasRelationshipsInPhase, initialPassDone, nextIrPosition, hasRelationships);
    }
}
