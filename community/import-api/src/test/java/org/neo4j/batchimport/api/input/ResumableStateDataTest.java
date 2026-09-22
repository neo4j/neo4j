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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.RepeatedTest;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;

@RandomSupportExtension
class ResumableStateDataTest {

    @Inject
    RandomSupport random;

    @RepeatedTest(100)
    void shouldRoundTripRandomData() throws IOException {
        ResumableStateData data = randomResumableStateData();

        ResumableStateData readBack =
                ResumableStateData.fromInputStream(new DataInputStream(new ByteArrayInputStream(data.asByteArray())));

        assertThat(readBack).isEqualTo(data);
    }

    private ResumableStateData randomResumableStateData() {
        int relationshipIrRangesSize = random.nextInt(10);
        List<RelationshipsIrRangeData> relationshipIrRanges = new ArrayList<>(relationshipIrRangesSize);
        for (int i = 0; i < relationshipIrRangesSize; i++) {
            relationshipIrRanges.add(randomRelationshipsIrRangeData());
        }

        return new ResumableStateData(
                randByte(), random.nextLong(), random.nextLong(), relationshipIrRanges, randByte());
    }

    private RelationshipsIrRangeData randomRelationshipsIrRangeData() {
        Map<RelationshipsIRPhase, Boolean> hasRelationshipsInPhase = new EnumMap<>(RelationshipsIRPhase.class);
        for (RelationshipsIRPhase phase : RelationshipsIRPhase.values()) {
            if (random.nextBoolean()) {
                hasRelationshipsInPhase.put(phase, random.nextBoolean());
            }
        }
        return new RelationshipsIrRangeData(
                hasRelationshipsInPhase, random.nextBoolean(), random.nextLong(), random.nextBoolean());
    }

    private byte randByte() {
        return (byte) random.nextInt(Byte.MAX_VALUE);
    }
}
