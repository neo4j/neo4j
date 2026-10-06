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
import java.util.HashMap;
import java.util.Map;
import org.neo4j.batchimport.api.DetailedProgressReport;

/**
 * The state of a {@link org.neo4j.batchimport.api.DetailedProgressReportBase} at the time of a checkpoint.
 * Per-token stats are keyed by token id, not name; thus not using {@link DetailedProgressReport.Resumable}.
 * <p>
 * Part of {@link ResumableStateData}.
 */
public record ProgressReportData(
        DetailedProgressReport.Stats nodeStats,
        DetailedProgressReport.Stats relationshipStats,
        Map<Integer, DetailedProgressReport.Stats> nodePerLabelStats,
        Map<Integer, DetailedProgressReport.Stats> relationshipPerTypeStats,
        DetailedProgressReport.Stats nodeIndexStats,
        DetailedProgressReport.Stats nodeConstraintStats,
        Map<Integer, DetailedProgressReport.Stats> nodeIndexPerLabelStats,
        Map<Integer, DetailedProgressReport.Stats> nodeConstraintPerLabelStats,
        DetailedProgressReport.Stats relationshipIndexStats,
        DetailedProgressReport.Stats relationshipConstraintStats,
        Map<Integer, DetailedProgressReport.Stats> relationshipIndexPerTypeStats,
        Map<Integer, DetailedProgressReport.Stats> relationshipConstraintPerTypeStats,
        long nodeIdMappingDurationNanos,
        long relationshipRangeDivisionDurationNanos,
        long storeApplyingDurationNanos,
        long schemaImportDurationNanos) {

    public ProgressReportData {
        nodePerLabelStats = Map.copyOf(nodePerLabelStats);
        relationshipPerTypeStats = Map.copyOf(relationshipPerTypeStats);
        nodeIndexPerLabelStats = Map.copyOf(nodeIndexPerLabelStats);
        nodeConstraintPerLabelStats = Map.copyOf(nodeConstraintPerLabelStats);
        relationshipIndexPerTypeStats = Map.copyOf(relationshipIndexPerTypeStats);
        relationshipConstraintPerTypeStats = Map.copyOf(relationshipConstraintPerTypeStats);
    }

    void writeDataToOutputStream(DataOutputStream checkpointStream) throws IOException {
        writeStats(checkpointStream, nodeStats);
        writeStats(checkpointStream, relationshipStats);
        writePerTokenStats(checkpointStream, nodePerLabelStats);
        writePerTokenStats(checkpointStream, relationshipPerTypeStats);
        writeStats(checkpointStream, nodeIndexStats);
        writeStats(checkpointStream, nodeConstraintStats);
        writePerTokenStats(checkpointStream, nodeIndexPerLabelStats);
        writePerTokenStats(checkpointStream, nodeConstraintPerLabelStats);
        writeStats(checkpointStream, relationshipIndexStats);
        writeStats(checkpointStream, relationshipConstraintStats);
        writePerTokenStats(checkpointStream, relationshipIndexPerTypeStats);
        writePerTokenStats(checkpointStream, relationshipConstraintPerTypeStats);
        checkpointStream.writeLong(nodeIdMappingDurationNanos);
        checkpointStream.writeLong(relationshipRangeDivisionDurationNanos);
        checkpointStream.writeLong(storeApplyingDurationNanos);
        checkpointStream.writeLong(schemaImportDurationNanos);
    }

    static ProgressReportData fromInputStream(DataInputStream inputStream) throws IOException {
        DetailedProgressReport.Stats nodeStats = readStats(inputStream);
        DetailedProgressReport.Stats relationshipStats = readStats(inputStream);
        Map<Integer, DetailedProgressReport.Stats> nodePerLabelStats = readPerTokenStats(inputStream);
        Map<Integer, DetailedProgressReport.Stats> relationshipPerTypeStats = readPerTokenStats(inputStream);
        DetailedProgressReport.Stats nodeIndexStats = readStats(inputStream);
        DetailedProgressReport.Stats nodeConstraintStats = readStats(inputStream);
        Map<Integer, DetailedProgressReport.Stats> nodeIndexPerLabelStats = readPerTokenStats(inputStream);
        Map<Integer, DetailedProgressReport.Stats> nodeConstraintPerLabelStats = readPerTokenStats(inputStream);
        DetailedProgressReport.Stats relationshipIndexStats = readStats(inputStream);
        DetailedProgressReport.Stats relationshipConstraintStats = readStats(inputStream);
        Map<Integer, DetailedProgressReport.Stats> relationshipIndexPerTypeStats = readPerTokenStats(inputStream);
        Map<Integer, DetailedProgressReport.Stats> relationshipConstraintPerTypeStats = readPerTokenStats(inputStream);
        return new ProgressReportData(
                nodeStats,
                relationshipStats,
                nodePerLabelStats,
                relationshipPerTypeStats,
                nodeIndexStats,
                nodeConstraintStats,
                nodeIndexPerLabelStats,
                nodeConstraintPerLabelStats,
                relationshipIndexStats,
                relationshipConstraintStats,
                relationshipIndexPerTypeStats,
                relationshipConstraintPerTypeStats,
                inputStream.readLong(),
                inputStream.readLong(),
                inputStream.readLong(),
                inputStream.readLong());
    }

    private static void writeStats(DataOutputStream checkpointStream, DetailedProgressReport.Stats stats)
            throws IOException {
        checkpointStream.writeLong(stats.processed());
        checkpointStream.writeLong(stats.created());
        assert stats.updated() == 0 : "Stats should not have updated count, but was " + stats.updated();
        assert stats.deleted() == 0 : "Stats should not have deleted count, but was " + stats.deleted();
    }

    private static DetailedProgressReport.Stats readStats(DataInputStream inputStream) throws IOException {
        return new DetailedProgressReport.Stats(inputStream.readLong(), inputStream.readLong(), 0, 0);
    }

    private static void writePerTokenStats(
            DataOutputStream checkpointStream, Map<Integer, DetailedProgressReport.Stats> stats) throws IOException {
        checkpointStream.writeInt(stats.size());
        for (Map.Entry<Integer, DetailedProgressReport.Stats> entry : stats.entrySet()) {
            checkpointStream.writeInt(entry.getKey());
            writeStats(checkpointStream, entry.getValue());
        }
    }

    private static Map<Integer, DetailedProgressReport.Stats> readPerTokenStats(DataInputStream inputStream)
            throws IOException {
        int size = inputStream.readInt();
        Map<Integer, DetailedProgressReport.Stats> stats = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            stats.put(inputStream.readInt(), readStats(inputStream));
        }
        return stats;
    }
}
