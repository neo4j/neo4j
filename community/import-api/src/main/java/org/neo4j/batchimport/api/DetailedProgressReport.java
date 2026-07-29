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
package org.neo4j.batchimport.api;

import java.time.Duration;
import java.util.Map;

/**
 * Detailed, (ideally) immutable progress report from an import. A new will be generated with a certain frequency.
 */
public interface DetailedProgressReport {
    long estimatedTotalNumberOfNodes();

    long estimatedTotalNumberOfRelationships();

    Stats nodeStats();

    Stats relationshipStats();

    Map<String, Stats> nodePerLabelStats();

    Map<String, Stats> relationshipPerTypeStats();

    Stats nodeIndexStats();

    Stats nodeConstraintStats();

    Map<String, Stats> nodeIndexPerLabelStats();

    Map<String, Stats> nodeConstraintPerLabelStats();

    Stats relationshipIndexStats();

    Stats relationshipConstraintStats();

    Map<String, Stats> relationshipIndexPerTypeStats();

    Map<String, Stats> relationshipConstraintPerTypeStats();

    record Stats(long processed, long created, long updated, long deleted) {}

    record Standard(
            long estimatedTotalNumberOfNodes,
            long estimatedTotalNumberOfRelationships,
            Stats nodeStats,
            Stats relationshipStats,
            Map<String, Stats> nodePerLabelStats,
            Map<String, Stats> relationshipPerTypeStats,
            Stats nodeIndexStats,
            Stats nodeConstraintStats,
            Map<String, Stats> nodeIndexPerLabelStats,
            Map<String, Stats> nodeConstraintPerLabelStats,
            Stats relationshipIndexStats,
            Stats relationshipConstraintStats,
            Map<String, Stats> relationshipIndexPerTypeStats,
            Map<String, Stats> relationshipConstraintPerTypeStats,
            Duration nodeImportDuration,
            Duration relationshipImportDuration,
            Duration schemaImportDuration)
            implements DetailedProgressReport {}

    record Skidbladnir(
            long estimatedTotalNumberOfNodes,
            long estimatedTotalNumberOfRelationships,
            Stats nodeStats,
            Stats relationshipStats,
            Map<String, Stats> nodePerLabelStats,
            Map<String, Stats> relationshipPerTypeStats,
            Stats nodeIndexStats,
            Stats nodeConstraintStats,
            Map<String, Stats> nodeIndexPerLabelStats,
            Map<String, Stats> nodeConstraintPerLabelStats,
            Stats relationshipIndexStats,
            Stats relationshipConstraintStats,
            Map<String, Stats> relationshipIndexPerTypeStats,
            Map<String, Stats> relationshipConstraintPerTypeStats,
            Duration nodeIdMappingDuration,
            Duration relationshipRangeDivisionDuration,
            Duration storeApplyingDuration,
            Duration schemaImportDuration)
            implements DetailedProgressReport {}
}
