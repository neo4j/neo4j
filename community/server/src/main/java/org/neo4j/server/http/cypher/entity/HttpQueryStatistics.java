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
package org.neo4j.server.http.cypher.entity;

import java.util.function.Function;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.virtual.MapValue;

public class HttpQueryStatistics implements QueryStatistics {
    private final int createdNodes;
    private final int deletedNodes;
    private final int createdRelationships;
    private final int deletedRelationships;
    private final int setProperties;
    private final int addedLabels;
    private final int removedLabels;
    private final int addedIndexes;
    private final int removedIndexes;
    private final int addedConstraints;
    private final int removedConstraints;
    private final int systemUpdates;
    private final boolean containsUpdates;
    private final boolean containsSystemUpdates;

    private HttpQueryStatistics(
            int createdNodes,
            int deletedNodes,
            int createdRelationships,
            int deletedRelationships,
            int setProperties,
            int addedLabels,
            int removedLabels,
            int addedIndexes,
            int removedIndexes,
            int addedConstraints,
            int removedConstraints,
            int systemUpdates,
            boolean containsUpdates,
            boolean containsSystemUpdates) {
        this.createdNodes = createdNodes;
        this.deletedNodes = deletedNodes;
        this.createdRelationships = createdRelationships;
        this.deletedRelationships = deletedRelationships;
        this.setProperties = setProperties;
        this.addedLabels = addedLabels;
        this.removedLabels = removedLabels;
        this.addedIndexes = addedIndexes;
        this.removedIndexes = removedIndexes;
        this.addedConstraints = addedConstraints;
        this.removedConstraints = removedConstraints;
        this.systemUpdates = systemUpdates;
        this.containsUpdates = containsUpdates;
        this.containsSystemUpdates = containsSystemUpdates;
    }

    public static QueryStatistics fromAnyValue(AnyValue anyValue) {
        if (anyValue == null) {
            return QueryStatistics.EMPTY;
        } else {
            MapValue queryStatsMap = (MapValue) anyValue;

            return new HttpQueryStatistics(
                    extractIntIfPresent(queryStatsMap, "nodes-created"),
                    extractIntIfPresent(queryStatsMap, "nodes-deleted"),
                    extractIntIfPresent(queryStatsMap, "relationships-created"),
                    extractIntIfPresent(queryStatsMap, "relationships-deleted"),
                    extractIntIfPresent(queryStatsMap, "properties-set"),
                    extractIntIfPresent(queryStatsMap, "labels-added"),
                    extractIntIfPresent(queryStatsMap, "labels-removed"),
                    extractIntIfPresent(queryStatsMap, "indexes-added"),
                    extractIntIfPresent(queryStatsMap, "indexes-removed"),
                    extractIntIfPresent(queryStatsMap, "constraints-added"),
                    extractIntIfPresent(queryStatsMap, "constraints-removed"),
                    extractIntIfPresent(queryStatsMap, "system-updates"),
                    extractBooleanIfPresent(queryStatsMap, "contains-updates"),
                    extractBooleanIfPresent(queryStatsMap, "contains-system-updates"));
        }
    }

    @Override
    public int getNodesCreated() {
        return createdNodes;
    }

    @Override
    public int getNodesDeleted() {
        return deletedNodes;
    }

    @Override
    public int getRelationshipsCreated() {
        return createdRelationships;
    }

    @Override
    public int getRelationshipsDeleted() {
        return deletedRelationships;
    }

    @Override
    public int getPropertiesSet() {
        return setProperties;
    }

    @Override
    public int getLabelsAdded() {
        return addedLabels;
    }

    @Override
    public int getLabelsRemoved() {
        return removedLabels;
    }

    @Override
    public int getIndexesAdded() {
        return addedIndexes;
    }

    @Override
    public int getIndexesRemoved() {
        return removedIndexes;
    }

    @Override
    public int getConstraintsAdded() {
        return addedConstraints;
    }

    @Override
    public int getConstraintsRemoved() {
        return removedConstraints;
    }

    @Override
    public int getSystemUpdates() {
        return systemUpdates;
    }

    @Override
    public boolean containsUpdates() {
        return containsUpdates;
    }

    @Override
    public boolean containsSystemUpdates() {
        return containsSystemUpdates;
    }

    private static <T> T extractIfPresent(
            MapValue queryStatsMap, String queryStatLabel, T defaultValue, Function<AnyValue, T> mapperFunction) {
        if (queryStatsMap.containsKey(queryStatLabel)) {
            return mapperFunction.apply(queryStatsMap.get(queryStatLabel));
        } else {
            return defaultValue;
        }
    }

    private static int extractIntIfPresent(MapValue queryStatsMap, String queryStatLabel) {
        return extractIfPresent(queryStatsMap, queryStatLabel, 0, v -> ((IntValue) v).value());
    }

    private static boolean extractBooleanIfPresent(MapValue queryStatsMap, String queryStatLabel) {
        return extractIfPresent(queryStatsMap, queryStatLabel, false, v -> ((BooleanValue) v).booleanValue());
    }
}
