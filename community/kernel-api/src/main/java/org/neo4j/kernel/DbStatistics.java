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
package org.neo4j.kernel;

import java.util.StringJoiner;
import org.neo4j.common.Edition;

public record DbStatistics(
        String storeVersion,
        KernelVersion kernelVersion,
        long lastCommittedTransactionId,
        Edition edition, // Meta
        int nodes,
        int sparseNodes,
        int denseNodes,
        int highNodeId,
        int nodeProperties,
        int relationships,
        int highRelationshipId,
        int relationshipProperties,
        int indexes,
        int lookupIndexes,
        int btreeIndexes,
        int constraints,
        int btreeConstraints,
        int schemaEntities)
        implements KernelVersionProvider {
    public static final int NO_DATA = -1;

    public String statisticsAsConstructorString() {
        StringJoiner newStatisticsString = new StringJoiner(", ", "new DbStatistics( ", " )");
        newStatisticsString.add("\"" + storeVersion + "\"");
        newStatisticsString.add(kernelVersion != null ? "KernelVersion." + kernelVersion.name() : "null");
        newStatisticsString.add("" + lastCommittedTransactionId);
        newStatisticsString.add("" + edition.name());
        newStatisticsString.add("" + nodes);
        newStatisticsString.add("" + sparseNodes);
        newStatisticsString.add("" + denseNodes);
        newStatisticsString.add("" + highNodeId);
        newStatisticsString.add("" + nodeProperties);
        newStatisticsString.add("" + relationships);
        newStatisticsString.add("" + highRelationshipId);
        newStatisticsString.add("" + relationshipProperties);
        newStatisticsString.add("" + indexes);
        newStatisticsString.add("" + lookupIndexes);
        newStatisticsString.add("" + btreeIndexes);
        newStatisticsString.add("" + constraints);
        newStatisticsString.add("" + btreeConstraints);
        newStatisticsString.add("" + schemaEntities);
        return newStatisticsString.toString();
    }
}
