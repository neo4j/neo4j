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
package org.neo4j.consistency.checker;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.values.storable.Values;

public class NodeIndexCheckerTest extends CheckerTestBase {
    int label;
    int prop;

    @Override
    void initialData(KernelTransaction tx) throws KernelException {
        label = tx.tokenWrite().labelGetOrCreateForName("myLabel");
        prop = tx.tokenWrite().propertyKeyGetOrCreateForName("myProp");

        // Generate enough data to get several partitions from newAllEntriesValueReader later
        for (int i = 0; i < 200; i++) {
            long node = tx.dataWrite().nodeCreateWithLabels(new int[] {label});
            tx.dataWrite().nodeSetProperty(node, prop, Values.stringValue("propValue" + "1".repeat(i)));
        }
    }

    @Test
    void consistentUniqueIndexShouldNotGenerateInconsistencies() throws Exception {
        uniqueIndex(SchemaDescriptors.forLabel(label, prop));

        // A setup that will give two empty partitions next to each other.
        // Since we do the checking in ranges some partitions can get empty if the
        // entries doesn't belong to nodes in the range.
        // The ordering of the created property values will give several empty partitions.
        ConsistencySummaryStatistics inconsistenciesSummary = new ConsistencySummaryStatistics();
        CheckerContext context = context(4, ConsistencyFlags.ALL, inconsistenciesSummary);
        NodeIndexChecker indexChecker = new NodeIndexChecker(context);
        indexChecker.check(LongRange.range(0L, 10L), true, false, EmptyMemoryTracker.INSTANCE);

        // We should not have found any inconsistencies in the consistent index.
        // There was a bug in consistency checker where two empty partitions would generate a not unique inconsistency
        // in this case.
        assertTrue(inconsistenciesSummary.isConsistent());
    }
}
