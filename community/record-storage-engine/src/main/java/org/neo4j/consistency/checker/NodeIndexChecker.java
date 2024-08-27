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

import static org.neo4j.consistency.checker.RecordLoading.safeGetNodeLabels;

import org.neo4j.common.EntityType;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class NodeIndexChecker extends IndexChecker<NodeRecord> {
    NodeIndexChecker(CheckerContext context) {
        super(context, EntityType.NODE, "Node");
    }

    @Override
    CommonAbstractStore<NodeRecord, ?> store() {
        return context.neoStores.getNodeStore();
    }

    @Override
    long highId() {
        return context.highNodeId;
    }

    @Override
    NodeRecord getEntity(StoreCursors storeCursors, long entityId) {
        return context.recordLoader.node(entityId, storeCursors, context.memoryTracker);
    }

    @Override
    int[] getEntityTokens(
            CheckerContext context,
            StoreCursors storeCursors,
            NodeRecord record,
            RecordReader<DynamicRecord> additionalReader) {
        return safeGetNodeLabels(
                context, storeCursors, record.getId(), record.getLabelField(), additionalReader, context.memoryTracker);
    }

    @Override
    RecordReader<DynamicRecord> additionalEntityTokenReader(CursorContext cursorContext) {
        return new RecordReader<>(
                context.neoStores.getNodeStore().getDynamicLabelStore(), false, cursorContext, context.memoryTracker);
    }

    @Override
    void reportEntityNotInUse(ConsistencyReport.IndexConsistencyReport report, NodeRecord record) {
        report.nodeNotInUse(record);
    }

    @Override
    void reportIndexedIncorrectValues(
            ConsistencyReport.IndexConsistencyReport report, NodeRecord record, Object[] propertyValues) {
        report.nodeIndexedWithWrongValues(record, propertyValues);
    }

    @Override
    void reportIndexedWhenShouldNot(ConsistencyReport.IndexConsistencyReport report, NodeRecord record) {
        report.nodeIndexedWhenShouldNot(record);
    }

    @Override
    ConsistencyReport.PrimitiveConsistencyReport getReport(NodeRecord cursor, ConsistencyReport.Reporter reporter) {
        return reporter.forNode(cursor);
    }
}
