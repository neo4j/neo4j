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

import org.neo4j.common.EntityType;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class RelationshipIndexChecker extends IndexChecker<RelationshipRecord> {
    public RelationshipIndexChecker(CheckerContext context) {
        super(context, EntityType.RELATIONSHIP, "Relationship");
    }

    @Override
    public boolean isNodeBasedCheck() {
        return false;
    }

    @Override
    CommonAbstractStore<RelationshipRecord, ?> store() {
        return context.neoStores.getRelationshipStore();
    }

    @Override
    long highId() {
        return context.highRelationshipId;
    }

    @Override
    RelationshipRecord getEntity(StoreCursors storeCursors, long entityId) {
        return context.recordLoader.relationship(entityId, storeCursors, context.memoryTracker);
    }

    @Override
    int[] getEntityTokens(
            CheckerContext context,
            StoreCursors storeCursors,
            RelationshipRecord record,
            RecordReader<DynamicRecord> additionalReader) {
        return new int[] {record.getType()};
    }

    @Override
    RecordReader<DynamicRecord> additionalEntityTokenReader(CursorContext cursorContext) {
        return null;
    }

    @Override
    void reportEntityNotInUse(ConsistencyReport.IndexConsistencyReport report, RelationshipRecord record) {
        report.relationshipNotInUse(record);
    }

    @Override
    void reportIndexedIncorrectValues(
            ConsistencyReport.IndexConsistencyReport report, RelationshipRecord record, Object[] propertyValues) {
        report.relationshipIndexedWithWrongValues(record, propertyValues);
    }

    @Override
    void reportIndexedWhenShouldNot(ConsistencyReport.IndexConsistencyReport report, RelationshipRecord record) {
        report.relationshipIndexedWhenShouldNot(record);
    }

    @Override
    ConsistencyReport.PrimitiveConsistencyReport getReport(
            RelationshipRecord cursor, ConsistencyReport.Reporter reporter) {
        return reporter.forRelationship(cursor);
    }
}
