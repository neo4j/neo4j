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

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.neo4j.consistency.checker.RecordLoading.lightReplace;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.io.IOUtils.closeAllUnchecked;

import java.util.Arrays;
import java.util.function.Function;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.TypeRepresentation;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class SchemaComplianceChecker implements AutoCloseable {
    private final CheckerContext context;
    private final IntObjectMap<? extends IntSet> mandatoryProperties;
    private final IntObjectMap<? extends IntObjectMap<PropertyTypeSet>> allowedTypes;
    private final IndexAccessors.IndexReaders indexReaders;
    private final Iterable<IndexDescriptor> indexes;
    private final CursorContext cursorContext;
    private final StoreCursors storeCursors;
    private IntHashSet reportedMissingMandatoryPropertyKeys = new IntHashSet();
    private IntHashSet reportedTypeViolationPropertyKeys = new IntHashSet();

    SchemaComplianceChecker(
            CheckerContext context,
            IntObjectMap<? extends IntSet> mandatoryProperties,
            IntObjectMap<? extends IntObjectMap<PropertyTypeSet>> allowedTypes,
            Iterable<IndexDescriptor> indexes,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        this.context = context;
        this.mandatoryProperties = mandatoryProperties;
        this.allowedTypes = allowedTypes;
        this.indexReaders = context.indexAccessors.readers();
        this.indexes = indexes;
        this.cursorContext = cursorContext;
        this.storeCursors = storeCursors;
    }

    <ENTITY extends PrimitiveRecord> void checkExistenceAndTypeConstraints(
            ENTITY entity,
            int[] entityTokens,
            IntObjectMap<Value> values,
            Function<ENTITY, ConsistencyReport.PrimitiveConsistencyReport> reportSupplier) {
        if (entityTokens.length > 0) {
            checkExistenceAndTypeConstraints(entity, values, entityTokens, reportSupplier);
        }
    }

    <ENTITY extends PrimitiveRecord> void checkCorrectlyIndexed(
            ENTITY entity,
            int[] entityTokens,
            IntObjectMap<Value> values,
            Function<ENTITY, ConsistencyReport.PrimitiveConsistencyReport> reportSupplier) {
        for (IndexDescriptor indexRule : indexes) {
            Value[] valueArray = RecordLoading.entityIntersectionWithSchema(entityTokens, values, indexRule);
            if (valueArray == null) {
                continue;
            }
            var reader = indexReaders.reader(indexRule);
            if (indexRule.isUnique()) {
                verifyIndexedUniquely(entity, valueArray, indexRule, reader, reportSupplier);
            } else {
                long count = reader.countIndexedEntities(
                        entity.getId(), cursorContext, indexRule.schema().getPropertyIds(), valueArray);
                reportIncorrectIndexCount(entity, valueArray, indexRule, count, reportSupplier);
            }
        }
    }

    @Override
    public void close() {
        closeAllUnchecked(indexReaders);
    }

    private <ENTITY extends PrimitiveRecord> void verifyIndexedUniquely(
            ENTITY entity,
            Value[] propertyValues,
            IndexDescriptor indexRule,
            ValueIndexReader reader,
            Function<ENTITY, ConsistencyReport.PrimitiveConsistencyReport> reportSupplier) {
        long nodeId = entity.getId();
        PropertyIndexQuery[] query = seek(indexRule.schema(), propertyValues);
        LongIterator indexedNodeIds = queryIndexOrEmpty(reader, query);
        long count = 0;
        while (indexedNodeIds.hasNext()) {
            long indexedNodeId = indexedNodeIds.next();
            if (nodeId == indexedNodeId) {
                count++;
            } else {
                reportSupplier
                        .apply(entity)
                        .uniqueIndexNotUnique(indexRule, Values.asObjects(propertyValues), indexedNodeId);
            }
        }

        reportIncorrectIndexCount(entity, propertyValues, indexRule, count, reportSupplier);
    }

    private static PropertyIndexQuery[] seek(SchemaDescriptor schema, Value[] propertyValues) {
        int[] propertyIds = schema.getPropertyIds();
        assert propertyIds.length == propertyValues.length;
        PropertyIndexQuery[] query = new PropertyIndexQuery[propertyValues.length];
        for (int i = 0; i < query.length; i++) {
            query[i] = PropertyIndexQuery.exact(propertyIds[i], propertyValues[i]);
        }
        return query;
    }

    private static LongIterator queryIndexOrEmpty(ValueIndexReader reader, PropertyIndexQuery[] query) {
        try {
            NodeValueIterator indexedNodeIds = new NodeValueIterator();
            reader.query(indexedNodeIds, NULL_CONTEXT, unconstrained(), query);
            return indexedNodeIds;
        } catch (IndexNotApplicableKernelException e) {
            throw new RuntimeException(
                    format(
                            "Consistency checking error: index provider does not support exact query %s",
                            Arrays.toString(query)),
                    e);
        }
    }

    private <ENTITY extends PrimitiveRecord> void reportIncorrectIndexCount(
            ENTITY entity,
            Value[] propertyValues,
            IndexDescriptor indexRule,
            long count,
            Function<ENTITY, ConsistencyReport.PrimitiveConsistencyReport> reportSupplier) {
        if (count == 0 && areValuesSupportedByIndex(indexRule, propertyValues)) {
            reportSupplier
                    .apply(context.recordLoader.entity(entity, storeCursors, context.memoryTracker))
                    .notIndexed(indexRule, Values.asObjects(propertyValues));
        } else if (count != 1) {
            reportSupplier
                    .apply(context.recordLoader.entity(entity, storeCursors, context.memoryTracker))
                    .indexedMultipleTimes(indexRule, Values.asObjects(propertyValues), count);
        }
    }

    private <ENTITY extends PrimitiveRecord> void checkExistenceAndTypeConstraints(
            ENTITY entity,
            IntObjectMap<Value> seenProperties,
            int[] entityTokenIds,
            Function<ENTITY, ConsistencyReport.PrimitiveConsistencyReport> reporter) {
        if (mandatoryProperties.isEmpty() && allowedTypes.isEmpty()) {
            return;
        }

        reportedMissingMandatoryPropertyKeys = lightReplace(reportedMissingMandatoryPropertyKeys);
        reportedTypeViolationPropertyKeys = lightReplace(reportedTypeViolationPropertyKeys);
        for (long entityToken : entityTokenIds) {
            int token = toIntExact(entityToken);

            IntSet mandatoryPropertyKeysForEntityToken = mandatoryProperties.get(token);
            if (mandatoryPropertyKeysForEntityToken != null) {
                checkPropertyExistence(entity, seenProperties, reporter, mandatoryPropertyKeysForEntityToken);
            }

            IntObjectMap<PropertyTypeSet> allowedTypesByPropertyKey = allowedTypes.get(token);
            if (allowedTypesByPropertyKey != null) {
                checkPropertyTypes(entity, seenProperties, reporter, allowedTypesByPropertyKey);
            }
        }
    }

    private <ENTITY extends PrimitiveRecord> void checkPropertyExistence(
            ENTITY entity,
            IntObjectMap<Value> seenProperties,
            Function<ENTITY, ConsistencyReport.PrimitiveConsistencyReport> reporter,
            IntSet mandatoryPropertyKeysForEntityToken) {
        IntIterator iterator = mandatoryPropertyKeysForEntityToken.intIterator();
        while (iterator.hasNext()) {
            int mandatoryPropertyKeyForEntityToken = iterator.next();
            if (!seenProperties.containsKey(mandatoryPropertyKeyForEntityToken)
                    && reportedMissingMandatoryPropertyKeys.add(mandatoryPropertyKeyForEntityToken)) {
                reporter.apply(entity).missingMandatoryProperty(mandatoryPropertyKeyForEntityToken);
            }
        }
    }

    private <ENTITY extends PrimitiveRecord> void checkPropertyTypes(
            ENTITY entity,
            IntObjectMap<Value> seenProperties,
            Function<ENTITY, ConsistencyReport.PrimitiveConsistencyReport> reporter,
            IntObjectMap<PropertyTypeSet> allowedTypesByPropertyKey) {
        for (var property : allowedTypesByPropertyKey.keyValuesView()) {
            var propertyKey = property.getOne();
            var allowedTypes = property.getTwo();
            var propertyValue = seenProperties.get(propertyKey);
            if (propertyValue != null
                    && TypeRepresentation.disallows(allowedTypes, propertyValue)
                    && reportedTypeViolationPropertyKeys.add(propertyKey)) {
                reporter.apply(entity).typeConstraintViolation(propertyKey);
            }
        }
    }

    static boolean areValuesSupportedByIndex(IndexDescriptor index, Value... values) {
        return values != null && index.getCapability().areValuesAccepted(values);
    }
}
