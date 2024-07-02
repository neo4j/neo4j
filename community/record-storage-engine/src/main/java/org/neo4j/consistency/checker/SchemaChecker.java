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

import static org.neo4j.consistency.checker.RecordLoading.checkValidToken;
import static org.neo4j.consistency.checker.RecordLoading.lightReplace;
import static org.neo4j.consistency.checker.RecordLoading.safeLoadDynamicRecordChain;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.SchemaRuleKey;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.recordstorage.SchemaStorage;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;

/**
 * Checks schema records, i.e. indexes and constraints so that they refer to valid tokens and that schema records
 * that refer to other schema records are consistent.
 */
class SchemaChecker {
    private static final String CONSISTENCY_TOKEN_CHECKER_TAG = "consistencyTokenChecker";
    private static final String CONSTRAINT_INDEX_RULE = "CONSTRAINT_INDEX_RULE";
    private static final String UNIQUENESS_CONSTRAINT = "UNIQUENESS_CONSTRAINT";
    private final NeoStores neoStores;
    private final TokenHolders tokenHolders;
    private final IndexAccessors indexAccessors;
    private final CheckerContext context;
    private final SchemaStore schemaStore;
    private final ConsistencyReport.Reporter reporter;
    private final ParallelExecution execution;

    SchemaChecker(CheckerContext context) {
        this.neoStores = context.neoStores;
        this.tokenHolders = context.tokenHolders;
        this.indexAccessors = context.indexAccessors;
        this.context = context;
        this.schemaStore = neoStores.getSchemaStore();
        this.reporter = context.reporter;
        this.execution = context.execution;
    }

    void check(
            MutableIntObjectMap<MutableIntSet> mandatoryNodeProperties,
            MutableIntObjectMap<MutableIntSet> mandatoryRelationshipProperties,
            MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedNodePropertyTypes,
            MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedRelationshipPropertyTypes,
            CursorContext cursorContext,
            StoreCursors storeCursors)
            throws Exception {
        checkSchema(
                mandatoryNodeProperties,
                mandatoryRelationshipProperties,
                allowedNodePropertyTypes,
                allowedRelationshipPropertyTypes,
                cursorContext,
                storeCursors);
        checkTokens();
    }

    private void checkSchema(
            MutableIntObjectMap<MutableIntSet> mandatoryNodeProperties,
            MutableIntObjectMap<MutableIntSet> mandatoryRelationshipProperties,
            MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedNodePropertyTypes,
            MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedRelationshipPropertyTypes,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        long highId = schemaStore.getIdGenerator().getHighId();
        try (RecordReader<SchemaRecord> schemaReader = new RecordReader<>(schemaStore, true, cursorContext)) {
            MutableLongObjectMap<SchemaRecord> indexObligations = LongObjectMaps.mutable.empty();
            MutableLongObjectMap<ConstraintObligation> constraintObligations = LongObjectMaps.mutable.empty();
            Map<SchemaRuleKey, SchemaRecord> verifiedRulesWithRecords = new HashMap<>();

            SchemaStorage schemaStorage = new SchemaStorage(schemaStore, tokenHolders);
            // Build map of obligations and such
            buildObligationsMap(
                    highId,
                    schemaReader,
                    schemaStorage,
                    indexObligations,
                    constraintObligations,
                    verifiedRulesWithRecords,
                    storeCursors);

            // Verify all things, now that we have the complete map of obligations back and forth
            performSchemaCheck(
                    highId,
                    schemaReader,
                    indexObligations,
                    constraintObligations,
                    schemaStorage,
                    mandatoryNodeProperties,
                    mandatoryRelationshipProperties,
                    allowedNodePropertyTypes,
                    allowedRelationshipPropertyTypes,
                    storeCursors,
                    cursorContext);
        }
    }

    private void buildObligationsMap(
            long highId,
            RecordReader<SchemaRecord> reader,
            SchemaStorage schemaStorage,
            MutableLongObjectMap<SchemaRecord> indexObligations,
            MutableLongObjectMap<ConstraintObligation> constraintObligations,
            Map<SchemaRuleKey, SchemaRecord> verifiedRulesWithRecords,
            StoreCursors storeCursors) {
        for (long id = schemaStore.getNumberOfReservedLowIds(); id < highId && !context.isCancelled(); id++) {
            try {
                SchemaRecord record = reader.read(id);
                if (!record.inUse()) {
                    continue;
                }

                SchemaRule schemaRule = schemaStorage.loadSingleSchemaRule(id, storeCursors);
                SchemaRecord previousContentRecord =
                        verifiedRulesWithRecords.put(SchemaRuleKey.from(schemaRule), new SchemaRecord(record));
                if (previousContentRecord != null) {
                    reporter.forSchema(record).duplicateRuleContent(previousContentRecord);
                }

                if (schemaRule instanceof IndexDescriptor rule) {
                    if (rule.isUnique() && rule.getOwningConstraintId().isPresent()) {
                        var previousObligation = constraintObligations.put(
                                rule.getOwningConstraintId().getAsLong(),
                                new ConstraintObligation(new SchemaRecord(record), rule.getIndexType()));
                        if (previousObligation != null) {
                            reporter.forSchema(record).duplicateObligation(previousObligation.schemaRecord());
                        }
                    }
                } else if (schemaRule instanceof ConstraintDescriptor rule) {
                    if (rule.enforcesUniqueness()) {
                        SchemaRecord previousObligation = indexObligations.put(
                                rule.asIndexBackedConstraint().ownedIndexId(), new SchemaRecord(record));
                        if (previousObligation != null) {
                            reporter.forSchema(record).duplicateObligation(previousObligation);
                        }
                    }
                }
            } catch (MalformedSchemaRuleException e) {
                // This is OK, we'll report it below
            }
        }
    }

    private void performSchemaCheck(
            long highId,
            RecordReader<SchemaRecord> reader,
            MutableLongObjectMap<SchemaRecord> indexObligations,
            MutableLongObjectMap<ConstraintObligation> constraintObligations,
            SchemaStorage schemaStorage,
            MutableIntObjectMap<MutableIntSet> mandatoryNodeProperties,
            MutableIntObjectMap<MutableIntSet> mandatoryRelationshipProperties,
            MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedNodePropertyTypes,
            MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedRelationshipPropertyTypes,
            StoreCursors storeCursors,
            CursorContext cursorContext) {
        SchemaRecord record = reader.record();
        BasicSchemaCheck basicSchemaCheck = new BasicSchemaCheck(record, storeCursors);
        MandatoryPropertiesCollector mandatoryPropertiesCollector =
                new MandatoryPropertiesCollector(mandatoryNodeProperties, mandatoryRelationshipProperties);
        AllowedTypesCollector allowedTypesCollector =
                new AllowedTypesCollector(allowedNodePropertyTypes, allowedRelationshipPropertyTypes);
        var propertyValues = new IntObjectHashMap<Value>();
        try (var propertyReader = new SafePropertyChainReader(context, cursorContext, true)) {
            for (long id = schemaStore.getNumberOfReservedLowIds(); id < highId && !context.isCancelled(); id++) {
                try {
                    reader.read(id);
                    if (record.inUse()) {
                        propertyValues = lightReplace(propertyValues);
                        boolean propertyChainIsOk =
                                propertyReader.read(propertyValues, record, reporter::forSchema, storeCursors);
                        if (!propertyChainIsOk) {
                            reporter.forSchema(record).malformedSchemaRule();
                            continue;
                        }

                        SchemaRule schemaRule = schemaStorage.loadSingleSchemaRule(id, storeCursors);
                        basicSchemaCheck.check(schemaRule.schema());
                        if (schemaRule instanceof IndexDescriptor rule) {
                            if (rule.isUnique()) {
                                SchemaRecord obligation = indexObligations.get(rule.getId());
                                if (obligation == null) // no pointer to here
                                {
                                    if (rule.getOwningConstraintId()
                                            .isPresent()) // we only expect a pointer if we have an owner
                                    {
                                        reporter.forSchema(record).missingObligation(UNIQUENESS_CONSTRAINT);
                                    }
                                } else {
                                    // if someone points to here, it must be our owner
                                    OptionalLong owningConstraintId = rule.getOwningConstraintId();
                                    if (owningConstraintId.isEmpty()
                                            || obligation.getId() != owningConstraintId.getAsLong()) {
                                        reporter.forSchema(record).constraintIndexRuleNotReferencingBack(obligation);
                                    }
                                }
                            }
                            if (indexAccessors.notOnlineRules().contains(rule)) {
                                reporter.forSchema(record).schemaRuleNotOnline(rule);
                            }
                            if (indexAccessors.inconsistentRules().contains(rule)) {
                                reporter.forSchema(record).malformedSchemaRule();
                            }
                        } else if (schemaRule instanceof ConstraintDescriptor rule) {
                            if (rule.enforcesUniqueness()) {
                                ConstraintObligation obligation = constraintObligations.get(rule.getId());
                                if (obligation == null) {
                                    reporter.forSchema(record).missingObligation(CONSTRAINT_INDEX_RULE);
                                } else {
                                    if (obligation.schemaRecord().getId()
                                            != rule.asIndexBackedConstraint().ownedIndexId()) {
                                        reporter.forSchema(record)
                                                .uniquenessConstraintNotReferencingBack(obligation.schemaRecord());
                                    } else if (obligation.indexType()
                                            != rule.asIndexBackedConstraint().indexType()) {
                                        reporter.forSchema(record)
                                                .uniquenessConstraintReferencingIndexOfWrongType(
                                                        obligation.schemaRecord());
                                    }
                                }
                            }
                            if (rule.enforcesPropertyExistence()) {
                                mandatoryPropertiesCollector.collect(rule.schema());
                            }
                            if (rule.enforcesPropertyType()) {
                                allowedTypesCollector.collect(rule.asPropertyTypeConstraint());
                            }
                        } else {
                            reporter.forSchema(record).unsupportedSchemaRuleType(null);
                        }
                    }
                } catch (MalformedSchemaRuleException e) {
                    reporter.forSchema(record).malformedSchemaRule();
                }
            }
        }
    }

    private void checkTokens() throws Exception {
        execution.run(
                getClass().getSimpleName() + "-checkTokens",
                () -> checkTokens(
                        neoStores.getLabelTokenStore(),
                        reporter::forLabelName,
                        dynamicRecord -> reporter.forDynamicBlock(RecordType.LABEL_NAME, dynamicRecord),
                        context.contextFactory),
                () -> checkTokens(
                        neoStores.getRelationshipTypeTokenStore(),
                        reporter::forRelationshipTypeName,
                        dynamicRecord -> reporter.forDynamicBlock(RecordType.RELATIONSHIP_TYPE_NAME, dynamicRecord),
                        context.contextFactory),
                () -> checkTokens(
                        neoStores.getPropertyKeyTokenStore(),
                        reporter::forPropertyKey,
                        dynamicRecord -> reporter.forDynamicBlock(RecordType.PROPERTY_KEY_NAME, dynamicRecord),
                        context.contextFactory));
    }

    private static <R extends TokenRecord> void checkTokens(
            TokenStore<R> store,
            Function<R, ConsistencyReport.NameConsistencyReport> report,
            Function<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> dynamicRecordReport,
            CursorContextFactory contextFactory) {
        DynamicStringStore nameStore = store.getNameStore();
        DynamicRecord nameRecord = nameStore.newRecord();
        long highId = store.getIdGenerator().getHighId();
        LongHashSet seenNameRecordIds = new LongHashSet();
        int blockSize = store.getNameStore().getRecordDataSize();
        try (var cursorContext = contextFactory.create(CONSISTENCY_TOKEN_CHECKER_TAG);
                RecordReader<R> tokenReader = new RecordReader<>(store, true, cursorContext);
                RecordReader<DynamicRecord> nameReader =
                        new RecordReader<>(store.getNameStore(), false, cursorContext)) {
            for (long id = 0; id < highId; id++) {
                R record = tokenReader.read(id);
                if (record.inUse() && !NULL_REFERENCE.is(record.getNameId())) {
                    seenNameRecordIds = lightReplace(seenNameRecordIds);
                    safeLoadDynamicRecordChain(
                            r -> {},
                            nameReader,
                            seenNameRecordIds,
                            record.getNameId(),
                            blockSize,
                            (i, r) -> dynamicRecordReport.apply(nameRecord).circularReferenceNext(r),
                            (i, r) -> report.apply(record).nameBlockNotInUse(nameRecord),
                            (i, r) -> dynamicRecordReport.apply(nameRecord).nextNotInUse(r),
                            (i, r) -> dynamicRecordReport.apply(r).emptyBlock(),
                            r -> dynamicRecordReport.apply(r).recordNotFullReferencesNext(),
                            r -> dynamicRecordReport.apply(r).invalidLength());
                }
            }
        }
    }

    static Function<AbstractBaseRecord, String> moreDescriptiveRecordToStrings(
            NeoStores neoStores, TokenHolders tokenHolders) {
        return record -> {
            String result = record.toString();
            if (record instanceof SchemaRecord) {
                try (var storeCursors = new CachedStoreCursors(neoStores, CursorContext.NULL_CONTEXT)) {
                    SchemaRule schemaRule = SchemaStore.readSchemaRule(
                            (SchemaRecord) record, neoStores.getPropertyStore(), tokenHolders, storeCursors);
                    result += " (" + schemaRule.userDescription(tokenHolders) + ")";
                } catch (Exception e) {
                    result += " (schema user description not available due to: " + e + ")";
                }
            }
            return result;
        };
    }

    /**
     * This check is responsible to check, for a {@link SchemaDescriptor}, if all token
     * (label/relType/propKey) ids referenced by it are present in the store.
     */
    private final class BasicSchemaCheck {
        private final SchemaRecord record;
        private final StoreCursors storeCursors;

        BasicSchemaCheck(SchemaRecord record, StoreCursors storeCursors) {
            this.record = record;
            this.storeCursors = storeCursors;
        }

        public void check(SchemaDescriptor schema) {
            checkValidEntityTokensIds(schema);
            checkValidPropertyKeyIds(schema);
        }

        private void checkValidEntityTokensIds(SchemaDescriptor schema) {
            switch (schema.entityType()) {
                case NODE -> {
                    for (int labelTokenId : schema.getEntityTokenIds()) {
                        checkValidToken(
                                null,
                                labelTokenId,
                                tokenHolders.labelTokens(),
                                neoStores.getLabelTokenStore(),
                                (record, id) -> {},
                                (ignore, token) -> reporter.forSchema(record).labelNotInUse(token),
                                storeCursors);
                    }
                }
                case RELATIONSHIP -> {
                    for (int relationshipTypeTokenId : schema.getEntityTokenIds()) {
                        checkValidToken(
                                null,
                                relationshipTypeTokenId,
                                tokenHolders.relationshipTypeTokens(),
                                neoStores.getRelationshipTypeTokenStore(),
                                (record, id) -> {},
                                (ignore, token) -> reporter.forSchema(record).relationshipTypeNotInUse(token),
                                storeCursors);
                    }
                }
                default -> throw new IllegalArgumentException(
                        "Schema with given entity type is not supported: " + schema.entityType());
            }
        }

        private void checkValidPropertyKeyIds(SchemaDescriptor schema) {
            for (int propertyKeyId : schema.getPropertyIds()) {
                checkValidToken(
                        null,
                        propertyKeyId,
                        tokenHolders.propertyKeyTokens(),
                        neoStores.getPropertyKeyTokenStore(),
                        (record, id) -> {},
                        (ignore, token) -> reporter.forSchema(record).propertyKeyNotInUse(token),
                        storeCursors);
            }
        }
    }

    /**
     * This collector is used to accumulate all the mandatory properties found on all
     * {@link SchemaDescriptor}s passed to {@link #collect(SchemaDescriptor)}.
     */
    private static class MandatoryPropertiesCollector {
        private final MutableIntObjectMap<MutableIntSet> mandatoryNodeProperties;
        private final MutableIntObjectMap<MutableIntSet> mandatoryRelationshipProperties;

        MandatoryPropertiesCollector(
                MutableIntObjectMap<MutableIntSet> mandatoryNodeProperties,
                MutableIntObjectMap<MutableIntSet> mandatoryRelationshipProperties) {
            this.mandatoryNodeProperties = mandatoryNodeProperties;
            this.mandatoryRelationshipProperties = mandatoryRelationshipProperties;
        }

        public void collect(SchemaDescriptor schema) {
            MutableIntObjectMap<MutableIntSet> targetMap;
            if (schema.isLabelSchemaDescriptor()) {
                targetMap = mandatoryNodeProperties;
            } else if (schema.isRelationshipTypeSchemaDescriptor()) {
                targetMap = mandatoryRelationshipProperties;
            } else {
                // We want to process only LabelSchemaDescriptor and RelationshipTypeSchemaDescriptors.
                return;
            }

            for (int entityToken : schema.getEntityTokenIds()) {
                MutableIntSet keys = targetMap.getIfAbsentPut(entityToken, IntHashSet::new);
                keys.addAll(schema.getPropertyIds());
            }
        }
    }

    /**
     * This collector is used to accumulate all the allowed types for properties found on all
     * {@link TypeConstraintDescriptor}s passed to {@link #collect(TypeConstraintDescriptor)}.
     */
    private static class AllowedTypesCollector {
        private final MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedNodePropertyTypes;
        private final MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedRelationshipPropertyTypes;

        AllowedTypesCollector(
                MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedNodePropertyTypes,
                MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedRelationshipPropertyTypes) {
            this.allowedNodePropertyTypes = allowedNodePropertyTypes;
            this.allowedRelationshipPropertyTypes = allowedRelationshipPropertyTypes;
        }

        public void collect(TypeConstraintDescriptor constraintDescriptor) {
            var schema = constraintDescriptor.schema();

            MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> targetMap;
            if (schema.isLabelSchemaDescriptor()) {
                targetMap = allowedNodePropertyTypes;
            } else if (schema.isRelationshipTypeSchemaDescriptor()) {
                targetMap = allowedRelationshipPropertyTypes;
            } else {
                // We want to process only LabelSchemaDescriptor and RelationshipTypeSchemaDescriptors.
                return;
            }

            for (int entityToken : schema.getEntityTokenIds()) {
                var allowedTypesByPropertyKey = targetMap.getIfAbsentPut(entityToken, IntObjectHashMap::new);
                // We "know" that constraints that enforces the type for properties only target
                // a single property, otherwise we could have done a loop on `schema.getPropertyIds()`.
                allowedTypesByPropertyKey.put(schema.getPropertyId(), constraintDescriptor.propertyType());
            }
        }
    }

    private record ConstraintObligation(SchemaRecord schemaRecord, IndexType indexType) {}
}
