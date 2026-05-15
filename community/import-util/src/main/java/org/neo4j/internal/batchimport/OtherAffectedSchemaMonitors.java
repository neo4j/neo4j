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
package org.neo4j.internal.batchimport;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.eclipse.collections.api.factory.primitive.IntIntMaps;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.map.primitive.IntIntMap;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.input.ApplicationMode;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.schema.EndpointType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.internal.schema.constraints.NodeLabelExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelationshipEndpointLabelConstraintDescriptor;
import org.neo4j.internal.schema.constraints.TypeRepresentation;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.values.storable.Value;

/**
 * uniqueness constraint index (prepare copies these):
 * - neo4j/schema/index/range-1.0/3
 * - copy to neo4j-incremental-12345/schema/index/range-1.0/3
 * - build: neo4j-incremental-12345/temp-schema/index/range-1.0/3
 *    - validate(): merge neo4j-incremental-12345/temp-schema/index/range-1.0/3 --> neo4j-incremental-12345/schema/index/range-1.0/3
 * - merge: move neo4j-incremental-12345/schema/index/range-1.0/3 --> neo4j/schema/index/range-1.0/3
 *<p>
 * non-uniqueness constraint index (prepare does not copy these):
 * - neo4j/schema/index/range-1.0/3
 * - build: neo4j-incremental-12345/temp-schema/index/range-1.0/3
 *    - validate(): move neo4j-incremental-12345/temp-schema/index/range-1.0/3 --> neo4j-incremental-12345/schema/index/range-1.0/3
 * - merge: merge neo4j-incremental-12345/schema/index/range-1.0/3 --> neo4j/schema/index/range-1.0/3
 *<p>
 * Regarding index update, and especially uniqueness indexes, this class focuses on the ability to make decisions
 * based on the data given to it - (for updates) not existing data of the entity. Almost all checks and
 * generated updates can be made based on the new data (delta for updates), and for the rest there's
 * {@link SchemaMonitor#indexUpdate(IndexEntryUpdate)}.
 */
public class OtherAffectedSchemaMonitors implements SchemaMonitors {
    private final SchemaCache schemaCache;
    private final EntityType entityType;
    private final LongToLongFunction indexedEntityIdConverter;
    private final boolean generateNonUniqueIndexUpdates;
    private final ImportPropertyConstraintEnforcer propertyConstraints;
    private final ImportIndexBuilder indexBuilder;
    private final TokenNameLookup tokenNameLookup;
    private final IntFunction<NodeLabelChecker> nodeLabelCheckerFactory;

    private final IntObjectMap<IntSet> requiredNodeLabels;
    private final IntIntMap requiredStartEndpointLabels;
    private final IntIntMap requiredEndEndpointLabels;

    public OtherAffectedSchemaMonitors(
            FileSystemAbstraction fileSystem,
            IndexProviderMap indexProviderMap,
            IndexProviderMap tempIndexes,
            SchemaCache schemaCache,
            TokenNameLookup tokenNameLookup,
            EntityType entityType,
            ImmutableSet<OpenOption> openOptions,
            PopulationWorkJobScheduler workScheduler,
            LongToLongFunction indexedEntityIdConverter,
            LongToLongFunction entityIdFromIndexIdConverter,
            Configuration configuration,
            IndexStatisticsStore indexStatisticsStore,
            StorageEngineIndexingBehaviour indexingBehaviour,
            boolean generateNonUniqueIndexUpdates,
            Predicate<IndexDescriptor> excludedIndexes,
            Config config,
            IndexPopulator.Configuration indexPopulatorConfiguration,
            IntFunction<NodeLabelChecker> nodeLabelCheckerFactory) {
        this.schemaCache = schemaCache;
        this.entityType = entityType;
        this.indexedEntityIdConverter = indexedEntityIdConverter;
        this.generateNonUniqueIndexUpdates = generateNonUniqueIndexUpdates;

        this.propertyConstraints = new ImportPropertyConstraintEnforcer(schemaCache, entityType);
        this.requiredNodeLabels = buildRequiredNodeLabels(schemaCache);

        this.requiredStartEndpointLabels = buildRequiredEndpointLabel(schemaCache, EndpointType.START);
        this.requiredEndEndpointLabels = buildRequiredEndpointLabel(schemaCache, EndpointType.END);
        this.nodeLabelCheckerFactory = Objects.requireNonNull(nodeLabelCheckerFactory);

        this.indexBuilder = new ImportIndexBuilder(
                fileSystem,
                indexProviderMap,
                tempIndexes,
                tokenNameLookup,
                openOptions,
                workScheduler,
                // We'll do this conversion ourselves when constructing the updates
                id -> id,
                entityIdFromIndexIdConverter,
                configuration,
                indexStatisticsStore,
                indexingBehaviour,
                excludedIndexes,
                config,
                indexPopulatorConfiguration);
        this.tokenNameLookup = tokenNameLookup;
    }

    private IntObjectMap<IntSet> buildRequiredNodeLabels(SchemaCache schemaCache) {
        MutableIntObjectMap<MutableIntSet> build = IntObjectMaps.mutable.empty();
        for (var constraint : schemaCache.constraints()) {
            if (constraint.isNodeLabelExistenceConstraint()
                    && constraint instanceof NodeLabelExistenceConstraintDescriptor desc) {
                int labelId = desc.schemaLabelId();
                var requirements = build.getIfAbsentPut(labelId, IntSets.mutable::empty);
                requirements.add(desc.requiredLabelId());
            }
        }

        // Finalize the construction, by making the map immutable.
        MutableIntObjectMap<IntSet> finalize = IntObjectMaps.mutable.ofInitialCapacity(build.size());
        build.forEachKey(key -> finalize.put(key, build.get(key).asUnmodifiable()));

        return finalize.asUnmodifiable();
    }

    private IntIntMap buildRequiredEndpointLabel(SchemaCache schemaCache, EndpointType endpointType) {
        MutableIntIntMap build = IntIntMaps.mutable.empty();
        for (var constraint : schemaCache.constraints()) {
            if (constraint instanceof RelationshipEndpointLabelConstraintDescriptor desc
                    && desc.endpointType() == endpointType) {
                build.put(desc.schema().getRelTypeId(), desc.endpointLabelId());
            }
        }

        // Finalize the construction, by making the map immutable.
        return build.asUnmodifiable();
    }

    /**
     * Will be invoked once for each worker, i.e. this should create a new monitor used by a single thread.
     */
    @Override
    public SchemaMonitor newMonitor(int workerId) {
        if (requiredStartEndpointLabels.notEmpty() || requiredEndEndpointLabels.notEmpty()) {
            return new OtherAffectedSchemaMonitor(nodeLabelCheckerFactory.apply(workerId));
        }
        // Since there are no conditions to verify, we don't need to allocate a NodeLabelChecker
        return new OtherAffectedSchemaMonitor(null);
    }

    /**
     * @return a list of entity IDs that violated constraints during complete (e.g. merging of indexes).
     */
    @Override
    public LongSet validate(Collector collector, ProgressMonitorFactory progressMonitorFactory) throws IOException {
        return indexBuilder.validate(collector, progressMonitorFactory);
    }

    @Override
    public void writeToTarget(
            LongPredicate violatingIdMapperEntityIds,
            LongSet otherViolatingEntityIds,
            ProgressMonitorFactory progressMonitorFactory) {
        indexBuilder.writeToTarget(violatingIdMapperEntityIds, otherViolatingEntityIds, progressMonitorFactory);
    }

    @Override
    public void close() throws IOException {
        indexBuilder.close();
    }

    @Override
    public LongSet affectedIndexes() {
        return indexBuilder.affectedIndexes();
    }

    @Override
    public Optional<IndexAccessor> openTempIndexAccessor(long indexId) throws IOException {
        return indexBuilder.openTempIndexAccessor(indexId);
    }

    @Override
    public Optional<IndexAccessor> openTargetIndexAccessor(long indexId) {
        return Optional.of(indexBuilder.openTargetIndexAccessor(schemaCache.getIndex(indexId)));
    }

    @Override
    public Optional<IndexDescriptor> indexDescriptor(long indexId) {
        return indexBuilder.indexDescriptor(indexId);
    }

    private class OtherAffectedSchemaMonitor implements SchemaMonitor {
        private final NodeLabelChecker nodeLabelChecker;

        OtherAffectedSchemaMonitor(NodeLabelChecker nodeLabelChecker) {
            this.nodeLabelChecker = nodeLabelChecker;
        }

        @Override
        public boolean handle(
                Entity entity,
                ExistingPropertyKeysLookup existingPropertyKeysLookup,
                ViolationVisitor violationVisitor,
                UniquenessIndexUpdatesListener uniquenessIndexUpdatesListener) {
            try {
                var mode = entity.mode;
                if (mode == null || mode == ApplicationMode.CREATE) {
                    boolean constraintsOk = true;
                    constraintsOk &= checkPropertyExistenceConstraintsOnCreate(entity, violationVisitor);
                    constraintsOk &= checkPropertyTypeConstraints(entity, violationVisitor);
                    if (entity instanceof Relationship rel) {
                        constraintsOk &= checkRelationshipEndpointLabels(rel, violationVisitor);
                    } else {
                        constraintsOk &= checkNodeLabelExistence(entity, entity.entityTokens, violationVisitor);
                    }

                    if (constraintsOk && generateNonUniqueIndexUpdates) {
                        // For CREATE all index updates are simply added to each respective index populator
                        // and uniqueness violations will be sorted out afterward, where violating entities
                        // are deleted.
                        generateIndexUpdatesForCreatedEntity(entity);
                    }
                    return constraintsOk;
                } else if (mode == ApplicationMode.UPDATE) {
                    boolean constraintsOk = true;
                    constraintsOk &= checkPropertyExistenceConstraintsOnUpdate(
                            entity, existingPropertyKeysLookup, violationVisitor);
                    constraintsOk &= checkPropertyTypeConstraints(entity, violationVisitor);
                    if (entity instanceof Relationship rel) {
                        constraintsOk &= checkRelationshipEndpointLabels(rel, violationVisitor);
                    } else {
                        IntSet labels = entity.existingEntityTokens
                                .union(entity.entityTokens)
                                .difference(entity.removedEntityTokens);
                        constraintsOk &= checkNodeLabelExistence(entity, labels, violationVisitor);
                    }

                    if (constraintsOk) {
                        // For UPDATE at least uniqueness index updates needs to be generated so that their
                        // ADD part can be written to the indexes and validated right here.
                        return generateAndValidateUniquenessIndexUpdates(
                                entity, violationVisitor, uniquenessIndexUpdatesListener);
                    } else {
                        return false;
                    }
                } else {
                    return true;
                }
            } finally {
                uniquenessIndexUpdatesListener.endEntity();
            }
        }

        private boolean checkPropertyExistenceConstraintsOnUpdate(
                Entity entity,
                ExistingPropertyKeysLookup existingPropertyKeysLookup,
                ViolationVisitor violationVisitor) {
            if (!entity.entityTokens.isEmpty()) {
                // Check if all added properties in this input entity satisfies the mandatory properties
                // if not then we must load the existing property keys for this node and check
                var mandatoryPropertyKeys = propertyConstraints.mandatoryPropertyKeys(entity.sortedEntityTokens());
                if (!mandatoryPropertyKeys.isEmpty()) {
                    var mandatoryPropertyKeysLeftToCheck = IntSets.mutable.ofAll(mandatoryPropertyKeys);
                    entity.propertiesMap().keySet().forEach(mandatoryPropertyKeysLeftToCheck::remove);
                    if (!mandatoryPropertyKeysLeftToCheck.isEmpty()) {
                        var existingRemainingKeys = existingPropertyKeysLookup.lookupPropertyKeys(
                                entity.entityId, mandatoryPropertyKeysLeftToCheck);
                        existingRemainingKeys.forEach(key -> {
                            if (!entity.removedProperties.contains(key)) {
                                mandatoryPropertyKeysLeftToCheck.remove(key);
                            }
                        });
                    }
                    if (!mandatoryPropertyKeysLeftToCheck.isEmpty()) {
                        violationVisitor.accept(entity, "a property existence constraint");
                        return false;
                    }
                }
            }

            if (!entity.removedProperties.isEmpty()) {
                var affectedLabels =
                        propertyConstraints.entityTokensRelatedToPropertyKeys(entity.removedProperties.toArray());
                if (!affectedLabels.isEmpty()) {
                    var affectedLabelsLeftToCheck = IntSets.mutable.ofAll(affectedLabels);
                    affectedLabelsLeftToCheck.removeAll(entity.removedEntityTokens);
                    if (affectedLabelsLeftToCheck.containsAny(entity.existingEntityTokens)
                            || affectedLabelsLeftToCheck.containsAny(entity.entityTokens)) {
                        violationVisitor.accept(entity, "a property existence constraint");
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * Used for allowing external code, which is applying changes to the store, generate relevant index updates
         * for this entity. Any updates to uniqueness indexes will be converted to removals (of the before-values),
         * making this method essential and interlinked with
         * {@link SchemaMonitor#handle(Entity, ExistingPropertyKeysLookup, ViolationVisitor, UniquenessIndexUpdatesListener)} for updated entities.
         * @see #generateAndValidateUniquenessIndexUpdates(Entity, ViolationVisitor, UniquenessIndexUpdatesListener)
         */
        @Override
        public void indexUpdate(IndexEntryUpdate indexUpdate) {
            // TODO can we make this general assumption here? It's probably good because the splitting of
            //  uniqueness index updates to just do the ADD part is _also_ in this monitor.
            if (indexUpdate.indexKey().isUnique() && indexUpdate.updateMode() == UpdateMode.CHANGED) {
                var valueUpdate = (ValueIndexEntryUpdate) indexUpdate;
                indexUpdate = EagerValueIndexEntryUpdate.remove(
                        indexUpdate.getEntityId(), indexUpdate.indexKey(), valueUpdate.beforeValues());
            }

            indexBuilder.add(indexUpdate);
        }

        @Override
        public boolean directIndexUpdate(IndexEntryUpdate indexUpdate) {
            return indexBuilder.addDirect(indexUpdate);
        }

        @Override
        public boolean checkUniqueness(EagerValueIndexEntryUpdate[] checks) {
            return indexBuilder.checkUniqueness(checks);
        }

        @Override
        public boolean checkNodeLabelExistence(
                SchemaMonitor.Entity entity, IntSet nodeLabels, ViolationVisitor violationVisitor) {
            MutableBoolean success = new MutableBoolean(true);
            nodeLabels.forEach(nodeLabel -> {
                var requiredLabels = requiredNodeLabels.getIfAbsent(nodeLabel, IntSets.immutable::empty);
                IntSet missingLabels = requiredLabels.difference(nodeLabels);
                if (!missingLabels.isEmpty()) {
                    success.setFalse();
                    // Report violations
                    missingLabels.forEach(missing -> violationVisitor.accept(
                            entity,
                            "Node(%d) with label %s is required to have label %s"
                                    .formatted(
                                            entity.entityId,
                                            tokenNameLookup.labelGetName(nodeLabel),
                                            tokenNameLookup.labelGetName(missing))));
                }
            });
            return success.get();
        }

        private boolean checkRelationshipEndpointLabels(
                SchemaMonitor.Relationship relationship, ViolationVisitor violationVisitor) {
            if (nodeLabelChecker == null) {
                // Skip, if there is no nodeLabelChecker (the entity itself is not sufficient to determine if the
                // constraints holds).
                return true;
            }

            int relType = relationship.relationshipType();

            boolean okStart = true;
            boolean okEnd = true;

            int requiredStartEndpointLabel = requiredStartEndpointLabels.getIfAbsent(relType, TokenConstants.NO_TOKEN);
            if (requiredStartEndpointLabel != TokenConstants.NO_TOKEN) {
                okStart = nodeLabelChecker.nodeHasLabel(relationship.startNodeId, requiredStartEndpointLabel);
            }

            int requiredEndEndpointLabel = requiredEndEndpointLabels.getIfAbsent(relType, TokenConstants.NO_TOKEN);
            if (requiredEndEndpointLabel != TokenConstants.NO_TOKEN) {
                okEnd = nodeLabelChecker.nodeHasLabel(relationship.endNodeId, requiredEndEndpointLabel);
            }

            if (!okStart && !okEnd) {
                violationVisitor.accept(
                        relationship,
                        "relationship type %s is required to start with label %s and end with label %s"
                                .formatted(
                                        tokenNameLookup.relationshipTypeGetName(relType),
                                        tokenNameLookup.labelGetName(requiredStartEndpointLabel),
                                        tokenNameLookup.labelGetName(requiredEndEndpointLabel)));
                return false;
            } else if (!okStart) {
                violationVisitor.accept(
                        relationship,
                        "relationship type %s is required to start with label %s"
                                .formatted(
                                        tokenNameLookup.relationshipTypeGetName(relType),
                                        tokenNameLookup.labelGetName(requiredStartEndpointLabel)));

                return false;
            } else if (!okEnd) {
                violationVisitor.accept(
                        relationship,
                        "relationship type %s is required to end with label %s"
                                .formatted(
                                        tokenNameLookup.relationshipTypeGetName(relType),
                                        tokenNameLookup.labelGetName(requiredEndEndpointLabel)));
                return false;
            } else {
                return true;
            }
        }

        @Override
        public void close() {
            IOUtils.closeUnchecked(nodeLabelChecker);
            indexBuilder.flushOnSchemaMonitorClose();
        }

        private void generateIndexUpdatesForCreatedEntity(Entity entity) {
            var propertyKeyTokens = entity.propertiesMap().keySet().toSortedArray();
            var indexes = schemaCache.getValueIndexesRelatedTo(
                    entity.sortedEntityTokens(), EMPTY_INT_ARRAY, propertyKeyTokens, true, entityType);
            for (var index : indexes) {
                indexBuilder.add(constructIndexUpdate(entity, index));
            }
        }

        /**
         * Used when updating an entity. Based on the data known about this entity, and also assuming that
         * uniqueness indexes always consists of a single entityToken/propertyKey pair, see which uniqueness indexes
         * this entity is associated with and attempt to add this entity into all such uniqueness indexes.
         * The trick here is that we don't need to know the previous values of the properties - instead just
         * add the new values to the affected indexes (atomically, i.e. if any fails then undo them all).
         * This relies on the actual index updates carrying the before-values will arrive later in
         * calls to {@link #indexUpdate(IndexEntryUpdate)}, where such updates will be transformed into
         * only removal of the before-values - thereby completing the updates.
         */
        private boolean generateAndValidateUniquenessIndexUpdates(
                Entity entity,
                ViolationVisitor violationVisitor,
                UniquenessIndexUpdatesListener uniquenessIndexUpdatesListener) {
            var properties = entity.propertiesMap();
            var identifierPropertyKeys = entity.identifierPropertyKeys();
            var propertyKeyTokens = identifierPropertyKeys.isEmpty()
                    ? properties.keySet().toSortedArray()
                    : properties
                            .keySet()
                            .reject(identifierPropertyKeys::contains)
                            .toSortedArray();
            var allEntityTokens = IntSets.mutable.ofAll(entity.existingEntityTokens);
            allEntityTokens.addAll(entity.entityTokens);
            var indexes = schemaCache.getValueIndexesRelatedTo(
                    allEntityTokens.toSortedArray(), EMPTY_INT_ARRAY, propertyKeyTokens, true, entityType);
            if (!indexes.isEmpty()) {
                List<EagerValueIndexEntryUpdate> appliedAdditions = new ArrayList<>();
                boolean failed = false;
                for (var index : indexes) {
                    if (index.isUnique()) {
                        var indexUpdate = constructIndexUpdate(entity, index);
                        if (!uniquenessIndexUpdatesListener.shouldApply(indexUpdate)) {
                            continue;
                        }
                        if (indexBuilder.addDirect(indexUpdate)) {
                            appliedAdditions.add(indexUpdate);
                        } else {
                            failed = true;
                            violationVisitor.accept(entity, index.toString());
                            break;
                        }
                    }
                }
                if (failed) {
                    for (var updateToUndo : appliedAdditions) {
                        boolean removed = indexBuilder.addDirect(EagerValueIndexEntryUpdate.remove(
                                updateToUndo.getEntityId(), updateToUndo.indexKey(), updateToUndo.values()));
                        assert removed;
                    }
                    return false;
                } else {
                    uniquenessIndexUpdatesListener.updates(appliedAdditions);
                }
            }
            return true;
        }

        private EagerValueIndexEntryUpdate constructIndexUpdate(Entity entity, IndexDescriptor index) {
            var propertyIds = index.schema().getPropertyIds();
            Value[] values = new Value[propertyIds.length];
            for (int i = 0; i < propertyIds.length; i++) {
                values[i] = entity.propertiesMap().get(propertyIds[i]);
            }
            return EagerValueIndexEntryUpdate.add(indexedEntityIdConverter.applyAsLong(entity.entityId), index, values);
        }

        private boolean checkPropertyExistenceConstraintsOnCreate(Entity entity, ViolationVisitor violationVisitor) {
            if (propertyConstraints.hasPropertyExistenceConstraints()) {
                for (int entityToken : entity.sortedEntityTokens()) {
                    var mandatoryPropertyKeys = propertyConstraints.mandatoryPropertyKeys(entityToken);
                    if (mandatoryPropertyKeys != null) {
                        if (!entity.propertiesMap().keySet().containsAll(mandatoryPropertyKeys)) {
                            violationVisitor.accept(entity, "a property existence constraint");
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        private boolean checkPropertyTypeConstraints(Entity entity, ViolationVisitor violationVisitor) {
            if (propertyConstraints.hasPropertyTypeConstraints()) {
                return checkPropertyTypeConstraints(entity, violationVisitor, entity.sortedEntityTokens())
                        && checkPropertyTypeConstraints(entity, violationVisitor, entity.sortedExistingEntityTokens());
            }
            return true;
        }

        private boolean checkPropertyTypeConstraints(Entity entity, ViolationVisitor violationVisitor, int[] tokens) {
            for (int token : tokens) {
                for (var typeConstraint : propertyConstraints.propertyTypeConstraints(token)) {
                    var value = entity.propertiesMap().get(typeConstraint.propertyKeyId());
                    if (TypeRepresentation.disallows(typeConstraint.type(), value)) {
                        violationVisitor.accept(entity, "a property type constraint " + value);
                        return false;
                    }
                }
            }
            return true;
        }
    }

    public interface NodeLabelChecker extends Closeable {
        boolean nodeHasLabel(long nodeId, int labelId);

        @Override
        default void close() {}

        IntFunction<NodeLabelChecker> DISABLED_NODE_CHECKER_FACTORY = (ignored) -> null;
    }
}
