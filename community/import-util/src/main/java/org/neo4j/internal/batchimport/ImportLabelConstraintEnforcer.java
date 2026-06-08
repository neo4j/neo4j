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

import java.io.IOException;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.function.LongPredicate;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.factory.primitive.IntIntMaps;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.map.primitive.IntIntMap;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.neo4j.batchimport.api.input.ApplicationMode;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.batchimport.OtherAffectedSchemaMonitors.NodeLabelChecker;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.schema.EndpointType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.constraints.NodeLabelExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelationshipEndpointLabelConstraintDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.token.api.TokenConstants;

public record ImportLabelConstraintEnforcer(
        TokenNameLookup tokenNameLookup,
        IntFunction<NodeLabelChecker> nodeLabelCheckerFactory,
        IntObjectMap<IntSet> requiredNodeLabels,
        IntIntMap requiredStartEndpointLabels,
        IntIntMap requiredEndEndpointLabels)
        implements SchemaMonitors {

    public static ImportLabelConstraintEnforcer of(
            SchemaCache schemaCache,
            TokenNameLookup tokenNameLookup,
            IntFunction<NodeLabelChecker> nodeLabelCheckerFactory) {
        return new ImportLabelConstraintEnforcer(
                tokenNameLookup,
                nodeLabelCheckerFactory,
                buildRequiredNodeLabels(schemaCache),
                buildRequiredEndpointLabel(schemaCache, EndpointType.START),
                buildRequiredEndpointLabel(schemaCache, EndpointType.END));
    }

    @Override
    public LongSet validate(Collector collector, ProgressMonitorFactory progressMonitorFactory) throws IOException {
        return LongSets.immutable.empty();
    }

    @Override
    public void writeToTarget(
            LongPredicate violatingIdMapperEntityIds,
            LongSet otherViolatingEntityIds,
            ProgressMonitorFactory progressMonitorFactory)
            throws IOException {}

    @Override
    public LongSet affectedIndexes() {
        return LongSets.immutable.empty();
    }

    @Override
    public Optional<IndexDescriptor> indexDescriptor(long indexId) {
        return Optional.empty();
    }

    @Override
    public Optional<IndexAccessor> openTempIndexAccessor(long indexId) throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<IndexAccessor> openTargetIndexAccessor(long indexId) {
        return Optional.empty();
    }

    @Override
    public SchemaMonitor newMonitor(int workerId) {
        return newWorker(workerId);
    }

    public ImportLabelConstraintMonitor newWorker(int workerId) {
        NodeLabelChecker checker = null;
        if (requiredStartEndpointLabels.notEmpty() || requiredEndEndpointLabels.notEmpty()) {
            checker = nodeLabelCheckerFactory.apply(workerId);
        }
        return new ImportLabelConstraintMonitor(checker);
    }

    @Override
    public void close() throws IOException {}

    public class ImportLabelConstraintMonitor implements SchemaMonitor {
        private NodeLabelChecker nodeLabelChecker;

        private ImportLabelConstraintMonitor(NodeLabelChecker nodeLabelChecker) {
            this.nodeLabelChecker = nodeLabelChecker;
        }

        public boolean handle(SchemaMonitor.Entity entity, SchemaMonitor.ViolationVisitor violationVisitor) {
            var mode = entity.mode;
            boolean constraintsOk = true;
            if (mode == null || mode == ApplicationMode.CREATE) {
                if (entity instanceof SchemaMonitor.Relationship rel) {
                    constraintsOk &= checkRelationshipEndpointLabels(rel, violationVisitor);
                } else {
                    constraintsOk &= checkNodeLabelExistence(entity, entity.entityTokens, violationVisitor);
                }
            } else if (mode == ApplicationMode.UPDATE) {
                if (entity instanceof SchemaMonitor.Relationship rel) {
                    constraintsOk &= checkRelationshipEndpointLabels(rel, violationVisitor);
                } else {
                    IntSet labels = entity.existingEntityTokens
                            .union(entity.entityTokens)
                            .difference(entity.removedEntityTokens);
                    constraintsOk &= checkNodeLabelExistence(entity, labels, violationVisitor);
                }
            }
            return constraintsOk;
        }

        @Override
        public boolean handle(
                Entity entity,
                ExistingPropertyKeysLookup existingPropertyKeysLookup,
                ViolationVisitor violationVisitor,
                UniquenessIndexUpdatesListener uniquenessIndexUpdatesListener) {
            return handle(entity, violationVisitor);
        }

        @Override
        public void indexUpdate(IndexEntryUpdate indexUpdate) {}

        @Override
        public boolean directIndexUpdate(IndexEntryUpdate indexUpdate) {
            return false;
        }

        @Override
        public boolean checkUniqueness(EagerValueIndexEntryUpdate[] checks) {
            return true;
        }

        public boolean checkNodeLabelExistence(
                SchemaMonitor.Entity entity, IntSet nodeLabels, SchemaMonitor.ViolationVisitor violationVisitor) {
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

        @Override
        public void close() {
            IOUtils.closeUnchecked(nodeLabelChecker);
        }

        public boolean checkRelationshipEndpointLabels(
                SchemaMonitor.Relationship relationship, SchemaMonitor.ViolationVisitor violationVisitor) {
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
    }

    private static IntObjectMap<IntSet> buildRequiredNodeLabels(SchemaCache schemaCache) {
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

    private static IntIntMap buildRequiredEndpointLabel(SchemaCache schemaCache, EndpointType endpointType) {
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
}
