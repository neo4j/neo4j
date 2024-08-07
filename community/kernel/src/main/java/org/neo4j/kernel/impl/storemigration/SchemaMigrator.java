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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.kernel.impl.storemigration.SchemaStore44MigrationUtil.asRangeBackedConstraint;
import static org.neo4j.kernel.impl.storemigration.SchemaStore44MigrationUtil.asRangeIndex;
import static org.neo4j.token.api.TokenConstants.NO_TOKEN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.neo4j.batchimport.api.ReadBehaviour;
import org.neo4j.batchimport.api.ReadBehaviour.PropertyInclusion;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.GraphTypeDependence;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.RelationshipEndpointSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.newapi.ReadOnlyTokenRead;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.SchemaRule44;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccessExtended;
import org.neo4j.token.TokenHolders;

public class SchemaMigrator {

    private SchemaMigrator() {}

    public static List<SchemaRule> migrateSchemaRules(
            StorageEngineFactory fromStorage,
            StorageEngineFactory toStorage,
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            DatabaseLayout from,
            DatabaseLayout toLayout,
            boolean from44store,
            CursorContextFactory contextFactory,
            LogTailMetadata fromTailMetadata,
            boolean forceBtreeIndexesToRange,
            ReadBehaviour readBehaviour)
            throws IOException, KernelException {
        // Need to start the stores with the correct logTail since some stores depend on tx-id.
        LogTailExtractor logTailExtractor = new LogTailExtractor(fs, config, toStorage, DatabaseTracers.EMPTY);
        LogTailMetadata logTail = logTailExtractor.getTailMetadata(toLayout, EmptyMemoryTracker.INSTANCE);

        var tokenHolders =
                fromStorage.loadReadOnlyTokens(fs, from, config, pageCache, pageCacheTracer, true, contextFactory);

        ArrayList<SchemaRule> skippedSchemaRules = new ArrayList<>();

        try (SchemaRuleMigrationAccessExtended schemaRuleMigrationAccess = toStorage.schemaRuleMigrationAccess(
                fs,
                pageCache,
                pageCacheTracer,
                config,
                toLayout,
                contextFactory,
                EmptyMemoryTracker.INSTANCE,
                logTail)) {
            TokenRead tokenRead = new ReadOnlyTokenRead(tokenHolders);

            LongObjectHashMap<IndexToConnect> indexesToConnect = new LongObjectHashMap<>();
            LongObjectHashMap<ConstraintToConnect> constraintsToConnect = new LongObjectHashMap<>();
            // Write the rules to the new store.
            //  - Translating the tokens since their ids might be different
            for (var schemaRule : getSrcSchemaRules(
                    fromStorage,
                    fs,
                    pageCache,
                    pageCacheTracer,
                    config,
                    from,
                    contextFactory,
                    from44store,
                    fromTailMetadata,
                    forceBtreeIndexesToRange,
                    tokenHolders)) {
                if (schemaRule instanceof IndexDescriptor indexDescriptor) {
                    try {
                        if (indexDescriptor.isTokenIndex()) {
                            // Skip token index since they have already been created by the copy operation
                            continue;
                        }
                        if (shouldSkipSinceFiltered(readBehaviour, tokenHolders, indexDescriptor.schema())) {
                            // Skip filtered out indexes
                            skippedSchemaRules.add(indexDescriptor);
                            continue;
                        }

                        SchemaDescriptor schema = translateToNewSchema(
                                indexDescriptor.schema(), tokenRead, schemaRuleMigrationAccess.tokenHolders());

                        IndexPrototype newPrototype = indexDescriptor.isUnique()
                                ? IndexPrototype.uniqueForSchema(schema, indexDescriptor.getIndexProvider())
                                : IndexPrototype.forSchema(schema, indexDescriptor.getIndexProvider());
                        newPrototype = newPrototype
                                .withName(indexDescriptor.getName())
                                .withIndexType(indexDescriptor.getIndexType())
                                .withIndexConfig(indexDescriptor.getIndexConfig());

                        if (indexDescriptor.isUnique()) {
                            // Handle constraint indexes later
                            indexesToConnect.put(
                                    indexDescriptor.getId(),
                                    new IndexToConnect(
                                            indexDescriptor.getId(),
                                            indexDescriptor.getOwningConstraintId(),
                                            newPrototype));
                        } else {
                            IndexDescriptor newDescriptor =
                                    newPrototype.materialise(schemaRuleMigrationAccess.nextId());
                            schemaRuleMigrationAccess.writeSchemaRule(newDescriptor);
                        }
                    } catch (Exception e) {
                        readBehaviour.error(e, "Could not copy %s", indexDescriptor.userDescription(tokenHolders));
                    }
                } else if (schemaRule instanceof ConstraintDescriptor constraintDescriptor) {
                    try {
                        if (shouldSkipSinceFiltered(readBehaviour, tokenHolders, constraintDescriptor.schema())) {
                            // Skip filtered out constraint
                            skippedSchemaRules.add(constraintDescriptor);
                            continue;
                        }
                        SchemaDescriptor schema = translateToNewSchema(
                                constraintDescriptor.schema(), tokenRead, schemaRuleMigrationAccess.tokenHolders());
                        ConstraintDescriptor descriptor =
                                switch (constraintDescriptor.type()) {
                                    case UNIQUE -> {
                                        IndexBackedConstraintDescriptor indexBacked =
                                                constraintDescriptor.asIndexBackedConstraint();
                                        yield ConstraintDescriptorFactory.uniqueForSchema(
                                                schema, indexBacked.indexType());
                                    }
                                    case EXISTS -> ConstraintDescriptorFactory.existsForSchema(
                                            schema,
                                            constraintDescriptor.graphTypeDependence()
                                                    == GraphTypeDependence.DEPENDENT);
                                    case UNIQUE_EXISTS -> {
                                        IndexBackedConstraintDescriptor indexBacked =
                                                constraintDescriptor.asIndexBackedConstraint();
                                        yield ConstraintDescriptorFactory.keyForSchema(schema, indexBacked.indexType());
                                    }
                                    case PROPERTY_TYPE -> ConstraintDescriptorFactory.typeForSchema(
                                            schema,
                                            constraintDescriptor
                                                    .asPropertyTypeConstraint()
                                                    .propertyType(),
                                            constraintDescriptor.graphTypeDependence()
                                                    == GraphTypeDependence.DEPENDENT);
                                    case ENDPOINT -> {
                                        var relEndpointSchemaDescriptor =
                                                constraintDescriptor.asRelationshipEndpointConstraint();
                                        yield ConstraintDescriptorFactory.relationshipEndpointForSchema(
                                                schema.asSchemaDescriptorType(
                                                        RelationshipEndpointSchemaDescriptor.class),
                                                relEndpointSchemaDescriptor.endpointLabelId(),
                                                relEndpointSchemaDescriptor.endpointType());
                                    }
                                };
                        descriptor = descriptor.withName(constraintDescriptor.getName());

                        if (descriptor.isIndexBackedConstraint()) {
                            // Handle index-backed constraints later
                            constraintsToConnect.put(
                                    constraintDescriptor.getId(),
                                    new ConstraintToConnect(
                                            constraintDescriptor.getId(),
                                            constraintDescriptor
                                                    .asIndexBackedConstraint()
                                                    .ownedIndexId(),
                                            descriptor));
                        } else {
                            descriptor = descriptor.withId(schemaRuleMigrationAccess.nextId());
                            schemaRuleMigrationAccess.writeSchemaRule(descriptor);
                        }
                    } catch (Exception e) {
                        readBehaviour.error(e, "Could not copy %s", constraintDescriptor.userDescription(tokenHolders));
                    }
                }
            }

            // Time to handle constraint/index connections
            for (ConstraintToConnect constraintToConnect : constraintsToConnect.values()) {
                IndexToConnect indexToConnect = indexesToConnect.remove(constraintToConnect.indexId);
                if (indexToConnect == null
                        || (indexToConnect.oldConstraintId.isPresent()
                                && indexToConnect.oldConstraintId.getAsLong() != constraintToConnect.oldId)) {
                    throw new UnderlyingStorageException(
                            "Encountered an inconsistent schema store - can not migrate. Affected rules have id "
                                    + constraintToConnect.oldId
                                    + (indexToConnect != null ? " and " + indexToConnect.oldId : ""));
                }

                long newIndexId = schemaRuleMigrationAccess.nextId();
                long newConstraintId = schemaRuleMigrationAccess.nextId();
                schemaRuleMigrationAccess.writeSchemaRule(
                        indexToConnect.prototype.materialise(newIndexId).withOwningConstraintId(newConstraintId));
                schemaRuleMigrationAccess.writeSchemaRule(
                        constraintToConnect.prototype.withId(newConstraintId).withOwnedIndexId(newIndexId));
            }

            // There shouldn't be any really, but it can happen - for example when crashing in a constraint creation.
            // Letting these through.
            for (IndexToConnect indexToConnect : indexesToConnect) {
                schemaRuleMigrationAccess.writeSchemaRule(
                        indexToConnect.prototype.materialise(schemaRuleMigrationAccess.nextId()));
            }
        }
        return skippedSchemaRules;
    }

    // Should we skip this index in migration due to it being filtered in some way
    private static boolean shouldSkipSinceFiltered(
            ReadBehaviour readBehaviour, TokenHolders tokenHolders, SchemaDescriptor schemaDescriptor) {
        String[] entityTokenNames =
                tokenHolders.entityTokensGetNames(schemaDescriptor.entityType(), schemaDescriptor.getEntityTokenIds());
        switch (schemaDescriptor.schemaPatternMatchingType()) {
            case COMPLETE_ALL_TOKENS -> {
                switch (schemaDescriptor.entityType()) {
                    case NODE -> {
                        for (int propertyTokenId : schemaDescriptor.getPropertyIds()) {
                            var propertyKeyName = tokenHolders.propertyKeyGetName(propertyTokenId);
                            if (readBehaviour.shouldIncludeNodeProperty(propertyKeyName, entityTokenNames, true)
                                    == PropertyInclusion.EXCLUDE) {
                                return true;
                            }
                        }
                        if (readBehaviour.filterLabels(entityTokenNames).length != entityTokenNames.length) {
                            return true;
                        }
                    }
                    case RELATIONSHIP -> {
                        for (String entityTokenName : entityTokenNames) {
                            for (int propertyTokenId : schemaDescriptor.getPropertyIds()) {
                                var propertyKeyName = tokenHolders.propertyKeyGetName(propertyTokenId);
                                if (readBehaviour.shouldIncludeRelationshipProperty(propertyKeyName, entityTokenName)
                                        == PropertyInclusion.EXCLUDE) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            case PARTIAL_ANY_TOKEN -> {
                switch (schemaDescriptor.entityType()) {
                    case NODE -> {
                        for (int propertyTokenId : schemaDescriptor.getPropertyIds()) {
                            var propertyKeyName = tokenHolders.propertyKeyGetName(propertyTokenId);
                            // Something should be included, do not skip!
                            if (readBehaviour.shouldIncludeNodeProperty(propertyKeyName, entityTokenNames, false)
                                    == PropertyInclusion.INCLUDE) {
                                return false;
                            }
                        }
                        if (readBehaviour.filterLabels(entityTokenNames).length == 0) {
                            // We have filtered everything out and we should skip this index
                            return true;
                        }
                        // All has been filtered out we should skip
                        return true;
                    }
                    case RELATIONSHIP -> {
                        for (String entityTokenName : entityTokenNames) {
                            for (int propertyTokenId : schemaDescriptor.getPropertyIds()) {
                                var propertyKeyName = tokenHolders.propertyKeyGetName(propertyTokenId);
                                if (readBehaviour.shouldIncludeRelationshipProperty(propertyKeyName, entityTokenName)
                                        == PropertyInclusion.INCLUDE) {
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
            case ENTITY_TOKENS -> {
                // Are not copied
                throw new IllegalArgumentException(
                        schemaDescriptor.schemaPatternMatchingType().name());
            }
        }
        return false;
    }

    private static List<SchemaRule> getSrcSchemaRules(
            StorageEngineFactory fromStorage,
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            DatabaseLayout from,
            CursorContextFactory contextFactory,
            boolean from44store,
            LogTailMetadata fromTailMetadata,
            boolean forceBtreeIndexesToRange,
            TokenHolders srcTokenHolders) {
        if (from44store) {
            List<SchemaRule44> schemaRule44s = fromStorage.load44SchemaRules(
                    fs, pageCache, pageCacheTracer, config, from, contextFactory, fromTailMetadata);

            SchemaStore44MigrationUtil.SchemaInfo44 schemaInfo44 =
                    SchemaStore44MigrationUtil.extractRuleInfo(true, schemaRule44s);

            // Throw on any non-converted rules if we haven't been asked to do a force migration
            SchemaStore44MigrationUtil.assertCanMigrate(
                    forceBtreeIndexesToRange,
                    schemaInfo44.nonReplacedIndexes(),
                    schemaInfo44.nonReplacedConstraints(),
                    srcTokenHolders);

            // Any non-converted left here should be converted and created
            // The actual ids here don't matter, we just need to use some unique ids to be able to connect indexes and
            // constraints.
            // Real ids will be allocated when writing the rules to the destination.
            AtomicLong highestExistingId = new AtomicLong(getHighestExistingId(schemaInfo44));
            for (SchemaRule44.Index index : schemaInfo44.nonReplacedIndexes()) {
                IndexDescriptor rangeIndex = asRangeIndex(index, highestExistingId::incrementAndGet);
                schemaInfo44.toCreate().add(rangeIndex);
            }
            for (Pair<SchemaRule44.Constraint, SchemaRule44.Index> constraintPair :
                    schemaInfo44.nonReplacedConstraints()) {
                var oldConstraint = constraintPair.first();
                var oldIndex = constraintPair.other();
                var rangeIndex = asRangeIndex(oldIndex, highestExistingId::incrementAndGet);
                var rangeBackedConstraint = asRangeBackedConstraint(
                        oldConstraint, rangeIndex, highestExistingId::incrementAndGet, srcTokenHolders);
                rangeIndex = rangeIndex.withOwningConstraintId(rangeBackedConstraint.getId());
                schemaInfo44.toCreate().add(rangeIndex);
                schemaInfo44.toCreate().add(rangeBackedConstraint);
            }
            return schemaInfo44.toCreate();
        }
        return fromStorage.loadSchemaRules(
                fs, pageCache, pageCacheTracer, config, from, true, Function.identity(), contextFactory);
    }

    private static long getHighestExistingId(SchemaStore44MigrationUtil.SchemaInfo44 schemaInfo44) {
        long highestExistingId = NO_TOKEN;
        for (SchemaRule schemaRule : schemaInfo44.toCreate()) {
            long id = schemaRule.getId();
            if (id > highestExistingId) {
                highestExistingId = id;
            }
        }
        return highestExistingId;
    }

    record IndexToConnect(long oldId, OptionalLong oldConstraintId, IndexPrototype prototype) {}

    record ConstraintToConnect(long oldId, long indexId, ConstraintDescriptor prototype) {}

    /**
     * Only to be used for expected types - no token indexes
     * Creates any tokens that are missing.
     */
    private static SchemaDescriptor translateToNewSchema(
            SchemaDescriptor schema, TokenRead tokenRead, TokenHolders dstTokenHolders) throws KernelException {
        int[] propertyIds = schema.getPropertyIds();
        int[] newPropertyIds = new int[propertyIds.length];
        for (int i = 0; i < propertyIds.length; i++) {
            newPropertyIds[i] =
                    dstTokenHolders.propertyKeyTokens().getOrCreateId(tokenRead.propertyKeyName(propertyIds[i]));
        }
        boolean forNodes = EntityType.NODE.equals(schema.entityType());

        // Fulltext is special and can have multiple entityTokens
        if (schema.isSchemaDescriptorType(FulltextSchemaDescriptor.class)) {
            int[] entityTokenIds = schema.getEntityTokenIds();
            int[] newEntityTokenIds = new int[entityTokenIds.length];
            for (int i = 0; i < entityTokenIds.length; i++) {
                newEntityTokenIds[i] = forNodes
                        ? dstTokenHolders.labelTokens().getOrCreateId(tokenRead.nodeLabelName(entityTokenIds[i]))
                        : dstTokenHolders
                                .relationshipTypeTokens()
                                .getOrCreateId(tokenRead.relationshipTypeName(entityTokenIds[i]));
            }
            return SchemaDescriptors.fulltext(schema.entityType(), newEntityTokenIds, newPropertyIds);
        }

        if (forNodes) {
            return SchemaDescriptors.forLabel(
                    dstTokenHolders.labelTokens().getOrCreateId(tokenRead.nodeLabelName(schema.getLabelId())),
                    newPropertyIds);
        }
        return SchemaDescriptors.forRelType(
                dstTokenHolders
                        .relationshipTypeTokens()
                        .getOrCreateId(tokenRead.relationshipTypeName(schema.getRelTypeId())),
                newPropertyIds);
    }
}
