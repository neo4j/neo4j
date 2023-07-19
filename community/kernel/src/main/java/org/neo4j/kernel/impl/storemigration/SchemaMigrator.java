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

import java.io.IOException;
import java.util.OptionalLong;
import java.util.function.Function;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.newapi.ReadOnlyTokenRead;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccessExtended;
import org.neo4j.token.TokenHolders;

public class SchemaMigrator {

    private SchemaMigrator() {
    }

    public static void migrateSchemaRules(
            StorageEngineFactory fromStorage,
            StorageEngineFactory toStorage,
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            DatabaseLayout from,
            DatabaseLayout toLayout,
            CursorContextFactory contextFactory)
            throws IOException, KernelException {
        var tokenHolders =
                fromStorage.loadReadOnlyTokens(fs, from, config, pageCache, pageCacheTracer, true, contextFactory);

        try (SchemaRuleMigrationAccessExtended schemaRuleMigrationAccess = toStorage.schemaRuleMigrationAccess(
                fs, pageCache, pageCacheTracer, config, toLayout, contextFactory, EmptyMemoryTracker.INSTANCE)) {
            TokenRead tokenRead = new ReadOnlyTokenRead(tokenHolders);

            // Write the rules to the new store.
            //  - Translating the tokens since their ids might be different
            //  - Keeping the schema ids (almost - +1 because block format uses id 0 but not record)
            //    to not have to keep track of connections between constraints and
            //    their indexes - the writing of the rule should have updated that
            //    id's used status
            for (var schemaRule : fromStorage.loadSchemaRules(
                    fs, pageCache, pageCacheTracer, config, from, true, Function.identity(), contextFactory)) {
                if (schemaRule instanceof IndexDescriptor indexDescriptor) {
                    if (indexDescriptor.isTokenIndex()) {
                        // Skip since they have already been created by the copy operation
                        continue;
                    }
                    SchemaDescriptor schema = translateToNewSchema(
                            indexDescriptor.schema(), tokenRead, schemaRuleMigrationAccess.tokenHolders());

                    IndexPrototype newPrototype = indexDescriptor.isUnique()
                            ? IndexPrototype.uniqueForSchema(schema, indexDescriptor.getIndexProvider())
                            : IndexPrototype.forSchema(schema, indexDescriptor.getIndexProvider());
                    IndexDescriptor newDescriptor = newPrototype
                            .withName(indexDescriptor.getName())
                            .withIndexType(indexDescriptor.getIndexType())
                            .withIndexConfig(indexDescriptor.getIndexConfig())
                            .materialise(indexDescriptor.getId() + 1);
                    OptionalLong owningConstraintId = indexDescriptor.getOwningConstraintId();
                    if (owningConstraintId.isPresent()) {
                        newDescriptor = newDescriptor.withOwningConstraintId(owningConstraintId.getAsLong() + 1);
                    }

                    schemaRuleMigrationAccess.writeSchemaRule(newDescriptor);
                } else if (schemaRule instanceof ConstraintDescriptor constraintDescriptor) {
                    SchemaDescriptor schema = translateToNewSchema(
                            constraintDescriptor.schema(), tokenRead, schemaRuleMigrationAccess.tokenHolders());
                    ConstraintDescriptor descriptor =
                            switch (constraintDescriptor.type()) {
                                case UNIQUE -> {
                                    IndexBackedConstraintDescriptor indexBacked =
                                            constraintDescriptor.asIndexBackedConstraint();
                                    yield ConstraintDescriptorFactory.uniqueForSchema(schema, indexBacked.indexType())
                                            .withOwnedIndexId(indexBacked.ownedIndexId() + 1);
                                }
                                case EXISTS -> ConstraintDescriptorFactory.existsForSchema(schema);
                                case UNIQUE_EXISTS -> {
                                    IndexBackedConstraintDescriptor indexBacked =
                                            constraintDescriptor.asIndexBackedConstraint();
                                    yield ConstraintDescriptorFactory.keyForSchema(schema, indexBacked.indexType())
                                            .withOwnedIndexId(indexBacked.ownedIndexId() + 1);
                                }
                                case PROPERTY_TYPE -> ConstraintDescriptorFactory.typeForSchema(
                                        schema,
                                        constraintDescriptor
                                                .asPropertyTypeConstraint()
                                                .propertyType());
                            };
                    descriptor = descriptor
                            .withId(constraintDescriptor.getId() + 1)
                            .withName(constraintDescriptor.getName());
                    schemaRuleMigrationAccess.writeSchemaRule(descriptor);
                }
            }
        }
    }

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
        if (schema.isFulltextSchemaDescriptor()) {
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
