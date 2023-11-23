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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;

import java.util.List;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;

public class TokenIndexProviderTest extends IndexProviderTests {
    private static final ProviderFactory factory =
            (pageCache,
                    fs,
                    dir,
                    monitors,
                    collector,
                    readOnlyChecker,
                    databaseLayout,
                    contextFactory,
                    pageCacheTracer) -> {
                DatabaseIndexContext context = DatabaseIndexContext.builder(
                                pageCache, fs, contextFactory, pageCacheTracer, databaseLayout.getDatabaseName())
                        .withMonitors(monitors)
                        .withReadOnlyChecker(readOnlyChecker)
                        .build();
                return new TokenIndexProvider(context, dir, collector, databaseLayout);
            };

    TokenIndexProviderTest() {
        super(factory);
    }

    @Override
    void setupIndexFolders(FileSystemAbstraction fs) {
        // Token indexes doesn't use a separate folder.
    }

    @Override
    IndexDescriptor descriptor() {
        return completeConfiguration(forSchema(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .withName("labelIndex")
                .materialise(indexId));
    }

    @Override
    IndexDescriptor otherDescriptor() {
        return completeConfiguration(forSchema(ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR)
                .withName("relTypeIndex")
                .materialise(indexId + 1));
    }

    @Override
    IndexPrototype validPrototype() {
        return forSchema(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, TokenIndexProvider.DESCRIPTOR)
                .withIndexType(IndexType.LOOKUP)
                .withName("index");
    }

    @Override
    List<IndexPrototype> invalidPrototypes() {
        return List.of(
                forSchema(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, TokenIndexProvider.DESCRIPTOR)
                        .withIndexType(IndexType.RANGE)
                        .withName("unsupported"),
                forSchema(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, RangeIndexProvider.DESCRIPTOR)
                        .withIndexType(IndexType.LOOKUP)
                        .withName("unsupported"),
                forSchema(forLabel(labelId, propId), TokenIndexProvider.DESCRIPTOR)
                        .withIndexType(IndexType.LOOKUP)
                        .withName("unsupported"),
                uniqueForSchema(forLabel(labelId, propId), TokenIndexProvider.DESCRIPTOR)
                        .withIndexType(IndexType.LOOKUP)
                        .withName("unsupported"));
    }
}
