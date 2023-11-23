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

import static org.neo4j.graphdb.schema.IndexSettingUtil.toIndexConfigFromIndexSettingObjectMap;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.fulltext;
import static org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.directoryFactory;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexProvider;
import org.neo4j.kernel.api.impl.schema.vector.VectorSimilarityFunction;

class VectorIndexProviderTest {
    abstract static class VectorIndexProviderTestBase extends IndexProviderTests {
        private static final ProviderFactory FACTORY =
                (pageCache,
                        fs,
                        dir,
                        monitors,
                        collector,
                        readOnlyChecker,
                        databaseLayout,
                        contextFactory,
                        pageCacheTracer) -> new VectorIndexProvider(
                        fs, directoryFactory(fs), dir, monitors, Config.defaults(), readOnlyChecker);

        private final IndexConfig validIndexConfig;

        VectorIndexProviderTestBase(Map<IndexSetting, Object> validIndexSettings) {
            super(FACTORY);
            this.validIndexConfig = indexConfigOf(validIndexSettings);
        }

        @Override
        IndexDescriptor descriptor() {
            return completeConfiguration(forSchema(forLabel(labelId, propId), PROVIDER_DESCRIPTOR)
                    .withIndexType(IndexType.VECTOR)
                    .withIndexConfig(validIndexConfig)
                    .withName("index")
                    .materialise(indexId));
        }

        @Override
        IndexDescriptor otherDescriptor() {
            return completeConfiguration(forSchema(forLabel(labelId, propId), PROVIDER_DESCRIPTOR)
                    .withIndexType(IndexType.VECTOR)
                    .withIndexConfig(validIndexConfig)
                    .withName("otherIndex")
                    .materialise(indexId + 1));
        }

        @Override
        IndexPrototype validPrototype() {
            return forSchema(forLabel(labelId, propId), PROVIDER_DESCRIPTOR)
                    .withIndexType(IndexType.VECTOR)
                    .withIndexConfig(validIndexConfig)
                    .withName("index");
        }

        @Override
        List<IndexPrototype> invalidPrototypes() {
            return List.of(
                    // Bad configurations
                    // no config
                    forSchema(forLabel(labelId, propId))
                            .withIndexType(IndexType.VECTOR)
                            .withName("unsupported"),

                    // invalid dimension
                    forSchema(forLabel(labelId, propId))
                            .withIndexType(IndexType.VECTOR)
                            .withIndexConfig(indexConfigOf(Map.of(
                                    IndexSetting.vector_Dimensions(),
                                    -1,
                                    IndexSetting.vector_Similarity_Function(),
                                    VectorSimilarityFunction.EUCLIDEAN.name())))
                            .withName("unsupported"),

                    // unsupported similarity function
                    forSchema(forLabel(labelId, propId))
                            .withIndexType(IndexType.VECTOR)
                            .withIndexConfig(indexConfigOf(Map.of(
                                    IndexSetting.vector_Dimensions(),
                                    123,
                                    IndexSetting.vector_Similarity_Function(),
                                    "malmo")))
                            .withName("unsupported"),

                    // Unsupported index types
                    forSchema(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                            .withName("unsupported"),
                    forSchema(fulltext(EntityType.NODE, new int[] {labelId}, new int[] {propId}))
                            .withName("unsupported"),
                    forSchema(forLabel(labelId, propId))
                            .withIndexType(IndexType.POINT)
                            .withName("unsupported"),
                    forSchema(forLabel(labelId, propId))
                            .withIndexType(IndexType.TEXT)
                            .withName("unsupported"),
                    forSchema(forLabel(labelId, propId), PROVIDER_DESCRIPTOR)
                            .withIndexType(IndexType.LOOKUP)
                            .withName("unsupported"));
        }

        @Override
        void setupIndexFolders(FileSystemAbstraction fs) throws IOException {
            fs.mkdirs(newProvider().directoryStructure().rootDirectory());
        }

        private static IndexConfig indexConfigOf(Map<IndexSetting, Object> indexSettings) {
            return toIndexConfigFromIndexSettingObjectMap(indexSettings);
        }
    }

    @Nested
    class Euclidean extends VectorIndexProviderTestBase {
        Euclidean() {
            super(Map.of(
                    IndexSetting.vector_Dimensions(),
                    123,
                    IndexSetting.vector_Similarity_Function(),
                    VectorSimilarityFunction.EUCLIDEAN.name()));
        }
    }

    @Nested
    class Cosine extends VectorIndexProviderTestBase {
        Cosine() {
            super(Map.of(
                    IndexSetting.vector_Dimensions(),
                    123,
                    IndexSetting.vector_Similarity_Function(),
                    VectorSimilarityFunction.COSINE.name()));
        }
    }
}
