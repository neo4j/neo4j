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
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.fulltext;
import static org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.directoryFactory;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Nested;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexProvider;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

class VectorIndexProviderTest {
    @Nested
    class V1 extends VectorIndexProviderTestBase {
        V1() {
            super(VectorIndexVersion.V1_0);
        }

        private VectorIndexSettings validSettings() {
            return VectorIndexSettings.create()
                    .withDimensions(version.maxDimensions())
                    .withSimilarityFunction(
                            version.supportedSimilarityFunctions().getAny());
        }

        @Override
        IndexPrototype validPrototype() {
            return super.validPrototype().withIndexConfig(validSettings().toIndexConfig());
        }

        @Override
        List<IndexPrototype> invalidPrototypes() {
            return List.of(
                    // Bad configurations
                    //   no config
                    vectorPrototype().withName("unsupported"),

                    //   invalid dimension
                    vectorPrototype()
                            .withIndexConfig(validSettings().withDimensions(-1).toIndexConfig())
                            .withName("unsupported"),

                    //   unsupported similarity function
                    vectorPrototype()
                            .withIndexConfig(validSettings()
                                    .withSimilarityFunction("malmo")
                                    .toIndexConfig())
                            .withName("unsupported"),

                    //   unrecognised vector index setting
                    validPrototype()
                            .withIndexConfig(validSettings()
                                    .set(IndexSetting.fulltext_Analyzer(), "swedish")
                                    .toIndexConfig())
                            .withName("unsupported"),

                    //   unrecognised vector settings for version
                    validPrototype()
                            .withIndexConfig(
                                    validSettings().withQuantization("OFF").toIndexConfig())
                            .withName("unsupported"),
                    validPrototype()
                            .withIndexConfig(validSettings().withHnswM(32).toIndexConfig())
                            .withName("unsupported"),
                    validPrototype()
                            .withIndexConfig(validSettings().withHnswM(256).toIndexConfig())
                            .withName("unsupported"),

                    // Unsupported index types
                    forSchema(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                            .withName("unsupported"),
                    forSchema(fulltext(EntityType.NODE, new int[] {labelId}, new int[] {propId}))
                            .withName("unsupported"),
                    forSchema(schemaDescriptor()).withIndexType(IndexType.POINT).withName("unsupported"),
                    forSchema(schemaDescriptor()).withIndexType(IndexType.TEXT).withName("unsupported"),
                    forSchema(schemaDescriptor(), PROVIDER_DESCRIPTOR)
                            .withIndexType(IndexType.LOOKUP)
                            .withName("unsupported"));
        }
    }

    @Nested
    class V2 extends VectorIndexProviderTestBase {
        V2() {
            super(VectorIndexVersion.V2_0);
        }

        @Override
        List<IndexPrototype> invalidPrototypes() {
            return List.of(
                    // Bad configurations
                    //   invalid dimension
                    vectorPrototype()
                            .withIndexConfig(VectorIndexSettings.create()
                                    .withDimensions(-1)
                                    .toIndexConfig())
                            .withName("unsupported"),

                    //   unsupported similarity function
                    vectorPrototype()
                            .withIndexConfig(VectorIndexSettings.create()
                                    .withSimilarityFunction("malmo")
                                    .toIndexConfig())
                            .withName("unsupported"),

                    //   unrecognised vector index setting
                    validPrototype()
                            .withIndexConfig(VectorIndexSettings.create()
                                    .set(IndexSetting.fulltext_Analyzer(), "swedish")
                                    .toIndexConfig())
                            .withName("unsupported"),

                    // Unsupported index types
                    forSchema(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                            .withName("unsupported"),
                    forSchema(fulltext(EntityType.NODE, new int[] {labelId}, new int[] {propId}))
                            .withName("unsupported"),
                    forSchema(schemaDescriptor()).withIndexType(IndexType.POINT).withName("unsupported"),
                    forSchema(schemaDescriptor()).withIndexType(IndexType.TEXT).withName("unsupported"),
                    forSchema(schemaDescriptor(), PROVIDER_DESCRIPTOR)
                            .withIndexType(IndexType.LOOKUP)
                            .withName("unsupported"));
        }
    }

    abstract static class VectorIndexProviderTestBase extends IndexProviderTests {
        protected final VectorIndexVersion version;

        VectorIndexProviderTestBase(VectorIndexVersion version) {
            super(factory(version));
            this.version = version;
        }

        protected SchemaDescriptor schemaDescriptor() {
            return forLabel(labelId, propId);
        }

        protected IndexPrototype vectorPrototype() {
            return forSchema(schemaDescriptor())
                    .withIndexType(IndexType.VECTOR)
                    .withIndexProvider(version.descriptor());
        }

        @Override
        IndexPrototype validPrototype() {
            return vectorPrototype().withName("valid");
        }

        @Override
        IndexDescriptor descriptor() {
            return completeConfiguration(validPrototype().withName("index").materialise(indexId));
        }

        @Override
        IndexDescriptor otherDescriptor() {
            return completeConfiguration(validPrototype().withName("otherIndex").materialise(indexId + 1));
        }

        @Override
        void setupIndexFolders(FileSystemAbstraction fs) throws IOException {
            fs.mkdirs(newProvider().directoryStructure().rootDirectory());
        }

        private static final AtomicInteger THREAD_POOL_JOB_SCHEDULER_ID = new AtomicInteger();

        private static ProviderFactory factory(VectorIndexVersion version) {
            return (pageCache,
                    fs,
                    dir,
                    monitors,
                    collector,
                    readOnlyChecker,
                    databaseLayout,
                    contextFactory,
                    pageCacheTracer) -> new VectorIndexProvider(
                    version,
                    fs,
                    directoryFactory(fs),
                    dir,
                    monitors,
                    Config.defaults(),
                    readOnlyChecker,
                    new ThreadPoolJobScheduler("%s-%s-%s"
                            .formatted(
                                    VectorIndexProviderTest.class.getSimpleName(),
                                    version,
                                    THREAD_POOL_JOB_SCHEDULER_ID.getAndIncrement())));
        }
    }
}
