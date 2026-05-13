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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.helpers.progress.ProgressListener.NONE;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexType.TEXT;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_ID_NAME_LOOKUP;
import static org.neo4j.internal.schema.StorageEngineIndexingBehaviour.EMPTY;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.ElementIdMapper.PLACEHOLDER;

import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.text.TextIndexProvider;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
class TextIndexAccessorTest {
    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fs;

    private JobScheduler scheduler;
    private IndexSamplingConfig samplingConfig;
    private TextIndexProvider provider;

    void start(LuceneContext luceneContext) {
        scheduler = new ThreadPoolJobScheduler();
        Config config = Config.defaults();
        samplingConfig = new IndexSamplingConfig(config);
        provider = new TextIndexProvider(
                fs,
                DirectoryFactory.persistent(luceneContext),
                directoriesByProvider(directory.homePath()),
                new Monitors(),
                config,
                writable(),
                NullLogProvider.getInstance());
    }

    @AfterEach
    void stop() {
        scheduler.close();
    }

    @ParameterizedTest
    @EnumSource
    void shouldInsertFrom(LuceneContext luceneContext) throws Exception {
        start(luceneContext);
        // given
        IndexDescriptor mainDescriptor = provider.completeConfiguration(
                forSchema(forLabel(1, 1)).withIndexType(TEXT).withName("index1").materialise(1L), EMPTY);
        IndexDescriptor otherDescriptor = provider.completeConfiguration(
                forSchema(forLabel(1, 1)).withIndexType(TEXT).withName("index2").materialise(2L), EMPTY);
        try (IndexAccessor mainIndex = createEmptyIndex(provider, samplingConfig, mainDescriptor);
                IndexAccessor otherIndex = createEmptyIndex(provider, samplingConfig, otherDescriptor)) {
            insertData(mainIndex, mainDescriptor, 0, 1_000);
            insertData(otherIndex, otherDescriptor, 1_000, 2_000);

            // when
            mainIndex.insertFrom(otherIndex, null, false, null, null, 4, scheduler, NONE);

            // then
            verifyData(mainIndex, 0, 2_000, LongSets.immutable.empty());
        }
    }

    @ParameterizedTest
    @EnumSource
    void shouldInsertFromWithFiltering(LuceneContext luceneContext) throws Exception {
        start(luceneContext);
        // given
        IndexDescriptor mainDescriptor = provider.completeConfiguration(
                forSchema(forLabel(1, 1)).withIndexType(TEXT).withName("index1").materialise(1L), EMPTY);
        IndexDescriptor otherDescriptor = provider.completeConfiguration(
                forSchema(forLabel(1, 1)).withIndexType(TEXT).withName("index2").materialise(2L), EMPTY);
        try (IndexAccessor mainIndex = createEmptyIndex(provider, samplingConfig, mainDescriptor);
                IndexAccessor otherIndex = createEmptyIndex(provider, samplingConfig, otherDescriptor)) {
            insertData(mainIndex, mainDescriptor, 0, 1_000);
            insertData(otherIndex, otherDescriptor, 1_000, 2_000);

            // when
            ImmutableLongSet excluded = LongSets.immutable.with(1010, 1234, 1357);
            mainIndex.insertFrom(otherIndex, null, false, null, id -> !excluded.contains(id), 4, scheduler, NONE);

            // then
            verifyData(mainIndex, 0, 2_000, excluded);
        }
    }

    private static IndexAccessor createEmptyIndex(
            TextIndexProvider provider, IndexSamplingConfig samplingConfig, IndexDescriptor descriptor)
            throws Exception {
        IndexPopulator populator = provider.getPopulator(
                descriptor,
                samplingConfig,
                ByteBufferFactory.heapBufferFactory((int) mebiBytes(8)),
                INSTANCE,
                TOKEN_ID_NAME_LOOKUP,
                PLACEHOLDER,
                Sets.immutable.empty(),
                EMPTY);
        populator.create();
        populator.close(true, NULL_CONTEXT);

        return provider.getOnlineAccessor(
                descriptor, samplingConfig, TOKEN_ID_NAME_LOOKUP, PLACEHOLDER, Sets.immutable.empty(), false, EMPTY);
    }

    private static void insertData(IndexAccessor index, IndexDescriptor descriptor, int from, int to)
            throws IndexEntryConflictException {
        try (IndexUpdater updater = index.newUpdater(ONLINE, NULL_CONTEXT, false)) {
            for (int i = from; i < to; i++) {
                updater.process(EagerValueIndexEntryUpdate.add(i, descriptor, Values.stringValue("string" + i)));
            }
        }
    }

    private static void verifyData(IndexAccessor index, int from, int to, LongSet excluded)
            throws IndexNotApplicableKernelException {
        try (ValueIndexReader reader = index.newValueReader(NO_USAGE_TRACKING);
                SimpleEntityValueClient client = new SimpleEntityValueClient()) {
            for (int i = from; i < to; i++) {
                reader.query(
                        client,
                        QueryContext.NULL_CONTEXT,
                        NULL_CONTEXT,
                        unconstrained(),
                        PropertyIndexQuery.exact(1, Values.stringValue("string" + i)));
                if (!excluded.contains(i)) {
                    assertThat(client.next()).isTrue();
                    assertThat(client.reference).isEqualTo(i);
                }
                assertThat(client.next()).isFalse();
            }
        }
    }
}
