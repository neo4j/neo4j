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
package org.neo4j.internal.recordstorage;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.dynamicPointer;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.store.ShortArray.LONG;
import static org.neo4j.kernel.impl.store.StoreType.NODE_LABEL;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class NodeCommandTest {
    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private PageCache pageCache;

    @Inject
    private DatabaseLayout databaseLayout;

    private NodeStore nodeStore;
    private final InMemoryClosableChannel channel = new InMemoryClosableChannel();
    private final LogCommandSerialization commandSerialization =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);
    private NeoStores neoStores;
    private DynamicAllocatorProvider allocatorProvider;

    @BeforeEach
    void before() {
        var pageCacheTracer = PageCacheTracer.NULL;
        StoreFactory storeFactory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCache,
                pageCacheTracer,
                fs,
                NullLogProvider.getInstance(),
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        neoStores = storeFactory.openAllNeoStores();
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
        nodeStore = neoStores.getNodeStore();
    }

    @AfterEach
    void after() {
        neoStores.close();
    }

    @Test
    void shouldSerializeAndDeserializeUnusedRecords() throws Exception {
        // Given
        NodeRecord before = new NodeRecord(12);
        NodeRecord after = new NodeRecord(12);
        // When
        assertSerializationWorksFor(new Command.NodeCommand(commandSerialization, before, after));
    }

    @Test
    void shouldSerializeCreatedRecord() throws Exception {
        // Given
        NodeRecord before = new NodeRecord(12);
        NodeRecord after = new NodeRecord(12).initialize(false, 1, false, 2, 0);
        after.setCreated();
        after.setInUse(true);
        // When
        assertSerializationWorksFor(new Command.NodeCommand(commandSerialization, before, after));
    }

    @Test
    void shouldSerializeDenseRecord() throws Exception {
        // Given
        NodeRecord before = new NodeRecord(12).initialize(false, 2, false, 1, 0);
        before.setInUse(true);
        NodeRecord after = new NodeRecord(12).initialize(false, 1, true, 2, 0);
        after.setInUse(true);
        // When
        assertSerializationWorksFor(new Command.NodeCommand(commandSerialization, before, after));
    }

    @Test
    void shouldSerializeUpdatedRecord() throws Exception {
        // Given
        NodeRecord before = new NodeRecord(12).initialize(false, 2, false, 1, 0);
        before.setInUse(true);
        NodeRecord after = new NodeRecord(12).initialize(false, 1, false, 2, 0);
        after.setInUse(true);
        // When
        assertSerializationWorksFor(new Command.NodeCommand(commandSerialization, before, after));
    }

    @Test
    void shouldSerializeInlineLabels() throws Exception {
        // Given
        NodeRecord before = new NodeRecord(12).initialize(false, 2, false, 1, 0);
        before.setInUse(true);
        NodeRecord after = new NodeRecord(12).initialize(false, 1, false, 2, 0);
        after.setInUse(true);
        NodeLabels nodeLabels = parseLabelsField(after);
        nodeLabels.add(
                1337, nodeStore, allocatorProvider.allocator(NODE_LABEL), NULL_CONTEXT, StoreCursors.NULL, INSTANCE);
        // When
        assertSerializationWorksFor(new Command.NodeCommand(commandSerialization, before, after));
    }

    @Test
    void shouldSerializeSecondaryUnitUsage() throws Exception {
        // Given
        // a record that is changed to include a secondary unit
        NodeRecord before = new NodeRecord(13).initialize(false, 2, false, 1, 0);
        before.setInUse(true);
        before.setSecondaryUnitIdOnLoad(
                NO_ID); // this and the previous line set the defaults, they are here for clarity
        NodeRecord after = new NodeRecord(13).initialize(false, 2, false, 1, 0);
        after.setInUse(true);
        after.setSecondaryUnitIdOnCreate(14L);

        Command.NodeCommand command = new Command.NodeCommand(commandSerialization, before, after);

        // Then
        assertSerializationWorksFor(command);
    }

    @Test
    void shouldSerializeDynamicRecordLabels() throws Exception {
        // Given
        NodeRecord before = new NodeRecord(12).initialize(false, 2, false, 1, 0);
        before.setInUse(true);
        NodeRecord after = new NodeRecord(12).initialize(false, 1, false, 2, 0);
        after.setInUse(true);
        NodeLabels nodeLabels = parseLabelsField(after);
        for (int i = 10; i < 100; i++) {
            nodeLabels.add(
                    i, nodeStore, allocatorProvider.allocator(NODE_LABEL), NULL_CONTEXT, StoreCursors.NULL, INSTANCE);
        }
        // When
        assertSerializationWorksFor(new Command.NodeCommand(commandSerialization, before, after));
    }

    @Test
    void shouldSerializeDynamicRecordsRemoved() throws Exception {
        channel.reset();
        // Given
        NodeRecord before = new NodeRecord(12).initialize(false, 2, false, 1, 0);
        before.setInUse(true);
        List<DynamicRecord> beforeDyn =
                singletonList(dynamicRecord(0, true, true, -1L, LONG.intValue(), new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
        before.setLabelField(dynamicPointer(beforeDyn), beforeDyn);
        NodeRecord after = new NodeRecord(12).initialize(false, 1, false, 2, 0);
        after.setInUse(true);
        List<DynamicRecord> dynamicRecords =
                singletonList(dynamicRecord(0, false, true, -1L, LONG.intValue(), new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
        after.setLabelField(dynamicPointer(dynamicRecords), dynamicRecords);
        // When
        Command.NodeCommand cmd = new Command.NodeCommand(commandSerialization, before, after);
        cmd.serialize(channel);
        Command.NodeCommand result = (Command.NodeCommand) commandSerialization.read(channel);
        // Then
        assertThat(result).isEqualTo(cmd);
        assertThat(result.getMode()).isEqualTo(cmd.getMode());
        assertThat(result.getBefore()).isEqualTo(cmd.getBefore());
        assertThat(result.getAfter()).isEqualTo(cmd.getAfter());
        // And dynamic records should be the same
        assertThat(result.getBefore().getDynamicLabelRecords())
                .isEqualTo(cmd.getBefore().getDynamicLabelRecords());
        Collection<DynamicRecord> operand = emptyAndUnused(cmd.getAfter().getDynamicLabelRecords(), LONG.intValue());
        assertThat(result.getAfter().getDynamicLabelRecords()).isEqualTo(operand);
    }

    private static DynamicRecord dynamicRecord(
            long id, boolean inUse, boolean isStartRecord, long nextBlock, int type, byte[] data) {
        DynamicRecord record = new DynamicRecord(id).initialize(inUse, isStartRecord, nextBlock, type);
        record.setData(data);
        return record;
    }

    private void assertSerializationWorksFor(Command.NodeCommand cmd) throws IOException {
        channel.reset();
        cmd.serialize(channel);
        Command.NodeCommand result = (Command.NodeCommand) commandSerialization.read(channel);
        // Then
        assertThat(result).isEqualTo(cmd);
        assertThat(result.getMode()).isEqualTo(cmd.getMode());
        assertThat(result.getBefore()).isEqualTo(cmd.getBefore());
        assertThat(result.getAfter()).isEqualTo(cmd.getAfter());
        // And created and dense flags should be the same
        assertThat(result.getBefore().isCreated()).isEqualTo(cmd.getBefore().isCreated());
        assertThat(result.getAfter().isCreated()).isEqualTo(cmd.getAfter().isCreated());
        assertThat(result.getBefore().isDense()).isEqualTo(cmd.getBefore().isDense());
        assertThat(result.getAfter().isDense()).isEqualTo(cmd.getAfter().isDense());
        // And labels should be the same
        assertThat(labels(result.getBefore())).isEqualTo(labels(cmd.getBefore()));
        assertThat(labels(result.getAfter())).isEqualTo(labels(cmd.getAfter()));
        // And dynamic records should be the same
        assertThat(result.getBefore().getDynamicLabelRecords())
                .isEqualTo(cmd.getBefore().getDynamicLabelRecords());
        assertThat(result.getAfter().getDynamicLabelRecords())
                .isEqualTo(cmd.getAfter().getDynamicLabelRecords());
        // And the secondary unit information should be the same
        // Before
        assertThat(result.getBefore().requiresSecondaryUnit())
                .isEqualTo(cmd.getBefore().requiresSecondaryUnit());
        assertThat(result.getBefore().hasSecondaryUnitId())
                .isEqualTo(cmd.getBefore().hasSecondaryUnitId());
        assertThat(result.getBefore().getSecondaryUnitId())
                .isEqualTo(cmd.getBefore().getSecondaryUnitId());
        // and after
        assertThat(result.getAfter().requiresSecondaryUnit())
                .isEqualTo(cmd.getAfter().requiresSecondaryUnit());
        assertThat(result.getAfter().hasSecondaryUnitId())
                .isEqualTo(cmd.getAfter().hasSecondaryUnitId());
        assertThat(result.getAfter().getSecondaryUnitId())
                .isEqualTo(cmd.getAfter().getSecondaryUnitId());
    }

    private Set<Integer> labels(NodeRecord record) {
        int[] rawLabels = parseLabelsField(record).get(nodeStore, StoreCursors.NULL, EmptyMemoryTracker.INSTANCE);
        Set<Integer> labels = new HashSet<>(rawLabels.length);
        for (int label : rawLabels) {
            labels.add(label);
        }
        return labels;
    }

    private static Collection<DynamicRecord> emptyAndUnused(Collection<DynamicRecord> dynamicLabelRecords, int type) {
        return dynamicLabelRecords.stream()
                .map(record -> {
                    DynamicRecord dynamicRecord = new DynamicRecord(record.getId());
                    dynamicRecord.setType(type);
                    return dynamicRecord;
                })
                .collect(Collectors.toList());
    }
}
