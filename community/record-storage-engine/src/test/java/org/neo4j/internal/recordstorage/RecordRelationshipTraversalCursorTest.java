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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.ArrayUtil.concatArrays;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordNodeCursor.relationshipsReferenceWithDenseMarker;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.ReadTracer;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
public class RecordRelationshipTraversalCursorTest {
    protected static final long NULL = Record.NULL_REFERENCE.longValue();
    protected static final long FIRST_OWNING_NODE = 1;
    protected static final long SECOND_OWNING_NODE = 2;
    protected static final int TYPE1 = 0;
    protected static final int TYPE2 = 1;
    protected static final int TYPE3 = 2;
    protected static final int TYPE4 = 3;
    protected static final int TYPE5 = 4;
    protected static final int TYPE6 = 5;

    @Inject
    protected PageCache pageCache;

    @Inject
    protected FileSystemAbstraction fs;

    @Inject
    protected RecordDatabaseLayout databaseLayout;

    protected NeoStores neoStores;
    private CachedStoreCursors storeCursors;

    private static Stream<Arguments> parameters() {
        return Stream.of(
                of(LOOP, false),
                of(LOOP, true),
                of(OUTGOING, false),
                of(OUTGOING, true),
                of(INCOMING, false),
                of(INCOMING, true));
    }

    private static Stream<Arguments> density() {
        return Stream.of(of(false), of(true));
    }

    @BeforeEach
    void setupStores() {
        var pageCacheTracer = PageCacheTracer.NULL;
        DefaultIdGeneratorFactory idGeneratorFactory =
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName());
        StoreFactory storeFactory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                fs,
                getRecordFormats(),
                NullLogProvider.getInstance(),
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                Sets.immutable.empty());
        neoStores = storeFactory.openAllNeoStores();
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
    }

    protected RecordFormats getRecordFormats() {
        return defaultFormat();
    }

    @AfterEach
    void shutDownStores() {
        storeCursors.close();
        neoStores.close();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void retrieveUsedRelationshipChain(RelationshipDirection direction, boolean dense) {
        long reference = createRelationshipStructure(dense, homogenousRelationships(4, TYPE1, direction));
        try (RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor()) {
            cursor.init(FIRST_OWNING_NODE, reference, ALL_RELATIONSHIPS);
            assertRelationships(cursor, 4, Direction.BOTH, TYPE1);
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void retrieveRelationshipChainWithUnusedLink(RelationshipDirection direction, boolean dense) {
        neoStores.getRelationshipStore().setHighId(10);
        long reference = createRelationshipStructure(dense, homogenousRelationships(4, TYPE1, direction));
        unUseRecord(2);
        int[] expectedRelationshipIds = new int[] {0, 1, 3};
        int relationshipIndex = 0;
        try (RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor()) {
            cursor.init(FIRST_OWNING_NODE, reference, ALL_RELATIONSHIPS);
            while (cursor.next()) {
                assertThat(cursor.entityReference()).isEqualTo(expectedRelationshipIds[relationshipIndex++]);
            }
            assertThat(cursor.entityReference()).isEqualTo(LongReference.NULL);
        }
    }

    @Test
    void shouldHandleDenseNodeWithNoRelationships() {
        // This can actually happen, since we upgrade sparse node --> dense node when creating relationships,
        // but we don't downgrade dense --> sparse when we delete relationships. So if we have a dense node
        // which no longer has relationships, there was this assumption that we could just call getRecord
        // on the NodeRecord#getNextRel() value. Although that value could actually be -1
        try (RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor()) {
            // WHEN
            cursor.init(FIRST_OWNING_NODE, NO_NEXT_RELATIONSHIP.intValue(), ALL_RELATIONSHIPS);

            // THEN
            assertThat(cursor.next()).isFalse();
            assertThat(cursor.entityReference()).isEqualTo(LongReference.NULL);
        }
    }

    @ParameterizedTest
    @MethodSource("density")
    void shouldSelectRelationshipsOfCertainDirection(boolean dense) {
        // given
        long reference = createRelationshipStructure(
                dense,
                concatArrays(
                        homogenousRelationships(4, TYPE1, OUTGOING),
                        homogenousRelationships(3, TYPE1, INCOMING),
                        homogenousRelationships(1, TYPE1, LOOP)));

        try (RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor()) {
            // outgoing
            cursor.init(FIRST_OWNING_NODE, reference, selection(Direction.OUTGOING));
            assertRelationships(cursor, 5, Direction.OUTGOING, TYPE1);

            // incoming
            cursor.init(FIRST_OWNING_NODE, reference, selection(Direction.INCOMING));
            assertRelationships(cursor, 4, Direction.INCOMING, TYPE1);

            // incoming
            cursor.init(FIRST_OWNING_NODE, reference, selection(Direction.BOTH));
            assertRelationships(cursor, 8, Direction.BOTH, TYPE1);
        }
    }

    @ParameterizedTest
    @MethodSource("density")
    void shouldSelectRelationshipsOfCertainTypeAndDirection(boolean dense) {
        // given
        long reference = createRelationshipStructure(
                dense,
                concatArrays(
                        homogenousRelationships(4, TYPE1, OUTGOING),
                        homogenousRelationships(3, TYPE1, INCOMING),
                        homogenousRelationships(1, TYPE1, LOOP),
                        homogenousRelationships(2, TYPE2, OUTGOING),
                        homogenousRelationships(5, TYPE2, INCOMING),
                        homogenousRelationships(6, TYPE2, LOOP)));

        try (RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor()) {
            // === TYPE1 ===
            cursor.init(FIRST_OWNING_NODE, reference, selection(TYPE1, Direction.OUTGOING));
            assertRelationships(cursor, 5, Direction.OUTGOING, TYPE1);
            cursor.init(FIRST_OWNING_NODE, reference, selection(TYPE1, Direction.INCOMING));
            assertRelationships(cursor, 4, Direction.INCOMING, TYPE1);
            cursor.init(FIRST_OWNING_NODE, reference, selection(TYPE1, Direction.BOTH));
            assertRelationships(cursor, 8, Direction.BOTH, TYPE1);

            // === TYPE2 ===
            cursor.init(FIRST_OWNING_NODE, reference, selection(TYPE2, Direction.OUTGOING));
            assertRelationships(cursor, 8, Direction.OUTGOING, TYPE2);
            cursor.init(FIRST_OWNING_NODE, reference, selection(TYPE2, Direction.INCOMING));
            assertRelationships(cursor, 11, Direction.INCOMING, TYPE2);
            cursor.init(FIRST_OWNING_NODE, reference, selection(TYPE2, Direction.BOTH));
            assertRelationships(cursor, 13, Direction.BOTH, TYPE2);
        }
    }

    @ParameterizedTest
    @MethodSource("density")
    void shouldSelectRelationshipsOfCertainTypesAndDirection(boolean dense) {
        // given
        long reference = createRelationshipStructure(
                dense,
                concatArrays(
                        homogenousRelationships(4, TYPE1, OUTGOING),
                        homogenousRelationships(1, TYPE1, LOOP),
                        homogenousRelationships(5, TYPE2, INCOMING),
                        homogenousRelationships(6, TYPE2, LOOP),
                        homogenousRelationships(2, TYPE3, OUTGOING),
                        homogenousRelationships(3, TYPE3, INCOMING)));

        try (RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor()) {
            // === TYPE1+TYPE3 ===
            {
                int[] types = {TYPE1, TYPE3};
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.OUTGOING));
                assertRelationships(cursor, 7, Direction.OUTGOING, types);
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.INCOMING));
                assertRelationships(cursor, 4, Direction.INCOMING, types);
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.BOTH));
                assertRelationships(cursor, 10, Direction.BOTH, types);
            }

            // === TYPE1+TYPE2 ===
            {
                int[] types = {TYPE1, TYPE2};
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.OUTGOING));
                assertRelationships(cursor, 11, Direction.OUTGOING, types);
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.INCOMING));
                assertRelationships(cursor, 12, Direction.INCOMING, types);
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.BOTH));
                assertRelationships(cursor, 16, Direction.BOTH, types);
            }
        }
    }

    @Test
    void shouldStopLookingForGroupsWhenPastEndOfSelection() {
        // given
        long reference = createRelationshipStructure(
                true,
                concatArrays(
                        homogenousRelationships(4, TYPE1, OUTGOING),
                        homogenousRelationships(1, TYPE1, LOOP),
                        homogenousRelationships(2, TYPE3, OUTGOING),
                        homogenousRelationships(3, TYPE3, INCOMING),
                        homogenousRelationships(3, TYPE4, INCOMING),
                        homogenousRelationships(1, TYPE4, OUTGOING),
                        homogenousRelationships(1, TYPE5, LOOP),
                        homogenousRelationships(2, TYPE6, OUTGOING)));

        try (RecordRelationshipTraversalCursor cursor = getNodeRelationshipCursor()) {
            AtomicInteger dbHits = new AtomicInteger();
            ReadTracer readTracer = createDbHitReadTracer(dbHits);
            cursor.setTracer(readTracer);
            // === TYPE1+TYPE3 ===
            {
                int[] types = {TYPE1, TYPE3};
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.OUTGOING));
                assertRelationships(cursor, 7, Direction.OUTGOING, types);
                // Should have visited all groups up to 3 and the one after - 1, 3 and 4 = 3
                assertNbrGroupsVisited(dbHits, 3);
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.INCOMING));
                assertRelationships(cursor, 4, Direction.INCOMING, types);
                assertNbrGroupsVisited(dbHits, 3);
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.BOTH));
                assertRelationships(cursor, 10, Direction.BOTH, types);
                assertNbrGroupsVisited(dbHits, 3);
            }

            // === TYPE1+TYPE4 ===
            {
                int[] types = {TYPE1, TYPE4};
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.OUTGOING));
                assertRelationships(cursor, 6, Direction.OUTGOING, types);
                // Should have visited all groups up to 4 and the one after - 1, 3, 4 and 5 = 4
                assertNbrGroupsVisited(dbHits, 4);
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.INCOMING));
                assertRelationships(cursor, 4, Direction.INCOMING, types);
                assertNbrGroupsVisited(dbHits, 4);
                cursor.init(FIRST_OWNING_NODE, reference, selection(types, Direction.BOTH));
                assertRelationships(cursor, 9, Direction.BOTH, types);
                assertNbrGroupsVisited(dbHits, 4);
            }
        }
    }

    @Test
    void shouldHaveCorrectEntityReferenceAfterLastDeleted() {
        // given
        var count = 4;
        var reference = createRelationshipStructure(false, homogenousRelationships(count, 1, OUTGOING));
        try (var cursor = getNodeRelationshipCursor()) {
            cursor.init(FIRST_OWNING_NODE, reference, ALL_RELATIONSHIPS);
            long lastId = -1;
            while (cursor.next()) {
                lastId = cursor.entityReference();
            }
            // delete the last one in the chain
            unUseRecord(lastId);
        }

        // when
        try (var cursor = getNodeRelationshipCursor()) {
            cursor.init(FIRST_OWNING_NODE, reference, ALL_RELATIONSHIPS);
            int countAfterDeletion = 0;
            while (cursor.next()) {
                countAfterDeletion++;
            }
            assertThat(countAfterDeletion).isEqualTo(count - 1);
            assertThat(cursor.entityReference()).isEqualTo(LongReference.NULL);
        }
    }

    private static void assertRelationships(
            RecordRelationshipTraversalCursor cursor, int count, Direction direction, int... types) {
        var expectedTypes = IntStream.of(types).boxed().collect(Collectors.toSet());
        int found = 0;
        while (cursor.next()) {
            found++;
            assertThat(cursor.type()).isIn(expectedTypes);
            switch (direction) {
                case OUTGOING -> assertThat(cursor.sourceNodeReference()).isEqualTo(FIRST_OWNING_NODE);
                case INCOMING -> assertThat(cursor.targetNodeReference()).isEqualTo(FIRST_OWNING_NODE);
                case BOTH -> assertThat(FIRST_OWNING_NODE == cursor.sourceNodeReference()
                                || FIRST_OWNING_NODE == cursor.targetNodeReference())
                        .isTrue();
            }
        }
        assertThat(found).isEqualTo(count);
        assertThat(cursor.entityReference()).isEqualTo(LongReference.NULL);
    }

    private static void assertNbrGroupsVisited(AtomicInteger dbHits, int nbrExpectedGroupsVisited) {
        assertThat(dbHits.get()).isEqualTo(nbrExpectedGroupsVisited);
        dbHits.set(0);
    }

    protected void unUseRecord(long recordId) {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        var readCursor = storeCursors.readCursor(RELATIONSHIP_CURSOR);
        RelationshipRecord relationshipRecord = relationshipStore.getRecordByCursor(
                recordId, new RelationshipRecord(-1), RecordLoad.FORCE, readCursor, EmptyMemoryTracker.INSTANCE);
        relationshipRecord.setInUse(false);
        try (var writeCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            relationshipStore.updateRecord(relationshipRecord, writeCursor, NULL_CONTEXT, storeCursors);
        }
    }

    protected static RelationshipGroupRecord createRelationshipGroup(long id, int type, long[] firstIds, long next) {
        return new RelationshipGroupRecord(id)
                .initialize(true, type, firstIds[0], firstIds[1], firstIds[2], FIRST_OWNING_NODE, next);
    }

    protected long createRelationshipStructure(boolean dense, RelationshipSpec... relationshipSpecs) {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        if (!dense) {
            // a single chain
            try (var cursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
                for (int i = 0; i < relationshipSpecs.length; i++) {
                    long nextRelationshipId = i == relationshipSpecs.length - 1 ? NULL : i + 1;
                    relationshipStore.updateRecord(
                            createRelationship(i, nextRelationshipId, relationshipSpecs[i]),
                            cursor,
                            NULL_CONTEXT,
                            storeCursors);
                }
            }
            return 0;
        } else {
            // split chains on type/direction
            Arrays.sort(relationshipSpecs);
            RelationshipGroupStore relationshipGroupStore = neoStores.getRelationshipGroupStore();
            int currentType = -1;
            long[] currentGroup = null;
            long nextGroupId = relationshipGroupStore.getNumberOfReservedLowIds();
            try (var groupCursor = storeCursors.writeCursor(GROUP_CURSOR);
                    var relCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
                for (int i = 0; i < relationshipSpecs.length; i++) {
                    RelationshipSpec spec = relationshipSpecs[i];
                    if (spec.type != currentType || currentGroup == null) {
                        if (currentGroup != null) {
                            relationshipGroupStore.updateRecord(
                                    createRelationshipGroup(nextGroupId++, currentType, currentGroup, nextGroupId),
                                    groupCursor,
                                    NULL_CONTEXT,
                                    storeCursors);
                        }
                        currentType = spec.type;
                        currentGroup = new long[] {NULL, NULL, NULL};
                    }

                    int relationshipOrdinal = relationshipSpecs[i].direction.ordinal();
                    long relationshipId = i;
                    long nextRelationshipId =
                            i < relationshipSpecs.length - 1 && relationshipSpecs[i + 1].equals(spec) ? i + 1 : NULL;
                    relationshipStore.updateRecord(
                            createRelationship(relationshipId, nextRelationshipId, relationshipSpecs[i]),
                            relCursor,
                            NULL_CONTEXT,
                            storeCursors);
                    if (currentGroup[relationshipOrdinal] == NULL) {
                        currentGroup[relationshipOrdinal] = relationshipId;
                    }
                }
                relationshipGroupStore.updateRecord(
                        createRelationshipGroup(nextGroupId, currentType, currentGroup, NULL),
                        groupCursor,
                        NULL_CONTEXT,
                        storeCursors);
            }
            return relationshipsReferenceWithDenseMarker(relationshipGroupStore.getNumberOfReservedLowIds(), true);
        }
    }

    protected static RelationshipRecord createRelationship(
            long id, long nextRelationship, RelationshipSpec relationshipSpec) {
        RelationshipRecord relationship = new RelationshipRecord(id);
        relationship.initialize(
                true,
                NO_NEXT_PROPERTY.intValue(),
                getFirstNode(relationshipSpec.direction),
                getSecondNode(relationshipSpec.direction),
                relationshipSpec.type,
                NO_NEXT_RELATIONSHIP.intValue(),
                nextRelationship,
                NO_NEXT_RELATIONSHIP.intValue(),
                nextRelationship,
                false,
                false);
        return relationship;
    }

    protected static long getSecondNode(RelationshipDirection direction) {
        return direction == INCOMING || direction == LOOP ? FIRST_OWNING_NODE : SECOND_OWNING_NODE;
    }

    protected static long getFirstNode(RelationshipDirection direction) {
        return direction == OUTGOING || direction == LOOP ? FIRST_OWNING_NODE : SECOND_OWNING_NODE;
    }

    protected RecordRelationshipTraversalCursor getNodeRelationshipCursor() {
        return new RecordRelationshipTraversalCursor(
                neoStores.getRelationshipStore(),
                neoStores.getRelationshipGroupStore(),
                null,
                NULL_CONTEXT,
                storeCursors,
                EmptyMemoryTracker.INSTANCE);
    }

    protected static RelationshipSpec[] homogenousRelationships(int count, int type, RelationshipDirection direction) {
        RelationshipSpec[] specs = new RelationshipSpec[count];
        Arrays.fill(specs, new RelationshipSpec(type, direction));
        return specs;
    }

    private static ReadTracer createDbHitReadTracer(AtomicInteger dbHits) {
        return new ReadTracer() {

            @Override
            public void onNode(long nodeReference) {}

            @Override
            public void onAllNodesScan() {}

            @Override
            public void onRelationship(long relationshipReference) {}

            @Override
            public void onProperty(int propertyKey) {}

            @Override
            public void onHasLabel(int label) {}

            @Override
            public void onHasLabel() {}

            @Override
            public void dbHit() {
                dbHits.incrementAndGet();
            }
        };
    }

    protected static class RelationshipSpec implements Comparable<RelationshipSpec> {
        final int type;
        final RelationshipDirection direction;

        RelationshipSpec(int type, RelationshipDirection direction) {
            this.type = type;
            this.direction = direction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RelationshipSpec that = (RelationshipSpec) o;
            return type == that.type && direction == that.direction;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, direction);
        }

        @Override
        public int compareTo(RelationshipSpec o) {
            int typeCompare = Integer.compare(type, o.type);
            if (typeCompare != 0) {
                return typeCompare;
            }
            return Integer.compare(direction.ordinal(), o.direction.ordinal());
        }
    }
}
