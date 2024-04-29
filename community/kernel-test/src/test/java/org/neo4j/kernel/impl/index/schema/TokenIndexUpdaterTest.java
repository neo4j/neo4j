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

import static java.lang.Integer.max;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.io.IOException;
import java.util.Random;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.index.internal.gbptree.GBPTreeVisitor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.EntityRange;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityTokenClient;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith(RandomExtension.class)
@PageCacheExtension
class TokenIndexUpdaterTest {
    private static final int LABEL_COUNT = 5;
    private static final int NODE_COUNT = 10_000;
    private final DefaultTokenIndexIdLayout idLayout = new DefaultTokenIndexIdLayout();

    @Inject
    private RandomSupport random;

    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fileSystem;

    private GBPTree<TokenScanKey, TokenScanValue> tree;

    @BeforeEach
    void openTree() {
        tree = new GBPTreeBuilder<>(pageCache, fileSystem, directory.file("file"), new TokenScanLayout()).build();
    }

    @AfterEach
    void closeTree() throws IOException {
        tree.close();
    }

    @Test
    void addAndSearchSequenceOfNodes() throws Exception {
        int labelId = 2;
        // GIVEN
        try (var updater = new TokenIndexUpdater(max(5, NODE_COUNT / 100), idLayout)) {
            updater.initialize(tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT));

            // WHEN
            for (long i = 0; i < NODE_COUNT; i++) {
                var update = TokenIndexEntryUpdate.change(i, null, EMPTY_INT_ARRAY, new int[] {labelId});
                updater.process(update);
            }
        }

        // THEN
        SimpleEntityTokenClient client = new SimpleEntityTokenClient();
        TokenScanValueIndexProgressor progressor = new TokenScanValueIndexProgressor(
                tree.seek(new TokenScanKey(labelId, 0), new TokenScanKey(labelId, Long.MAX_VALUE), NULL_CONTEXT),
                client,
                IndexOrder.ASCENDING,
                EntityRange.FULL,
                idLayout,
                labelId);
        long expectedNodeId = 0;
        while (progressor.next()) {
            assertThat(client.reference).isEqualTo(expectedNodeId);
            expectedNodeId++;
        }
        assertThat(expectedNodeId).isEqualTo(NODE_COUNT);
    }

    @Test
    void shouldAddAndRemoveLabels() throws Exception {
        // GIVEN
        long[] expected = new long[NODE_COUNT];
        try (TokenIndexUpdater writer = new TokenIndexUpdater(max(5, NODE_COUNT / 100), idLayout)) {
            writer.initialize(tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT));

            // WHEN
            for (int i = 0; i < NODE_COUNT * 3; i++) {
                TokenIndexEntryUpdate<?> update = randomUpdate(expected);
                writer.process(update);
            }
        }

        // THEN
        for (int i = 0; i < LABEL_COUNT; i++) {
            long[] expectedNodeIds = nodesWithLabel(expected, i);
            SimpleEntityTokenClient client = new SimpleEntityTokenClient();
            TokenScanValueIndexProgressor progressor = new TokenScanValueIndexProgressor(
                    tree.seek(new TokenScanKey(i, 0), new TokenScanKey(i, Long.MAX_VALUE), NULL_CONTEXT),
                    client,
                    IndexOrder.ASCENDING,
                    EntityRange.FULL,
                    idLayout,
                    i);
            MutableLongList actualNodeIds = LongLists.mutable.empty();
            while (progressor.next()) {
                actualNodeIds.add(client.reference);
            }
            assertArrayEquals(expectedNodeIds, actualNodeIds.toArray(), "For label " + i);
        }
    }

    @Test
    void shouldTracePageCacheAccess() throws Exception {
        // Given
        int nodeCount = 5;
        var cacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);
        var cursorContext = contextFactory.create("tracePageCacheAccessOnWrite");

        // When
        try (TokenIndexUpdater writer = new TokenIndexUpdater(nodeCount, idLayout)) {
            writer.initialize(tree.writer(W_BATCHED_SINGLE_THREADED, cursorContext));
            for (int i = 0; i < nodeCount; i++) {
                writer.process(TokenIndexEntryUpdate.change(i, null, EMPTY_INT_ARRAY, new int[] {1}));
            }
        }

        // Then
        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isEqualTo(1);
        assertThat(cursorTracer.unpins()).isEqualTo(1);
        assertThat(cursorTracer.hits()).isEqualTo(1);
        assertThat(cursorTracer.faults()).isEqualTo(0);
    }

    @Test
    void shouldNotAcceptUnsortedTokens() {
        // GIVEN
        assertThatThrownBy(() -> {
                    try (TokenIndexUpdater writer = new TokenIndexUpdater(1, idLayout)) {
                        writer.initialize(tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT));

                        // WHEN
                        writer.process(TokenIndexEntryUpdate.change(0, null, EMPTY_INT_ARRAY, new int[] {2, 1}));
                    }
                })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsorted");
    }

    @Test
    void shouldNotAcceptInvalidTokens() {
        // GIVEN
        assertThatThrownBy(() -> {
                    try (TokenIndexUpdater writer = new TokenIndexUpdater(1, idLayout)) {
                        writer.initialize(tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT));

                        // WHEN
                        writer.process(TokenIndexEntryUpdate.change(0, null, EMPTY_INT_ARRAY, new int[] {2, -1}));
                    }
                })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected non-negative int value");
    }

    @Test
    void shouldRemoveEmptyTreeEntries() throws Exception {
        // given
        int numberOfTreeEntries = 3;
        int numberOfNodesInEach = 5;
        int labelId = 1;
        int[] labels = {labelId};
        var idLayout = this.idLayout;
        try (TokenIndexUpdater writer = new TokenIndexUpdater(max(5, NODE_COUNT / 100), idLayout)) {
            writer.initialize(tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT));

            // a couple of tree entries with a couple of nodes each
            // concept art: [xxxx          ][xxxx          ][xxxx          ] where x is used node.
            for (int i = 0; i < numberOfTreeEntries; i++) {
                long baseNodeId = idLayout.firstIdOfRange(i);
                for (int j = 0; j < numberOfNodesInEach; j++) {
                    writer.process(TokenIndexEntryUpdate.change(baseNodeId + j, null, EMPTY_INT_ARRAY, labels));
                }
            }
        }
        assertTreeHasKeysRepresentingIdRanges(setOfRange(0, numberOfTreeEntries));

        // when removing all the nodes from one of the tree nodes
        int treeEntryToRemoveFrom = 1;
        try (TokenIndexUpdater writer = new TokenIndexUpdater(max(5, NODE_COUNT / 100), this.idLayout)) {
            writer.initialize(tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT));
            long baseNodeId = idLayout.firstIdOfRange(treeEntryToRemoveFrom);
            for (int i = 0; i < numberOfNodesInEach; i++) {
                writer.process(TokenIndexEntryUpdate.change(baseNodeId + i, null, labels, EMPTY_INT_ARRAY));
            }
        }

        // then
        MutableLongSet expected = setOfRange(0, numberOfTreeEntries);
        expected.remove(treeEntryToRemoveFrom);
        assertTreeHasKeysRepresentingIdRanges(expected);
    }

    private TokenIndexEntryUpdate<?> randomUpdate(long[] expected) {
        int nodeId = random.nextInt(expected.length);
        long labels = expected[nodeId];
        int[] before = getLabels(labels);
        int changeCount = random.nextInt(4) + 1;
        for (int i = 0; i < changeCount; i++) {
            labels = flipRandom(labels, LABEL_COUNT, random.random());
        }
        expected[nodeId] = labels;
        return TokenIndexEntryUpdate.change(nodeId, null, before, getLabels(labels));
    }

    private void assertTreeHasKeysRepresentingIdRanges(MutableLongSet expected) throws IOException {
        tree.visit(
                new GBPTreeVisitor.Adaptor<>() {
                    @Override
                    public void key(TokenScanKey tokenScanKey, boolean isLeaf, long offloadId) {
                        if (isLeaf) {
                            assertTrue(expected.remove(tokenScanKey.idRange));
                        }
                    }
                },
                NULL_CONTEXT);
        assertTrue(expected.isEmpty());
    }

    private static MutableLongSet setOfRange(long from, long to) {
        MutableLongSet set = LongSets.mutable.empty();
        for (long i = from; i < to; i++) {
            set.add(i);
        }
        return set;
    }

    static long[] nodesWithLabel(long[] expected, int labelId) {
        int mask = 1 << labelId;
        int count = 0;
        for (long labels : expected) {
            if ((labels & mask) != 0) {
                count++;
            }
        }

        long[] result = new long[count];
        int cursor = 0;
        for (int nodeId = 0; nodeId < expected.length; nodeId++) {
            long labels = expected[nodeId];
            if ((labels & mask) != 0) {
                result[cursor++] = nodeId;
            }
        }
        return result;
    }

    static long flipRandom(long existingLabels, int highLabelId, Random random) {
        return existingLabels ^ (1L << random.nextInt(highLabelId));
    }

    public static int[] getLabels(long bits) {
        int[] result = new int[Long.bitCount(bits)];
        for (int labelId = 0, c = 0; labelId < LABEL_COUNT; labelId++) {
            int mask = 1 << labelId;
            if ((bits & mask) != 0) {
                result[c++] = labelId;
            }
        }
        return result;
    }
}
