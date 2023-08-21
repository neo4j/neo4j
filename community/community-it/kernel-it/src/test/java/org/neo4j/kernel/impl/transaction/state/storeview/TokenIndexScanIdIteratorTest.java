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
package org.neo4j.kernel.impl.transaction.state.storeview;

import static java.util.stream.StreamSupport.stream;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class TokenIndexScanIdIteratorTest {
    @Inject
    GraphDatabaseAPI db;

    @Inject
    IndexingService indexingService;

    @Test
    void shouldFindNodesWithAnyOfGivenLabels() throws Exception {
        // GIVEN
        int labelId1 = 3;
        int labelId2 = 5;
        int labelId3 = 13;

        IndexDescriptor index;
        try (Transaction tx = db.beginTx()) {
            index = ((IndexDefinitionImpl) stream(tx.schema().getIndexes().spliterator(), false)
                            .filter(IndexDefinition::isNodeIndex)
                            .findFirst()
                            .get())
                    .getIndexReference();
        }

        IndexProxy indexProxy = indexingService.getIndexProxy(index);

        try (IndexUpdater indexUpdater =
                indexProxy.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
            indexUpdater.process(IndexEntryUpdate.change(2, index, EMPTY_LONG_ARRAY, new long[] {labelId1, labelId2}));
            indexUpdater.process(IndexEntryUpdate.change(1, index, EMPTY_LONG_ARRAY, new long[] {labelId1}));
            indexUpdater.process(IndexEntryUpdate.change(4, index, EMPTY_LONG_ARRAY, new long[] {labelId1, labelId3}));
            indexUpdater.process(
                    IndexEntryUpdate.change(5, index, EMPTY_LONG_ARRAY, new long[] {labelId1, labelId2, labelId3}));
            indexUpdater.process(IndexEntryUpdate.change(3, index, EMPTY_LONG_ARRAY, new long[] {labelId1}));
            indexUpdater.process(IndexEntryUpdate.change(7, index, EMPTY_LONG_ARRAY, new long[] {labelId2}));
            indexUpdater.process(IndexEntryUpdate.change(8, index, EMPTY_LONG_ARRAY, new long[] {labelId3}));
            indexUpdater.process(IndexEntryUpdate.change(6, index, EMPTY_LONG_ARRAY, new long[] {labelId2}));
            indexUpdater.process(IndexEntryUpdate.change(9, index, EMPTY_LONG_ARRAY, new long[] {labelId3}));
        }

        // THEN
        try (TokenIndexReader indexReader = indexProxy.newTokenReader()) {
            assertThat(findAllWithTokens(indexReader, new int[] {labelId1, labelId2}))
                    .isEqualTo(new long[] {1, 2, 3, 4, 5, 6, 7});
            assertThat(findAllWithTokens(indexReader, new int[] {labelId1, labelId3}))
                    .isEqualTo(new long[] {1, 2, 3, 4, 5, 8, 9});
            assertThat(findAllWithTokens(indexReader, new int[] {labelId1, labelId2, labelId3}))
                    .isEqualTo(new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9});
        }
    }

    private static long[] findAllWithTokens(TokenIndexReader indexReader, int[] tokens) {
        TokenIndexScanIdIterator iter = new TokenIndexScanIdIterator(indexReader, tokens, CursorContext.NULL_CONTEXT);
        MutableLongList found = LongLists.mutable.empty();
        while (iter.hasNext()) {
            found.add(iter.next());
        }
        return found.toArray(new long[] {});
    }
}
