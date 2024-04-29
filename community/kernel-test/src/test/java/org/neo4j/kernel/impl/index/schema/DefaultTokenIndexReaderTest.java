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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracker.NO_USAGE_TRACKER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.storageengine.api.schema.SimpleEntityTokenClient;

@Execution(CONCURRENT)
class DefaultTokenIndexReaderTest {
    private static final int LABEL_ID = 1;
    private final long[] expected = {
        // base 0*64 = 0
        1,
        6,
        7,
        11,
        15,
        // base 1*64 = 64
        64 + 3,
        64 + 9,
        // base 3*64 = 192
        192,
        192 + 5,
        192 + 7,
        192 + 13
    };
    private GBPTree<TokenScanKey, TokenScanValue> index;

    @BeforeEach
    void setUp() throws IOException {
        index = mock(GBPTree.class);

        when(index.seek(any(TokenScanKey.class), any(TokenScanKey.class), eq(NULL_CONTEXT)))
                .thenAnswer(innvocation -> cursor(innvocation.getArgument(0, TokenScanKey.class).idRange
                        <= innvocation.getArgument(1, TokenScanKey.class).idRange));
    }

    private Seeker<TokenScanKey, TokenScanValue> cursor(boolean ascending) {
        var layout = new DefaultTokenIndexIdLayout();
        List<Pair<TokenScanKey, TokenScanValue>> entries = new ArrayList<>();
        TokenScanKey currentKey = null;
        TokenScanValue currentValue = new TokenScanValue();
        for (long id : expected) {
            long idRange = layout.rangeOf(id);
            if (currentKey == null || currentKey.idRange != idRange) {
                if (currentKey != null) {
                    entries.add(Pair.of(currentKey, currentValue));
                }
                currentKey = new TokenScanKey(LABEL_ID, idRange);
                currentValue = new TokenScanValue();
            }
            currentValue.set((int) (id % TokenScanValue.RANGE_SIZE));
        }
        entries.add(Pair.of(currentKey, currentValue));
        return new LabelsSeeker(entries, ascending);
    }

    @Test
    void shouldFindMultipleEntitiesInEachRange() {
        // WHEN
        var reader = new DefaultTokenIndexReader(index, NO_USAGE_TRACKER, new DefaultTokenIndexIdLayout());
        SimpleEntityTokenClient tokenClient = new SimpleEntityTokenClient();
        reader.query(tokenClient, unconstrained(), new TokenPredicate(LABEL_ID), NULL_CONTEXT);

        // THEN
        assertThat(asArray(tokenClient)).contains(expected);
    }

    @Test
    void shouldFindMultipleWithProgressorAscending() {
        // WHEN
        var reader = new DefaultTokenIndexReader(index, NO_USAGE_TRACKER, new DefaultTokenIndexIdLayout());
        SimpleEntityTokenClient tokenClient = new SimpleEntityTokenClient();
        reader.query(
                tokenClient,
                IndexQueryConstraints.constrained(IndexOrder.ASCENDING, false),
                new TokenPredicate(LABEL_ID),
                NULL_CONTEXT);

        // THEN
        assertThat(asArray(tokenClient)).contains(expected);
    }

    @Test
    void shouldFindMultipleWithProgressorDescending() {
        // WHEN
        var reader = new DefaultTokenIndexReader(index, NO_USAGE_TRACKER, new DefaultTokenIndexIdLayout());
        SimpleEntityTokenClient tokenClient = new SimpleEntityTokenClient();
        reader.query(
                tokenClient,
                IndexQueryConstraints.constrained(IndexOrder.DESCENDING, false),
                new TokenPredicate(LABEL_ID),
                NULL_CONTEXT);

        // THEN
        ArrayUtils.reverse(expected);
        assertThat(asArray(tokenClient)).contains(expected);
    }

    private static long[] asArray(SimpleEntityTokenClient valueClient) {
        MutableLongList result = LongLists.mutable.empty();
        while (valueClient.next()) {
            result.add(valueClient.reference);
        }
        return result.toArray();
    }

    private static TokenScanValue value(long bits) {
        TokenScanValue value = new TokenScanValue();
        value.bits = bits;
        return value;
    }

    private static TokenScanKey key(long idRange) {
        return new TokenScanKey(LABEL_ID, idRange);
    }
}
