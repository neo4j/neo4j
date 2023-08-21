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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;

abstract class TokenIndexOrderTestBase<TOKEN_INDEX_CURSOR extends Cursor>
        extends KernelAPIWriteTestBase<WriteTestSupport> {

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldTokenScanInOrder(IndexOrder indexOrder) throws Exception {
        List<Long> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithToken(tx, "INSIDE"));
            entityWithToken(tx, "OUTSIDE1");
            expected.add(entityWithToken(tx, "INSIDE"));
            expected.add(entityWithToken(tx, "INSIDE"));
            expected.add(entityWithToken(tx, "INSIDE"));
            entityWithToken(tx, "OUTSIDE2");
            expected.add(entityWithToken(tx, "INSIDE"));
            expected.add(entityWithToken(tx, "INSIDE"));
            expected.add(entityWithToken(tx, "INSIDE"));
            expected.add(entityWithToken(tx, "INSIDE"));
            entityWithToken(tx, "OUTSIDE1");
            expected.add(entityWithToken(tx, "INSIDE"));
            entityWithToken(tx, "OUTSIDE1");
            entityWithToken(tx, "OUTSIDE1");
            entityWithToken(tx, "OUTSIDE2");
            expected.add(entityWithToken(tx, "INSIDE"));
            entityWithToken(tx, "OUTSIDE2");
            tx.commit();
        }

        // when
        try (KernelTransaction tx = beginTransaction()) {
            int label = tokenByName(tx, "INSIDE");

            try (var cursor = getIndexCursor(tx)) {
                entityWithToken(tx, "OUTSIDE1");
                entityWithToken(tx, "OUTSIDE1");
                expected.add(entityWithToken(tx, "INSIDE"));
                entityWithToken(tx, "OUTSIDE1");
                entityWithToken(tx, "OUTSIDE2");
                expected.add(entityWithToken(tx, "INSIDE"));
                expected.add(entityWithToken(tx, "INSIDE"));
                expected.add(entityWithToken(tx, "INSIDE"));
                entityWithToken(tx, "OUTSIDE2");
                expected.add(entityWithToken(tx, "INSIDE"));

                tokenScan(indexOrder, tx, label, cursor);

                if (isNodeBased(tx)) {
                    assertThat(exhaust(cursor)).containsExactlyInAnyOrderElementsOf(expected);
                } else {
                    assertTokenResultsInOrder(expected, cursor, indexOrder);
                }
            }
        }
    }

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }

    protected void assertTokenResultsInOrder(List<Long> expected, TOKEN_INDEX_CURSOR cursor, IndexOrder indexOrder) {
        expected.sort(indexOrder == IndexOrder.ASCENDING ? Comparator.naturalOrder() : Comparator.reverseOrder());
        assertThat(exhaust(cursor)).isEqualTo(expected);
    }

    protected List<Long> exhaust(TOKEN_INDEX_CURSOR cursor) {
        var actual = new ArrayList<Long>();
        while (cursor.next()) {
            actual.add(entityReference(cursor));
        }
        return actual;
    }

    protected abstract long entityWithToken(KernelTransaction tx, String name) throws Exception;

    protected abstract int tokenByName(KernelTransaction tx, String name);

    protected abstract void tokenScan(IndexOrder indexOrder, KernelTransaction tx, int label, TOKEN_INDEX_CURSOR cursor)
            throws KernelException;

    protected abstract long entityReference(TOKEN_INDEX_CURSOR cursor);

    protected abstract TOKEN_INDEX_CURSOR getIndexCursor(KernelTransaction tx);
}
