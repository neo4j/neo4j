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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;

@Execution(CONCURRENT)
class PhysicalToLogicalTokenChangesTest {
    @Test
    void shouldSeeSimpleAddition() {
        convertAndAssert(
                // before/after
                ids(), ids(2),
                // removed/added
                ids(), ids(2));
    }

    @Test
    void shouldSeeSimpleRemoval() {
        convertAndAssert(
                // before/after
                ids(2), ids(),
                // removed/added
                ids(2), ids());
    }

    @Test
    void shouldSeeSomeAdded() {
        convertAndAssert(
                // before/after
                ids(1, 3, 5), ids(1, 2, 3, 4, 5, 6),
                // removed/added
                ids(), ids(2, 4, 6));
    }

    @Test
    void shouldSeeSomeRemoved() {
        convertAndAssert(
                // before/after
                ids(1, 2, 3, 4, 5, 6), ids(1, 3, 5),
                // removed/added
                ids(2, 4, 6), ids());
    }

    @Test
    void shouldSeeSomeAddedAndSomeRemoved() {
        convertAndAssert(
                // before/after
                ids(1, 3, 4, 6), ids(0, 2, 3, 5, 6),
                // removed/added
                ids(1, 4), ids(0, 2, 5));
    }

    @Test
    void shouldFailOnNegativeInput() {
        assertIAE(ids(1, 3, -1, 6), ids());
        assertIAE(ids(1, 3, -1, 6), ids(1, 3, 4));
        assertIAE(ids(), ids(1, 3, -1, 6));
        assertIAE(ids(1, 3, 4), ids(1, 3, -1, 6));
    }

    private static void convertAndAssert(long[] before, long[] after, long[] expectedRemoved, long[] expectedAdded) {
        TokenIndexEntryUpdate<?> update = TokenIndexEntryUpdate.change(0, null, before, after);
        PhysicalToLogicalTokenChanges.LogicalTokenUpdates logicalTokenUpdates =
                PhysicalToLogicalTokenChanges.convertToAdditionsAndRemovals(update);
        assertThat(truncate(logicalTokenUpdates.removals())).containsExactly(expectedRemoved);
        assertThat(truncate(logicalTokenUpdates.additions())).containsExactly(expectedAdded);
    }

    private void assertIAE(long[] before, long[] after) {
        TokenIndexEntryUpdate<?> update = TokenIndexEntryUpdate.change(0, null, before, after);
        assertThatThrownBy(() -> PhysicalToLogicalTokenChanges.convertToAdditionsAndRemovals(update))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected non-negative long value");
    }

    private static long[] truncate(long[] tokenIds) {
        int length = actualLength(tokenIds);
        return length == tokenIds.length ? tokenIds : Arrays.copyOf(tokenIds, length);
    }

    private static int actualLength(long[] labels) {
        for (int i = 0; i < labels.length; i++) {
            if (labels[i] == -1) {
                return i;
            }
        }
        return labels.length;
    }

    private static long[] ids(long... ids) {
        return ids;
    }
}
