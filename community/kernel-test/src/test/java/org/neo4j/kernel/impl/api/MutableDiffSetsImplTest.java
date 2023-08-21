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
package org.neo4j.kernel.impl.api;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterators.asCollection;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.diffset.MutableDiffSets;
import org.neo4j.collection.diffset.MutableDiffSetsImpl;
import org.neo4j.memory.EmptyMemoryTracker;

class MutableDiffSetsImplTest {
    private static final Predicate<Long> ODD_FILTER = item -> item % 2 != 0;

    private final MutableDiffSets<Long> diffSets = MutableDiffSetsImpl.newMutableDiffSets(EmptyMemoryTracker.INSTANCE);

    @Test
    void testAdd() {
        // WHEN
        diffSets.add(1L);
        diffSets.add(2L);

        // THEN
        assertEquals(asSet(1L, 2L), diffSets.getAdded());
        assertTrue(diffSets.getRemoved().isEmpty());
    }

    @Test
    void testRemove() {
        // WHEN
        diffSets.add(1L);
        diffSets.remove(2L);

        // THEN
        assertEquals(asSet(1L), diffSets.getAdded());
        assertEquals(asSet(2L), diffSets.getRemoved());
    }

    @Test
    void testAddRemove() {
        // WHEN
        diffSets.add(1L);
        diffSets.remove(1L);

        // THEN
        assertTrue(diffSets.getAdded().isEmpty());
        assertTrue(diffSets.getRemoved().isEmpty());
    }

    @Test
    void testRemoveAdd() {
        // WHEN
        diffSets.remove(1L);
        diffSets.add(1L);

        // THEN
        assertTrue(diffSets.getAdded().isEmpty());
        assertTrue(diffSets.getRemoved().isEmpty());
    }

    @Test
    void testIsAddedOrRemoved() {
        // WHEN
        diffSets.add(1L);
        diffSets.remove(10L);

        // THEN
        assertTrue(diffSets.isAdded(1L));
        assertFalse(diffSets.isAdded(2L));
        assertTrue(diffSets.isRemoved(10L));
        assertFalse(diffSets.isRemoved(2L));
    }

    @Test
    void testReturnSourceFromApplyWithEmptyDiffSets() {
        // WHEN
        Iterator<Long> result = diffSets.apply(singletonList(18L).iterator());

        // THEN
        assertEquals(singletonList(18L), asCollection(result));
    }

    @Test
    void testAppendAddedToSourceInApply() {
        // GIVEN
        diffSets.add(52L);
        diffSets.remove(43L);

        // WHEN
        Iterator<Long> result = diffSets.apply(singletonList(18L).iterator());

        // THEN
        assertEquals(asList(18L, 52L), asCollection(result));
    }

    @Test
    void testFilterRemovedFromSourceInApply() {
        // GIVEN
        diffSets.remove(43L);

        // WHEN
        Iterator<Long> result = diffSets.apply(asList(42L, 43L, 44L).iterator());

        // THEN
        assertEquals(asList(42L, 44L), asCollection(result));
    }

    @Test
    void testFilterAddedFromSourceInApply() {
        // GIVEN
        diffSets.add(42L);
        diffSets.add(44L);

        // WHEN
        Iterator<Long> result = diffSets.apply(asList(42L, 43L).iterator());

        // THEN
        Collection<Long> collectedResult = asCollection(result);
        assertEquals(3, collectedResult.size());
        assertThat(collectedResult).contains(43L, 42L, 44L);
    }
}
