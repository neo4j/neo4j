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
package org.neo4j.collection.trackable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIterable;

import java.util.function.IntSupplier;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;

public class HeapTrackingSkipListTest {
    @Test
    public void newSkipListIsEmpty() {
        var skipList = create();

        assertThat(skipList.isEmpty()).isTrue();
    }

    @Test
    public void popEmptyListReturnsNull() {
        var skipList = create();

        assertThat(skipList.pop()).isNull();
    }

    @Test
    public void insertThenPopReturnsElement() {
        var skipList = create();

        skipList.insert(1);

        assertThat(skipList.pop()).isEqualTo(1);
    }

    @Test
    public void populatedListIsNotEmpty() {
        var skipList = create();

        skipList.insert(1);

        assertThat(skipList.isEmpty()).isFalse();
    }

    @Test
    public void poppedListIsEmpty() {
        var skipList = create();

        skipList.insert(1);
        skipList.pop();

        assertThat(skipList.isEmpty()).isTrue();
    }

    @Test
    public void insertingUniqueElementReturnsTrue() {
        var skipList = create();

        assertThat(skipList.insert(1)).isTrue();
    }

    @Test
    public void insertingDuplicateElementReturnsFalse() {
        var skipList = create();

        skipList.insert(1);

        assertThat(skipList.insert(1)).isFalse();
    }

    @Test
    public void insertedElementAppearsInIterator() {
        var skipList = create();

        skipList.insert(1);

        assertThatIterable(skipList).containsExactly(1);
    }

    @Test
    public void insertedElementsAreIteratedInAscendingOrder() {
        var skipList = create();

        skipList.insert(2);
        skipList.insert(1);

        assertThatIterable(skipList).containsExactly(1, 2);
    }

    @Test
    public void popReturnsSmallestElement() {
        var skipList = create();

        skipList.insert(2);
        skipList.insert(1);
        skipList.insert(3);

        assertThat(skipList.pop()).isEqualTo(1);
    }

    @Test
    public void popRetainsLargerElements() {
        var skipList = create();

        skipList.insert(2);
        skipList.insert(1);
        skipList.insert(3);
        skipList.pop();

        assertThatIterable(skipList).containsExactly(2, 3);
    }

    @Test
    public void basicMemoryEstimation() {
        var mt = new LocalMemoryTracker();
        var skipList = new IntSkipList(mt);

        assertThat(mt.estimatedHeapMemory()).isEqualTo(240);
    }

    @Test
    public void memoryEstimationSingleLevel() {
        var mt = new LocalMemoryTracker();
        // here we use a constant level for new elements in the list so that the test is deterministic
        var skipList = new IntSkipList(mt, () -> 0);

        skipList.insert(1);

        assertThat(mt.estimatedHeapMemory()).isEqualTo(288L);
    }

    @Test
    public void closeReleasesAllEstimatedMemory() {
        var mt = new LocalMemoryTracker();
        var skipList = new IntSkipList(mt);

        skipList.insert(1);

        skipList.close();
        assertThat(mt.estimatedHeapMemory()).isEqualTo(0);
    }

    private IntSkipList create() {
        return new IntSkipList(EmptyMemoryTracker.INSTANCE);
    }

    class IntSkipList extends HeapTrackingSkipList<Integer> {
        private final IntSupplier getLevel;

        public IntSkipList(MemoryTracker memoryTracker, IntSupplier getLevel) {
            super(memoryTracker, Integer::compare);
            this.getLevel = getLevel;
        }

        public IntSkipList(MemoryTracker memoryTracker) {
            this(memoryTracker, null);
        }

        @Override
        protected int getLevel(Integer value) {
            return getLevel == null ? super.getLevel(value) : getLevel.getAsInt();
        }
    }
}
