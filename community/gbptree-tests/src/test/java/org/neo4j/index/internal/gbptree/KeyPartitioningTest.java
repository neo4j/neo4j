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
package org.neo4j.index.internal.gbptree;

import static java.lang.Math.abs;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith({RandomExtension.class, SoftAssertionsExtension.class})
class KeyPartitioningTest {
    @Inject
    private RandomSupport random;

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldPartitionEvenly() {
        // given
        final var layout = layout();
        final var numberOfKeys = random.nextInt(50, 200);
        final var allKeys = keys(numberOfKeys);
        final var partitioning = new KeyPartitioning<>(layout);

        // when
        final var from = random.nextInt(numberOfKeys - 1);
        final var to = random.nextInt(from + 1, numberOfKeys);
        final var desiredNumberOfPartitions = random.nextInt(1, to - from + 1);

        final var partitionEdges = partitioning.partition(
                allKeys, new PartitionKey(from), new PartitionKey(to), desiredNumberOfPartitions);

        // then verify that the partitions have no seams in between them, that they cover the whole requested range and
        // are fairly evenly distributed
        softly.assertThat(partitionEdges.size() - 1)
                .as("at least one partition")
                .isGreaterThanOrEqualTo(1)
                .as("no larger than desired")
                .isLessThanOrEqualTo(desiredNumberOfPartitions);
        softly.assertThat(partitionEdges.get(0).value).as("initial edge").isEqualTo(from);
        softly.assertThat(partitionEdges.get(partitionEdges.size() - 1).value)
                .as("final edge")
                .isEqualTo(to);
        final var diff = diff(partitionEdges, 0);
        for (int i = 1; i < partitionEdges.size() - 2; i++) {
            softly.assertThat(abs(diff - diff(partitionEdges, i)))
                    .as("no seams in between partitions %d and %d", i, i + 1)
                    .isLessThanOrEqualTo(1);
        }
    }

    private int diff(List<PartitionKey> partitionEdges, int partition) {
        softly.assertThat(partition)
                .as("valid partition index")
                .isGreaterThanOrEqualTo(0)
                .isLessThanOrEqualTo(partitionEdges.size() - 2);

        return partitionEdges.get(partition).value - partitionEdges.get(partition + 1).value;
    }

    private static Layout<PartitionKey, ?> layout() {
        @SuppressWarnings("unchecked")
        final var layout = (Layout<PartitionKey, ?>) mock(Layout.class);

        when(layout.newKey()).thenAnswer(invocationOnMock -> new PartitionKey());
        when(layout.copyKey(any(), any())).thenAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(1, PartitionKey.class).value =
                    invocationOnMock.getArgument(0, PartitionKey.class).value;
            return null;
        });
        when(layout.compare(any(), any()))
                .thenAnswer(invocationOnMock -> Integer.compare(
                        invocationOnMock.getArgument(0, PartitionKey.class).value,
                        invocationOnMock.getArgument(1, PartitionKey.class).value));

        return layout;
    }

    private static SortedSet<PartitionKey> keys(int count) {
        return IntStream.range(0, count)
                .mapToObj(PartitionKey::new)
                .collect(Collectors.toCollection(() -> new TreeSet<>(layout())));
    }

    private static class PartitionKey {
        int value;

        PartitionKey() {
            this(0);
        }

        PartitionKey(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
}
