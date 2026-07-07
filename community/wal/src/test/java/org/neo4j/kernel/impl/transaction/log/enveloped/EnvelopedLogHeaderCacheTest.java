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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.storageengine.api.StoreIdentifier;

class EnvelopedLogHeaderCacheTest {

    private static final int SEGMENT_BLOCK_SIZE = 512;

    private static LogHeader header(long logVersion, long lastAppendIndex) {
        return LogFormat.V10.newHeader(
                logVersion,
                lastAppendIndex,
                1,
                StoreIdentifier.UNKNOWN,
                SEGMENT_BLOCK_SIZE,
                0,
                KernelVersion.GLORIOUS_FUTURE,
                UNSPECIFIED_CREATION_TIME);
    }

    @Test
    void newCacheShouldBeEmpty() {
        var cache = new EnvelopedLogHeaderCache();
        assertThat(cache.isEmpty()).isTrue();
    }

    @Test
    void shouldCacheFirstHeader() {
        var cache = new EnvelopedLogHeaderCache();
        var h = header(7, 100);

        cache.cache(h);

        assertThat(cache.isEmpty()).isFalse();
        assertThat(cache.get(7)).isEqualTo(h);
    }

    @Test
    void shouldAppendContiguousHeaders() {
        var cache = new EnvelopedLogHeaderCache();
        var h0 = header(0, 10);
        var h1 = header(1, 20);
        var h2 = header(2, 30);

        cache.cache(h0);
        cache.cache(h1);
        cache.cache(h2);

        assertThat(cache.get(0)).isEqualTo(h0);
        assertThat(cache.get(1)).isEqualTo(h1);
        assertThat(cache.get(2)).isEqualTo(h2);
    }

    @Test
    void shouldPrependContiguousHeader() {
        var cache = new EnvelopedLogHeaderCache();
        var h5 = header(5, 50);
        var h4 = header(4, 40);
        var h3 = header(3, 30);

        cache.cache(h5);
        cache.cache(h4);
        cache.cache(h3);

        assertThat(cache.get(3)).isEqualTo(h3);
        assertThat(cache.get(4)).isEqualTo(h4);
        assertThat(cache.get(5)).isEqualTo(h5);
    }

    @Test
    void shouldReplaceExistingHeaderInsideRange() {
        var cache = new EnvelopedLogHeaderCache();
        var h0 = header(0, 10);
        var h1 = header(1, 20);
        var h2 = header(2, 30);
        cache.cache(h0);
        cache.cache(h1);
        cache.cache(h2);

        var h1Updated = header(1, 999);
        cache.cache(h1Updated);

        assertThat(cache.get(1)).isEqualTo(h1Updated);
        assertThat(cache.get(0)).isEqualTo(h0);
        assertThat(cache.get(2)).isEqualTo(h2);
    }

    @Test
    void shouldThrowForVersionBelowRange() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(5, 50));
        cache.cache(header(6, 60));

        assertThatThrownBy(() -> cache.get(4))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("Log version 4 not found in cache: available range is [5, 6]");
    }

    @Test
    void shouldThrowForVersionAboveRange() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(5, 50));
        cache.cache(header(6, 60));

        assertThatThrownBy(() -> cache.get(7))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("Log version 7 not found in cache: available range is [5, 6]");
    }

    @Test
    void cacheShouldRejectNull() {
        var cache = new EnvelopedLogHeaderCache();
        assertThatThrownBy(() -> cache.cache(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowOnGapAfterEnd() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));

        assertThatThrownBy(() -> cache.cache(header(3, 30)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Log versions are not contiguous: newVersion 3, currentEndVersion 1");
    }

    @Test
    void shouldThrowOnGapBeforeStartWhenContiguousEnforced() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(5, 50));
        cache.cache(header(6, 60));

        assertThatThrownBy(() -> cache.cache(header(2, 20)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Log versions are not contiguous: newVersion 2, currentStartVersion 5");
    }

    @Test
    void clearShouldEmptyTheCache() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));

        cache.clear();

        assertThat(cache.isEmpty()).isTrue();
        assertThat(cache.binarySearchReader().size()).isZero();
    }

    @Test
    void clearShouldBeIdempotentOnEmptyCache() {
        var cache = new EnvelopedLogHeaderCache();
        cache.clear();
        assertThat(cache.isEmpty()).isTrue();
    }

    @Test
    void deleteToShouldBeNoOpOnEmptyCache() {
        var cache = new EnvelopedLogHeaderCache();
        cache.deleteTo(10);
        assertThat(cache.isEmpty()).isTrue();
    }

    @Test
    void deleteToShouldBeNoOpWhenVersionBelowRange() {
        var cache = new EnvelopedLogHeaderCache();
        var h3 = header(3, 30);
        var h4 = header(4, 40);
        cache.cache(h3);
        cache.cache(h4);

        cache.deleteTo(2);

        assertThat(cache.get(3)).isEqualTo(h3);
        assertThat(cache.get(4)).isEqualTo(h4);
    }

    @Test
    void deleteToShouldRemoveUpToAndIncludingGivenVersion() {
        var cache = new EnvelopedLogHeaderCache();
        var h0 = header(0, 10);
        var h1 = header(1, 20);
        var h2 = header(2, 30);
        var h3 = header(3, 40);
        cache.cache(h0);
        cache.cache(h1);
        cache.cache(h2);
        cache.cache(h3);

        cache.deleteTo(1);

        assertThatThrownBy(() -> cache.get(0))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("Log version 0 not found in cache: available range is [2, 3]");
        assertThatThrownBy(() -> cache.get(1))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("Log version 1 not found in cache: available range is [2, 3]");
        assertThat(cache.get(2)).isEqualTo(h2);
        assertThat(cache.get(3)).isEqualTo(h3);
    }

    @Test
    void deleteToShouldClearAllWhenVersionAtLastEntry() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));
        cache.cache(header(2, 30));

        cache.deleteTo(2);

        assertThat(cache.isEmpty()).isTrue();
    }

    @Test
    void deleteToShouldClearAllWhenVersionBeyondRange() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));

        cache.deleteTo(99);

        assertThat(cache.isEmpty()).isTrue();
    }

    @Test
    void deleteFromShouldBeNoOpOnEmptyCache() {
        var cache = new EnvelopedLogHeaderCache();
        cache.deleteFrom(10);
        assertThat(cache.isEmpty()).isTrue();
    }

    @Test
    void deleteFromShouldClearAllWhenVersionAtOrBelowFirst() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(5, 50));
        cache.cache(header(6, 60));

        cache.deleteFrom(5);

        assertThat(cache.isEmpty()).isTrue();
    }

    @Test
    void deleteFromShouldRemoveFromGivenVersionOnwards() {
        var cache = new EnvelopedLogHeaderCache();
        var h0 = header(0, 10);
        var h1 = header(1, 20);
        var h2 = header(2, 30);
        var h3 = header(3, 40);
        cache.cache(h0);
        cache.cache(h1);
        cache.cache(h2);
        cache.cache(h3);

        cache.deleteFrom(2);

        assertThat(cache.get(0)).isEqualTo(h0);
        assertThat(cache.get(1)).isEqualTo(h1);
        assertThatThrownBy(() -> cache.get(2))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("Log version 2 not found in cache: available range is [0, 1]");
        assertThatThrownBy(() -> cache.get(3))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("Log version 3 not found in cache: available range is [0, 1]");
    }

    @Test
    void deleteFromShouldBeNoOpWhenVersionBeyondRange() {
        var cache = new EnvelopedLogHeaderCache();
        var h0 = header(0, 10);
        var h1 = header(1, 20);
        cache.cache(h0);
        cache.cache(h1);

        cache.deleteFrom(5);

        assertThat(cache.get(0)).isEqualTo(h0);
        assertThat(cache.get(1)).isEqualTo(h1);
    }

    @Test
    void shouldAllowReuseAfterClearWithDifferentStartingVersion() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));
        cache.clear();

        var h7 = header(7, 70);
        cache.cache(h7);

        assertThat(cache.get(7)).isEqualTo(h7);
        assertThatThrownBy(() -> cache.get(0))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("Log version 0 not found in cache: available range is [7, 7]");
    }

    @Test
    void binarySearchReaderShouldExposeSnapshotOfCurrentHeaders() {
        var cache = new EnvelopedLogHeaderCache();
        var h0 = header(0, 10);
        var h1 = header(1, 20);
        var h2 = header(2, 30);
        cache.cache(h0);
        cache.cache(h1);
        cache.cache(h2);

        var reader = cache.binarySearchReader();

        assertThat(reader.size()).isEqualTo(3);
        assertThat(reader.get(0)).isEqualTo(0);
        assertThat(reader.get(1)).isEqualTo(1);
        assertThat(reader.get(2)).isEqualTo(2);
    }

    @Test
    void binarySearchReaderShouldBeIsolatedFromLaterCacheMutations() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));
        var reader = cache.binarySearchReader();

        cache.clear();
        cache.cache(header(99, 999));

        assertThat(reader.size()).isEqualTo(2);
        assertThat(reader.get(0)).isEqualTo(0);
        assertThat(reader.get(1)).isEqualTo(1);
    }

    @Test
    void binarySearchReaderCompareShouldReturnNegativeWhenLastAppendIndexBelowTarget() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));

        var reader = cache.binarySearchReader();

        assertThat(reader.compare(0, 50)).isNegative();
    }

    @Test
    void binarySearchReaderCompareShouldReturnPositiveWhenLastAppendIndexAtOrAboveTarget() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));

        var reader = cache.binarySearchReader();

        assertThat(reader.compare(0, 10)).isPositive();
        assertThat(reader.compare(1, 5)).isPositive();
    }

    @Test
    void binarySearchReaderCompareShouldReturnPositiveForUnknownVersion() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));

        var reader = cache.binarySearchReader();

        assertThat(reader.compare(42, 0)).isPositive();
    }

    @Test
    void getFirstShouldReturnEmptyWhenCacheIsEmpty() {
        var cache = new EnvelopedLogHeaderCache();
        assertThat(cache.getFirst()).isEqualTo(Optional.empty());
    }

    @Test
    void getFirstShouldReturnSingleHeader() {
        var cache = new EnvelopedLogHeaderCache();
        var h = header(7, 70);
        cache.cache(h);

        assertThat(cache.getFirst()).contains(h);
    }

    @Test
    void getFirstShouldReturnLowestVersionedHeader() {
        var cache = new EnvelopedLogHeaderCache();
        var h3 = header(3, 30);
        var h4 = header(4, 40);
        var h5 = header(5, 50);
        cache.cache(h4);
        cache.cache(h5);
        cache.cache(h3);

        assertThat(cache.getFirst()).contains(h3);
    }

    @Test
    void getFirstShouldReflectDeleteTo() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));
        var h2 = header(2, 30);
        cache.cache(h2);

        cache.deleteTo(1);

        assertThat(cache.getFirst()).contains(h2);
    }

    @Test
    void getFirstShouldReturnEmptyAfterClear() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));

        cache.clear();

        assertThat(cache.getFirst()).isEqualTo(Optional.empty());
    }

    @Test
    void getLastShouldReturnEmptyWhenCacheIsEmpty() {
        var cache = new EnvelopedLogHeaderCache();
        assertThat(cache.getLast()).isEqualTo(Optional.empty());
    }

    @Test
    void getLastShouldReturnSingleHeader() {
        var cache = new EnvelopedLogHeaderCache();
        var h = header(7, 70);
        cache.cache(h);

        assertThat(cache.getLast()).contains(h);
    }

    @Test
    void getLastShouldReturnHighestVersionedHeader() {
        var cache = new EnvelopedLogHeaderCache();
        var h3 = header(3, 30);
        var h4 = header(4, 40);
        var h5 = header(5, 50);
        cache.cache(h4);
        cache.cache(h3);
        cache.cache(h5);

        assertThat(cache.getLast()).contains(h5);
    }

    @Test
    void getLastShouldReflectDeleteFrom() {
        var cache = new EnvelopedLogHeaderCache();
        var h0 = header(0, 10);
        cache.cache(h0);
        cache.cache(header(1, 20));
        cache.cache(header(2, 30));

        cache.deleteFrom(1);

        assertThat(cache.getLast()).contains(h0);
    }

    @Test
    void getLastShouldReturnEmptyAfterClear() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));

        cache.clear();

        assertThat(cache.getLast()).isEqualTo(Optional.empty());
    }

    @Test
    void getFirstAndLastShouldReturnSameHeaderForSingleEntryCache() {
        var cache = new EnvelopedLogHeaderCache();
        var h = header(42, 420);
        cache.cache(h);

        assertThat(cache.getFirst()).contains(h);
        assertThat(cache.getLast()).contains(h);
    }

    @Test
    void logVersionsRangeShouldBeEmptyRangeWhenCacheIsEmpty() {
        var cache = new EnvelopedLogHeaderCache();
        assertThat(cache.logVersionsRange()).isEqualTo(LongRange.EMPTY_RANGE);
    }

    @Test
    void logVersionsRangeShouldCoverSingleEntry() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(5, 50));

        assertThat(cache.logVersionsRange()).isEqualTo(LongRange.range(5, 5));
    }

    @Test
    void logVersionsRangeShouldSpanFromLowestToHighestVersion() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(3, 30));
        cache.cache(header(4, 40));
        cache.cache(header(5, 50));

        assertThat(cache.logVersionsRange()).isEqualTo(LongRange.range(3, 5));
    }

    @Test
    void logVersionsRangeShouldUpdateAfterPrepend() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(5, 50));
        cache.cache(header(4, 40));
        cache.cache(header(3, 30));

        assertThat(cache.logVersionsRange()).isEqualTo(LongRange.range(3, 5));
    }

    @Test
    void logVersionsRangeShouldUpdateAfterDeleteTo() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));
        cache.cache(header(2, 30));
        cache.cache(header(3, 40));

        cache.deleteTo(1);

        assertThat(cache.logVersionsRange()).isEqualTo(LongRange.range(2, 3));
    }

    @Test
    void logVersionsRangeShouldUpdateAfterDeleteFrom() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));
        cache.cache(header(2, 30));
        cache.cache(header(3, 40));

        cache.deleteFrom(2);

        assertThat(cache.logVersionsRange()).isEqualTo(LongRange.range(0, 1));
    }

    @Test
    void logVersionsRangeShouldBeEmptyRangeAfterClear() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));

        cache.clear();

        assertThat(cache.logVersionsRange()).isEqualTo(LongRange.EMPTY_RANGE);
    }

    @Test
    void forEachLogHeaderShouldNotInvokeConsumerWhenCacheIsEmpty() {
        var cache = new EnvelopedLogHeaderCache();
        var collected = new ArrayList<LogHeader>();

        cache.forEachLogHeader(collected::add);

        assertThat(collected).isEmpty();
    }

    @Test
    void forEachLogHeaderShouldVisitHeadersInVersionOrder() {
        var cache = new EnvelopedLogHeaderCache();
        var h0 = header(0, 10);
        var h1 = header(1, 20);
        var h2 = header(2, 30);
        cache.cache(h2);
        cache.cache(h1);
        cache.cache(h0);

        var collected = new ArrayList<LogHeader>();
        cache.forEachLogHeader(collected::add);

        assertThat(collected).containsExactly(h0, h1, h2);
    }

    @Test
    void forEachLogHeaderShouldVisitReplacedHeader() {
        var cache = new EnvelopedLogHeaderCache();
        var h0 = header(0, 10);
        var h1 = header(1, 20);
        var h2 = header(2, 30);
        cache.cache(h0);
        cache.cache(h1);
        cache.cache(h2);

        var h1Updated = header(1, 999);
        cache.cache(h1Updated);

        var collected = new ArrayList<LogHeader>();
        cache.forEachLogHeader(collected::add);

        assertThat(collected).containsExactly(h0, h1Updated, h2);
    }

    @Test
    void forEachLogHeaderShouldReflectDeletions() {
        var cache = new EnvelopedLogHeaderCache();
        var h0 = header(0, 10);
        var h1 = header(1, 20);
        var h2 = header(2, 30);
        var h3 = header(3, 40);
        cache.cache(h0);
        cache.cache(h1);
        cache.cache(h2);
        cache.cache(h3);

        cache.deleteTo(0);
        cache.deleteFrom(3);

        var collected = new ArrayList<LogHeader>();
        cache.forEachLogHeader(collected::add);

        assertThat(collected).containsExactly(h1, h2);
    }

    @Test
    void forEachLogHeaderShouldVisitNothingAfterClear() {
        var cache = new EnvelopedLogHeaderCache();
        cache.cache(header(0, 10));
        cache.cache(header(1, 20));
        cache.clear();

        var collected = new ArrayList<LogHeader>();
        cache.forEachLogHeader(collected::add);

        assertThat(collected).isEmpty();
    }
}
