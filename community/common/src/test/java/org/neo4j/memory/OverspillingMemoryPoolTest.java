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
package org.neo4j.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

class OverspillingMemoryPoolTest {
    private static final long LOCAL_RESERVED = 100;

    private final MemoryPool global = new MemoryPoolImpl(1000, true, "global.limit");
    private final OverspillingMemoryPool pool =
            new OverspillingMemoryPool(MemoryGroup.RAFT, global, "db", LOCAL_RESERVED, "local.limit");

    @Test
    void closeReturnsHeldGlobalReservations() {
        // given
        pool.reserveHeap(LOCAL_RESERVED + 50);
        pool.reserveNative(LOCAL_RESERVED + 50);

        // when
        pool.close();

        // then
        assertThat(global.usedHeap()).isZero();
        assertThat(global.usedNative()).isZero();
    }

    @Test
    void reserveHeapWithinReservationUsesLocalOnly() {
        // when
        pool.reserveHeap(60);

        // then
        assertThat(pool.usedHeap()).isEqualTo(60);
        assertThat(pool.free()).isEqualTo(LOCAL_RESERVED - 60);
        assertThat(global.usedHeap()).isZero();
    }

    @Test
    void reserveHeapBeyondReservationOverflowsRemainderToGlobal() {
        // when
        pool.reserveHeap(LOCAL_RESERVED + 50);

        // then
        assertThat(pool.usedHeap()).isEqualTo(LOCAL_RESERVED);
        assertThat(global.usedHeap()).isEqualTo(50);
    }

    @Test
    void reserveHeapGoesEntirelyToGlobalWhenReservationAlreadyExhausted() {
        // given
        pool.reserveHeap(LOCAL_RESERVED);

        // when
        pool.reserveHeap(30);

        // then
        assertThat(pool.usedHeap()).isEqualTo(LOCAL_RESERVED);
        assertThat(global.usedHeap()).isEqualTo(30);
    }

    @Test
    void reserveHeapNoThrowWithinReservationUsesLocalOnly() {
        // when
        pool.reserveHeapNoThrow(60);

        // then
        assertThat(pool.usedHeap()).isEqualTo(60);
        assertThat(pool.free()).isEqualTo(LOCAL_RESERVED - 60);
        assertThat(global.usedHeap()).isZero();
    }

    @Test
    void reserveHeapNoThrowBeyondReservationOverflowsRemainderToGlobal() {
        // when
        pool.reserveHeapNoThrow(LOCAL_RESERVED + 50);

        // then
        assertThat(pool.usedHeap()).isEqualTo(LOCAL_RESERVED);
        assertThat(global.usedHeap()).isEqualTo(50);
    }

    @Test
    void reserveHeapNoThrowBeyondGlobalLimitIsRecordedWithoutThrowing() {
        // when
        pool.reserveHeapNoThrow(LOCAL_RESERVED);
        assertDoesNotThrow(() -> pool.reserveHeapNoThrow(global.totalSize() + 50));

        // then
        assertThat(pool.usedHeap()).isEqualTo(LOCAL_RESERVED);
        assertThat(global.usedHeap()).isEqualTo(global.totalSize() + 50);
    }

    @Test
    void releaseHeapReleasesFromLocalIfNothingInGlobal() {
        // given
        pool.reserveHeap(LOCAL_RESERVED);

        // when
        pool.releaseHeap(50);

        // then
        assertThat(global.usedHeap()).isZero();
        assertThat(pool.usedHeap()).isEqualTo(LOCAL_RESERVED - 50);
    }

    @Test
    void releaseHeapReleasesFromGlobalBeforeLocal() {
        // given
        pool.reserveHeap(LOCAL_RESERVED + 50);

        // when
        pool.releaseHeap(50);

        // then
        assertThat(global.usedHeap()).isZero();
        assertThat(pool.usedHeap()).isEqualTo(LOCAL_RESERVED);
    }

    @Test
    void releaseHeapSpanningBothReturnsGlobalThenLocal() {
        // given
        pool.reserveHeap(LOCAL_RESERVED + 50);

        // when
        pool.releaseHeap(120);

        // then
        assertThat(global.usedHeap()).isZero();
        assertThat(pool.usedHeap()).isEqualTo(LOCAL_RESERVED - 70);
    }

    @Test
    void reserveNativeWithinReservationUsesLocalOnly() {
        // when
        pool.reserveNative(60);

        // then
        assertThat(pool.usedNative()).isEqualTo(60);
        assertThat(pool.free()).isEqualTo(LOCAL_RESERVED - 60);
        assertThat(global.usedNative()).isZero();
    }

    @Test
    void reserveNativeBeyondReservationOverflowsRemainderToGlobal() {
        // when
        pool.reserveNative(LOCAL_RESERVED + 50);

        // then
        assertThat(pool.usedNative()).isEqualTo(LOCAL_RESERVED);
        assertThat(global.usedNative()).isEqualTo(50);
    }

    @Test
    void reserveNativeGoesEntirelyToGlobalWhenReservationAlreadyExhausted() {
        // given
        pool.reserveNative(LOCAL_RESERVED);

        // when
        pool.reserveNative(30);

        // then
        assertThat(pool.usedNative()).isEqualTo(LOCAL_RESERVED);
        assertThat(global.usedNative()).isEqualTo(30);
    }

    @Test
    void reserveNativeNoThrowWithinReservationUsesLocalOnly() {
        // when
        pool.reserveNativeNoThrow(60);

        // then
        assertThat(pool.usedNative()).isEqualTo(60);
        assertThat(pool.free()).isEqualTo(LOCAL_RESERVED - 60);
        assertThat(global.usedNative()).isZero();
    }

    @Test
    void reserveNativeNoThrowBeyondReservationOverflowsRemainderToGlobal() {
        // when
        pool.reserveNativeNoThrow(LOCAL_RESERVED + 50);

        // then
        assertThat(pool.usedNative()).isEqualTo(LOCAL_RESERVED);
        assertThat(global.usedNative()).isEqualTo(50);
    }

    @Test
    void reserveNativeNoThrowBeyondGlobalLimitIsRecordedWithoutThrowing() {
        // when
        pool.reserveNativeNoThrow(LOCAL_RESERVED);
        assertDoesNotThrow(() -> pool.reserveNativeNoThrow(global.totalSize() + 50));

        // then
        assertThat(pool.usedNative()).isEqualTo(LOCAL_RESERVED);
        assertThat(global.usedNative()).isEqualTo(global.totalSize() + 50);
    }

    @Test
    void releaseNativeReleasesFromLocalIfNothingInGlobal() {
        // given
        pool.reserveNative(LOCAL_RESERVED);

        // when
        pool.releaseNative(50);

        // then
        assertThat(global.usedNative()).isZero();
        assertThat(pool.usedNative()).isEqualTo(LOCAL_RESERVED - 50);
    }

    @Test
    void releaseNativeReleasesFromGlobalBeforeLocal() {
        // given
        pool.reserveNative(LOCAL_RESERVED + 50);

        // when
        pool.releaseNative(50);

        // then
        assertThat(global.usedNative()).isZero();
        assertThat(pool.usedNative()).isEqualTo(LOCAL_RESERVED);
    }

    @Test
    void releaseNativeSpanningBothReturnsGlobalThenLocal() {
        // given
        pool.reserveNative(LOCAL_RESERVED + 50);

        // when
        pool.releaseNative(120);

        // then
        assertThat(global.usedNative()).isZero();
        assertThat(pool.usedNative()).isEqualTo(LOCAL_RESERVED - 70);
    }
}
