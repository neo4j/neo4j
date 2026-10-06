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
package org.neo4j.io.async;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.io.async.AsyncCompletionHandler.EMPTY_COMPLETION_HANDLER;
import static org.neo4j.io.async.IllegalStateExceptionFailureHandler.ILLEGAL_STATE_HANDLER;

import org.junit.jupiter.api.Test;
import org.neo4j.memory.EmptyMemoryTracker;

class DefaultAsyncIOProviderTest {

    @Test
    void asyncProviderIsNotAvailableByDefault() {
        var asyncIOProvider = AsyncIOProvider.getInstance();
        var asyncBlockAccessor = asyncIOProvider.createAsyncBlockAccessor(
                10, EMPTY_COMPLETION_HANDLER, ILLEGAL_STATE_HANDLER, EmptyMemoryTracker.INSTANCE);

        assertThat(asyncBlockAccessor.isAvailable()).isFalse();

        assertThatThrownBy(() -> asyncBlockAccessor.asyncRead(1, 2, 3, 4, 5))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> asyncBlockAccessor.asyncWrite(1, 2, 3, 4, 5))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> asyncBlockAccessor.asyncVectorRead(1, 2, new long[] {3}, new int[] {4}))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> asyncBlockAccessor.asyncVectorWrite(
                        1, 2, new long[] {3}, new int[] {4}, 1, new long[] {1}, new long[] {2}, 1))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void describesADeclinedProviderWithEveryCause() {
        var failure = new NoClassDefFoundError("Could not initialize class Ring");
        failure.initCause(new ExceptionInInitializerError(new IllegalArgumentException("Cannot open library")));

        assertThat(AsyncIOProvider.AsyncIOProviderHolder.describeFailure(new AbsentAsyncIOProvider(), failure))
                .startsWith(AbsentAsyncIOProvider.class.getName())
                .contains("Could not initialize class Ring")
                .contains("Cannot open library");
    }
}
