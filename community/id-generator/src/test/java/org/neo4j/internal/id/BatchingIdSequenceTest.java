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
package org.neo4j.internal.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

@Execution(CONCURRENT)
class BatchingIdSequenceTest {
    public static final long INTEGER_MINUS_ONE = 0xFFFFFFFFL;

    @Test
    void ShouldSkipNullId() {
        BatchingIdSequence idSequence = new BatchingIdSequence();

        idSequence.set(INTEGER_MINUS_ONE - 1);
        assertThat(idSequence.peek()).isEqualTo(INTEGER_MINUS_ONE - 1);

        // The 'NULL Id' should be skipped, and never be visible anywhere.
        // Peek should always return what nextId will return

        assertThat(idSequence.nextId(NULL_CONTEXT)).isEqualTo(INTEGER_MINUS_ONE - 1);
        assertThat(idSequence.peek()).isEqualTo(INTEGER_MINUS_ONE + 1);
        assertThat(idSequence.nextId(NULL_CONTEXT)).isEqualTo(INTEGER_MINUS_ONE + 1);

        // And what if someone were to set it directly to the NULL id
        idSequence.set(INTEGER_MINUS_ONE);

        assertThat(idSequence.peek()).isEqualTo(INTEGER_MINUS_ONE + 1);
        assertThat(idSequence.nextId(NULL_CONTEXT)).isEqualTo(INTEGER_MINUS_ONE + 1);
    }

    @Test
    void resetShouldSetDefault() {
        BatchingIdSequence idSequence = new BatchingIdSequence();

        idSequence.set(99L);

        assertThat(idSequence.peek()).isEqualTo(99L);
        assertThat(idSequence.nextId(NULL_CONTEXT)).isEqualTo(99L);
        assertThat(idSequence.peek()).isEqualTo(100L);

        idSequence.reset();

        assertThat(idSequence.peek()).isEqualTo(0L);
        assertThat(idSequence.nextId(NULL_CONTEXT)).isEqualTo(0L);
        assertThat(idSequence.peek()).isEqualTo(1L);
    }
}
