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
package org.neo4j.internal.batchimport.cache.idmapping.string;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class RadixTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldHandleCountsLargerThanInt() {
        // when
        Radix radix = Radix.LONG.newInstance();
        long value = random.nextLong(0xFFFFFFFFFFFFL);
        long count = 0x100000000L;
        for (long i = 0; i < count; i++) {
            radix.registerRadixOf(value);
        }

        // then
        assertThat(LongStream.of(radix.radixIndexCount).sum()).isEqualTo(count);
    }

    @Test
    void shouldKeepNullValuesInSeparateCounter() {
        // given
        Radix radix = Radix.LONG.newInstance();
        long nullValue = EncodingIdMapper.GAP_VALUE;

        // when
        int expectedNumNullValues = 0;
        for (int i = 0; i < 100; i++) {
            boolean realValue = random.nextBoolean();
            radix.registerRadixOf(realValue ? random.nextLong(1, 10_000) : nullValue);
            if (!realValue) {
                expectedNumNullValues++;
            }
        }

        // then
        assertThat(radix.getNullCount()).isEqualTo(expectedNumNullValues);
    }
}
