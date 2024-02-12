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
package org.neo4j.bolt.protocol.common.connector.accounting.error;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.time.FakeClock;

class TimeConstrainedCounterTest {

    @TestFactory
    Stream<DynamicTest> shouldIncrementWithinWindow() {
        return IntStream.range(1, 30).mapToObj(multiplier -> {
            var window = 100 * multiplier;

            return DynamicTest.dynamicTest(String.format("%d ms", window), () -> {
                var clock = new FakeClock();

                var counter = new TimeConstrainedCounter(window, clock);

                for (var i = 0; i < 8; ++i) {
                    var result = counter.incrementAndGet();

                    Assertions.assertThat(result).isEqualTo(i + 1);
                }

                clock.forward(50, TimeUnit.MILLISECONDS);

                var result = counter.incrementAndGet();

                Assertions.assertThat(result).isEqualTo(9);

                clock.forward(window, TimeUnit.MILLISECONDS);

                result = counter.incrementAndGet();

                Assertions.assertThat(result).isEqualTo(1);
            });
        });
    }
}
