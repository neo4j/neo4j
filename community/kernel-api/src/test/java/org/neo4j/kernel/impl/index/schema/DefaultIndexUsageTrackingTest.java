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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.test.Race;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

class DefaultIndexUsageTrackingTest {
    private FakeClock clock;
    private DefaultIndexUsageTracking tracking;
    private long creationTime;

    @BeforeEach
    void setUp() {
        clock = Clocks.fakeClock();
        clock.forward(10, TimeUnit.SECONDS);
        creationTime = clock.millis();
        tracking = new DefaultIndexUsageTracking(clock);
    }

    @Test
    void shouldAddLocalStatisticsToParent() {
        for (int i = 0; i < 2; i++) {
            clock.forward(5, TimeUnit.SECONDS);
            tracking.queried();
            clock.forward(1, TimeUnit.SECONDS);
            tracking.queried();

            // then
            var stats = tracking.getAndReset();
            assertThat(stats.trackedSince()).isEqualTo(creationTime);
            assertThat(stats.readCount()).isEqualTo(2);
            assertThat(stats.lastRead()).isEqualTo(clock.millis());
        }
    }

    @Test
    void shouldAddLocalStatisticsToParentConcurrently() {
        // given
        var race = new Race();
        var numThreads = 4;
        var queriesPerThread = 100;
        race.addContestants(numThreads, () -> {
            var rng = ThreadLocalRandom.current();
            for (var i = 0; i < queriesPerThread; i++) {
                clock.forward(rng.nextInt(1, 100), TimeUnit.MILLISECONDS);
                tracking.queried();
            }
        });

        // when
        race.goUnchecked();

        // then
        var stats = tracking.getAndReset();
        assertThat(stats.readCount()).isEqualTo(numThreads * queriesPerThread);
        assertThat(stats.trackedSince()).isEqualTo(creationTime);
        assertThat(stats.lastRead()).isEqualTo(clock.millis());
    }
}
