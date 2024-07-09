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
package org.neo4j.internal.batchimport.staging;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang3.mutable.MutableInt;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class WeightedExternalProgressReporterTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldProgressThroughDifferentlyWeightedLocalProgresses() {
        // given
        var percentageCompleted = new MutableInt();
        var monitor = new Monitor() {
            @Override
            public void percentageCompleted(int percentage) {
                assertThat(percentage).isBetween(0, 100);
                assertThat(percentage).isGreaterThanOrEqualTo(percentageCompleted.intValue());
                percentageCompleted.setValue(percentage);
            }
        };
        var reporter = new WeightedExternalProgressReporter(monitor);

        // when
        int numParts = random.nextInt(1, 10);
        double totalWeight = 0;
        for (int i = 0; i < numParts; i++) {
            double remainingWeight = 1 - totalWeight;
            double weight = i == numParts - 1 ? remainingWeight : random.nextDouble() * remainingWeight * 0.8;
            var listener = reporter.next(weight);
            totalWeight += weight;

            long total = random.nextLong(100, 1_000);
            long progress = 0;
            while (progress < total) {
                int stride = random.nextInt(1, 10);
                progress = Math.min(total, progress + stride);
                listener.update(progress, total);
            }
        }

        // then
        assertThat(percentageCompleted.intValue()).isCloseTo(100, Percentage.withPercentage(10));
        reporter.close();
        assertThat(percentageCompleted.intValue()).isEqualTo(100);
    }
}
