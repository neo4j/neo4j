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
package org.neo4j.storageengine.api.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.kernel.api.PopulationProgress.multiple;
import static org.neo4j.internal.kernel.api.PopulationProgress.single;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;

@RandomSupportExtension
class PopulationProgressTest {
    @Inject
    protected RandomSupport random;

    @Test
    void shouldCalculateProgressOfSingle() {
        // given
        PopulationProgress populationProgress = single(50, 100);

        // when
        float progress = populationProgress.getProgress();

        // then
        assertThat(progress).isEqualTo(0.5f);
    }

    @Test
    void shouldCalculateProgressOfMultipleEquallyWeightedProgresses() {
        // given
        PopulationProgress part1 = single(1, 1);
        PopulationProgress part2 = single(4, 10);
        PopulationProgress multi = multiple().add(part1, 1).add(part2, 1).build();

        // when
        float progress = multi.getProgress();

        // then
        assertThat(progress).isEqualTo(0.5f + 0.2f);
    }

    @Test
    void shouldCalculateProgressOfMultipleDifferentlyWeightedProgresses() {
        // given
        PopulationProgress part1 = single(1, 3);
        PopulationProgress part2 = single(4, 10);
        PopulationProgress multi = multiple().add(part1, 3).add(part2, 1).build();

        // when
        float progress = multi.getProgress();

        // then
        assertThat(progress).isEqualTo(((1f / 3f) * (3f / 4f)) + ((4f / 10) * (1f / 4f)));
    }

    @Test
    void shouldAlwaysResultInFullyCompleted() {
        // given
        int partCount = random.nextInt(5, 10);
        PopulationProgress.MultiBuilder builder = multiple();
        for (int i = 0; i < partCount; i++) {
            long total = random.nextLong(10_000_000);
            builder.add(single(total, total), random.nextFloat() * random.nextInt(1, 10));
        }
        PopulationProgress populationProgress = builder.build();

        // when
        float progress = populationProgress.getProgress();

        // then
        assertThat(progress).isEqualTo(1f);
    }

    @Test
    void shouldCalculateProgressForNestedMultipleParts() {
        // given
        PopulationProgress multiPart1 =
                multiple().add(single(1, 1), 1).add(single(1, 5), 1).build(); // should result in 60%
        assertThat(multiPart1.getProgress()).isEqualTo(0.6f);
        PopulationProgress multiPart2 =
                multiple().add(single(6, 10), 1).add(single(1, 5), 1).build(); // should result in 40%
        assertThat(multiPart2.getProgress()).isEqualTo(0.4f);

        // when
        PopulationProgress.MultiBuilder builder = multiple();
        PopulationProgress all = builder.add(multiPart1, 1).add(multiPart2, 1).build();

        // then
        assertThat(all.getProgress()).isEqualTo(0.5f);
    }
}
