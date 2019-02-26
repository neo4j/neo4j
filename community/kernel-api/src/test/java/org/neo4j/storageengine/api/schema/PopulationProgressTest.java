/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api.schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( RandomExtension.class )
class PopulationProgressTest
{
    @Inject
    protected RandomRule random;

    @Test
    void shouldCalculateProgressOfSingle()
    {
        // given
        PopulationProgress populationProgress = PopulationProgress.single( 50, 100 );

        // when
        float progress = populationProgress.getProgress();

        // then
        assertEquals( 0.5f, progress );
    }

    @Test
    void shouldCalculateProgressOfMultipleEquallyWeightedProgresses()
    {
        // given
        PopulationProgress part1 = PopulationProgress.single( 1, 1 );
        PopulationProgress part2 = PopulationProgress.single( 4, 10 );
        PopulationProgress multi = PopulationProgress.multiple().add( part1, 1 ).add( part2, 1 ).build();

        // when
        float progress = multi.getProgress();

        // then
        assertEquals( 0.5f + 0.2f, progress );
    }

    @Test
    void shouldCalculateProgressOfMultipleDifferentlyWeightedProgresses()
    {
        // given
        PopulationProgress part1 = PopulationProgress.single( 1, 3 );
        PopulationProgress part2 = PopulationProgress.single( 4, 10 );
        PopulationProgress multi = PopulationProgress.multiple().add( part1, 3 ).add( part2, 1 ).build();

        // when
        float progress = multi.getProgress();

        // then
        assertEquals( ((1f / 3f) * (3f / 4f)) + ((4f / 10) * (1f / 4f)), progress );
    }

    @Test
    void shouldAlwaysResultInFullyCompleted()
    {
        // given
        int partCount = random.nextInt( 5, 10 );
        PopulationProgress.MultiBuilder builder = PopulationProgress.multiple();
        for ( int i = 0; i < partCount; i++ )
        {
            long total = random.nextLong( 10_000_000 );
            builder.add( PopulationProgress.single( total, total ), random.nextFloat() * random.nextInt( 1, 10 ) );
        }
        PopulationProgress populationProgress = builder.build();

        // when
        float progress = populationProgress.getProgress();

        // then
        assertEquals( 1f, progress );
    }
}
