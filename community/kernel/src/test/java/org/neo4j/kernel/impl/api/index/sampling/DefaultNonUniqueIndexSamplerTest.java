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
package org.neo4j.kernel.impl.api.index.sampling;

import org.junit.Test;

import org.neo4j.storageengine.api.schema.IndexSample;

import static org.junit.Assert.assertEquals;

public class DefaultNonUniqueIndexSamplerTest
{
    private final String value = "aaa";

    @Test
    public void shouldSampleNothing()
    {
        // given
        NonUniqueIndexSampler sampler = new DefaultNonUniqueIndexSampler( 10 );

        // when
        // nothing has been sampled

        // then
        assertSampledValues( sampler, 0, 0, 0 );
    }

    @Test
    public void shouldSampleASingleValue()
    {
        // given
        NonUniqueIndexSampler sampler = new DefaultNonUniqueIndexSampler( 10 );

        // when
        sampler.include( value, 2 );

        // then
        assertSampledValues( sampler, 2, 1, 2 );
    }

    @Test
    public void shouldSampleDuplicateValues()
    {
        // given
        NonUniqueIndexSampler sampler = new DefaultNonUniqueIndexSampler( 10 );

        // when
        sampler.include( value, 5 );
        sampler.include( value, 4 );
        sampler.include( "bbb", 3 );

        // then
        assertSampledValues( sampler, 12, 2, 12 );
    }

    @Test
    public void shouldDivideTheSamplingInStepsNotBiggerThanBatchSize()
    {
        // given
        NonUniqueIndexSampler sampler = new DefaultNonUniqueIndexSampler( 1 );

        // when
        sampler.include( value, 5 );
        sampler.include( value, 4 );
        sampler.include( "bbb", 3 );

        // then
        int expectedSampledSize = /* index size */ 12  / /* steps */ 3;
        assertSampledValues( sampler, 12, 1, expectedSampledSize );
    }

    @Test
    public void shouldExcludeValuesFromTheCurrentSampling1()
    {
        // given
        NonUniqueIndexSampler sampler = new DefaultNonUniqueIndexSampler( 10 );
        sampler.include( value, 5  );
        sampler.include( value, 4  );
        sampler.include( "bbb", 3  );

        // when
        sampler.exclude( value, 3 );

        // then
        assertSampledValues( sampler, 9, 2, 9 );
    }

    @Test
    public void shouldExcludeValuesFromTheCurrentSampling2()
    {
        // given
        NonUniqueIndexSampler sampler = new DefaultNonUniqueIndexSampler( 10 );
        sampler.include( value, 1  );
        sampler.include( value, 4  );
        sampler.include( "bbb", 1  );

        // when
        sampler.exclude( value, 4 );

        // then
        assertSampledValues( sampler, 2, 2, 2 );
    }

    @Test
    public void shouldDoNothingWhenExcludingAValueInAnEmptySample()
    {
        // given
        NonUniqueIndexSampler sampler = new DefaultNonUniqueIndexSampler( 10 );

        // when
        sampler.exclude( value, 1 );
        sampler.include( value, 1 );

        // then
        assertSampledValues( sampler, 1, 1, 1 );
    }

    private void assertSampledValues( NonUniqueIndexSampler sampler, long expectedIndexSize, long expectedUniqueValues,
            long expectedSampledSize )
    {
        IndexSample sample = sampler.result();
        assertEquals( expectedIndexSize, sample.indexSize() );
        assertEquals( expectedUniqueValues, sample.uniqueValues() );
        assertEquals( expectedSampledSize, sample.sampleSize() );
    }
}
