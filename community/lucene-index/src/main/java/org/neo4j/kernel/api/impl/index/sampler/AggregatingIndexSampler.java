/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.impl.index.sampler;

import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.storageengine.api.schema.IndexSampler;

/**
 * Index sampler implementation that provide total sampling result of multiple provided samples, by aggregating their
 * internal independent samples.
 */
public class AggregatingIndexSampler implements IndexSampler
{
    private List<IndexSampler> indexSamplers;

    public AggregatingIndexSampler( List<IndexSampler> indexSamplers )
    {
        this.indexSamplers = indexSamplers;
    }

    @Override
    public long sampleIndex( Register.DoubleLong.Out result ) throws IndexNotFoundKernelException
    {
        SampleResult sampleResult = indexSamplers.parallelStream()
                                    .map( this::sampleIndex )
                                    .reduce( SampleResult::combine ).get();
        sampleResult.getRegister().copyTo( result );
        return sampleResult.sample;
    }

    private SampleResult sampleIndex( IndexSampler sampler)
    {
        try
        {
            Register.DoubleLongRegister register = Registers.newDoubleLongRegister();
            long sample = sampler.sampleIndex( register );
            return new SampleResult( register, sample );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static class SampleResult
    {
        private Register.DoubleLongRegister register;
        private long sample;

        /**
         * Combine two sample results into one by sum their corresponding values.
         * @param sampleResult1 first sample to combine
         * @param sampleResult2 second sample to combine
         * @return new result sample that will represent sum of first and second sample
         */
        public static SampleResult combine(SampleResult sampleResult1, SampleResult sampleResult2)
        {
            Register.DoubleLongRegister register1 = sampleResult1.getRegister();
            Register.DoubleLongRegister register2 = sampleResult2.getRegister();
            Register.DoubleLongRegister register = Registers.newDoubleLongRegister(
                    add( register1.readFirst(), register2.readFirst() ),
                    add( register1.readSecond(), register2.readSecond() ) );
            long sample = add( sampleResult1.getSample(), sampleResult2.getSample() );
            return new SampleResult( register, sample );
        }

        private static long add( long sample, long sample2 )
        {
            return Math.addExact( sample, sample2 );
        }

        SampleResult( Register.DoubleLongRegister register, long sample )
        {
            this.register = register;
            this.sample = sample;
        }

        public Register.DoubleLongRegister getRegister()
        {
            return register;
        }

        public long getSample()
        {
            return sample;
        }
    }
}
