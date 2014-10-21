package org.neo4j.kernel.impl.api.index;

import static org.junit.Assert.assertEquals;
import static org.neo4j.register.Register.DoubleLongRegister;

import org.junit.Test;
import org.neo4j.register.Registers;

public class SampleVisitorTest
{
    private final Object value = new Object();

    @Test
    public void shouldSampleNothing()
    {
        // given
        SampleVisitor sampler = new SampleVisitor( 10 );

        // when
        // nothing has been sampled

        // then
        assertSampledValues( sampler, 0, 0 );
    }

    @Test
    public void shouldSampleASingleValue()
    {
        // given
        SampleVisitor sampler = new SampleVisitor( 10 );

        // when
        sampler.include( value );

        // then
        assertSampledValues( sampler, 1, 1 );

    }

    @Test
    public void shouldSampleDuplicateValues()
    {
        // given
        SampleVisitor sampler = new SampleVisitor( 10 );

        // when
        sampler.include( value );
        sampler.include( value );
        sampler.include( new Object() );

        // then
        assertSampledValues( sampler, 2, 3 );
    }

    @Test
    public void shouldDivideTheSamplingInStepsNotBiggerThanBatchSize()
    {
        // given
        SampleVisitor sampler = new SampleVisitor( 1 );

        // when
        sampler.include( value );
        sampler.include( value );
        sampler.include( new Object() );

        // then
        assertSampledValues( sampler, 1, 1 );
    }

    @Test
    public void shouldExcludeValuesFromTheCurrentSampling()
    {
        // given
        SampleVisitor sampler = new SampleVisitor( 10 );
        sampler.include( value );
        sampler.include( value );
        sampler.include( new Object() );

        // when
        sampler.exclude( value );

        // then
        assertSampledValues( sampler, 2, 2 );
    }

    @Test
    public void shouldDoNothingWhenExcludingAValueInAnEmptySample()
    {
        // given
        SampleVisitor sampler = new SampleVisitor( 10 );

        // when
        sampler.exclude( value );
        sampler.include( value );

        // then
        assertSampledValues( sampler, 1, 1 );
    }

    private void assertSampledValues( SampleVisitor sampler, long expectedUniqueValues, long expectedSampledSize )
    {
        final DoubleLongRegister register = Registers.newDoubleLongRegister();
        sampler.result( register );
        assertEquals( expectedUniqueValues, register.readFirst() );
        assertEquals( expectedSampledSize, register.readSecond() );
    }
}
