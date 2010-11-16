package org.neo4j.server.rrd;

import org.junit.Test;
import org.rrd4j.core.Sample;

import javax.management.MalformedObjectNameException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RrdSamplerTest
{
    @Test
    public void canSampleADatabase() throws MalformedObjectNameException
    {
        Sampleable testSamplable = new TestSamplable( "myTest", 15 );

        Sample sample = mock( Sample.class );

        RrdSampler sampler = new RrdSampler( sample, testSamplable );
        sampler.updateSample();

        verify( sample ).setValue( "myTest", 15 );
    }

    private class TestSamplable implements Sampleable
    {
        private String name;
        private long value;

        private TestSamplable( String name, long value )
        {
            this.name = name;
            this.value = value;
        }

        public String getName()
        {
            return name;
        }

        public long getValue()
        {
            return value;
        }
    }
}
