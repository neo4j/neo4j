package org.neo4j.server.rrd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;


import java.io.IOException;

import javax.management.MalformedObjectNameException;

import org.junit.Before;
import org.junit.Test;

public class MemoryUsedSampleableTest {

	MemoryUsedSampleable sampleable;
	
	@Before
    public void setUp() throws Exception
    {
        sampleable = new MemoryUsedSampleable();
    }
	
	@Test
    public void memoryUsageIsBetweenZeroAndOneHundred() throws IOException, MalformedObjectNameException
    {
        assertThat( sampleable.getValue(), greaterThan( 0l ) );
        assertThat( sampleable.getValue(), lessThan( 100l ) );
    }
	
}
