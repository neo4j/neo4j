package org.neo4j.server.rrd;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;

public class RrdJobTest {

	@Test
	public void testGuardsAgainstQuickRuns() throws Exception
	{
	
		RrdSampler sampler = mock(RrdSampler.class);
		TimeSource time = mock(TimeSource.class);
		stub(time.getTime())
			.toReturn(10000l).toReturn(10000l)  // First call (getTime gets called twice)
			.toReturn(10000l)                   // Second call
			.toReturn(12000l);                  // Third call
		
		RrdJob job = new RrdJob(sampler, time);
		
		job.run();
		job.run();
		job.run();
		
		verify(sampler,times(2)).updateSample();
		
	}

}
