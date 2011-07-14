package org.neo4j.server.rrd;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;

public class RrdJobTest {

	@Test
	public void testGuardsAgainstQuickRuns() throws Exception
	{
	
		RrdSampler sampler = mock(RrdSampler.class);
		
		RrdJob job = new RrdJob(sampler);
		
		job.run();
		job.run();
		job.run();
		
		Thread.sleep(1001);
		
		job.run();
		
		verify(sampler,times(2)).updateSample();
		
	}

}
