package org.neo4j.server.rrd;

public class RrdJob implements Job
{
    private RrdSampler sampler;

    public RrdJob( RrdSampler sampler )
    {
        this.sampler = sampler;
    }

    public void run()
    {
        sampler.updateSample();
    }
}
