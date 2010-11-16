package org.neo4j.server.rrd;

public interface JobScheduler
{
    void scheduleToRunEvery_Seconds( Job job, int runEverySeconds );
}
