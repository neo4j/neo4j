package org.neo4j.server.rrd;

import java.util.Timer;
import java.util.TimerTask;

public class ScheduledJob
{
    private Job job;
    private boolean running = true;
    public Timer timer;

    public ScheduledJob( Job job, int runEvery_seconds )
    {
        this.job = job;

        timer = new Timer( "rrd" );
        timer.scheduleAtFixedRate( runJob, 0, runEvery_seconds * 1000 );
    }

    private TimerTask runJob = new TimerTask()
    {
        public void run()
        {
            if ( !running )
            {
                this.cancel();
            } else
            {
                job.run();
            }
        }
    };

    public void runEvery_Seconds( int seconds )
    {
    }

    public void stop()
    {
        running = false;
        timer.cancel();
    }
}
