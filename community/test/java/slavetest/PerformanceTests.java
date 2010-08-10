package slavetest;

import org.junit.Test;

public class PerformanceTests
{
    @Test
    public void allocateIds() throws Exception
    {
        testJob( new CommonJobs.PerformanceIdAllocationJob( 100000 ) );
    }
    
    private void testJob( Job<Void> job ) throws Exception
    {
        SingleJvmTesting single = new SingleJvmTesting();
        single.initializeDbs( 1 );
        time( "No HA", executeOnMaster( single, job ) );
        time( "Single JVM HA", executeOnSlave( single, job ) );
        single.shutdownDbs();
        
        MultiJvmTesting multi = new MultiJvmTesting();
        multi.initializeDbs( 1 );
        time( "Multi JVM HA", executeOnSlave( multi, job ) );
        multi.shutdownDbs();
    }

    private Runnable executeOnSlave( final AbstractHaTest test, final Job<Void> job )
    {
        return new Runnable()
        {
            public void run()
            {
                try
                {
                    test.executeJob( job, 0 );
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
        };
    }

    private Runnable executeOnMaster( final AbstractHaTest test, final Job<Void> job )
    {
        return new Runnable()
        {
            public void run()
            {
                try
                {
                    test.executeJobOnMaster( job );
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
        };
    }

    protected void time( String message, Runnable runnable )
    {
        long t = System.currentTimeMillis();
        try
        {
            runnable.run();
        }
        catch ( RuntimeException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
        long time = System.currentTimeMillis() - t;
        System.out.println( message + ": " + time + "ms" );
    }
}
