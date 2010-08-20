package slavetest;

import org.junit.Test;

public class PerformanceTests
{
    @Test
    public void allocateIds() throws Exception
    {
        testJob( new CommonJobs.PerformanceIdAllocationJob( 1000000 ) );
    }

    @Test
    public void createFewNodesInManyTransactions() throws Exception
    {
        testJob( new CommonJobs.PerformanceCreateNodesJob( 10, 100 ) );
    }
    
    @Test
    public void createManyNodesInFewTransactions() throws Exception
    {
        testJob( new CommonJobs.PerformanceCreateNodesJob( 10, 1000 ) );
    }

    @Test
    public void grabLocks() throws Exception
    {
        testJob( new CommonJobs.PerformanceAcquireWriteLocksJob( 10000 ) );
    }

    public static void main( String[] args ) throws Exception
    {
        PerformanceTests perf = new PerformanceTests();
        perf.selective = true;
        for ( String test : args )
        {
            PerformanceTests.class.getDeclaredMethod( test ).invoke( perf );
        }
    }

    private boolean selective = false;

    private void testJob( Job<Void> job ) throws Exception
    {
        final boolean noHa = doTest( "NO_HA" );
        final boolean singleJvm = doTest( "SINGLE_JVM" );
        if ( noHa || singleJvm )
        {
            SingleJvmTesting single = new SingleJvmTesting();
            if ( noHa )
            {
                single.initializeDbs( 1 );
                time( "No HA", executeOnMaster( single, job ) );
                single.shutdownDbs();
            }
            if ( singleJvm )
            {
                single.initializeDbs( 1 );
                time( "Single JVM HA", executeOnSlave( single, job ) );
                single.shutdownDbs();
            }
        }

        if ( doTest( "MULTI_JVM" ) )
        {
            MultiJvmTesting multi = new MultiJvmTesting();
            multi.initializeDbs( 1 );
            time( "Multi JVM HA", executeOnSlave( multi, job ) );
            multi.shutdownDbs();
        }
        if ( doTest( "FULL_HA" ) )
        {
            MultiJvmWithZooKeeperTesting multiZoo = new MultiJvmWithZooKeeperTesting();
            MultiJvmWithZooKeeperTesting.startZooKeeperCluster();
            multiZoo.initializeDbs( 1 );
            time( "Multi JVM HA with ZooKeeper", executeOnSlave( multiZoo, job ) );
            multiZoo.shutdownDbs();
            MultiJvmWithZooKeeperTesting.shutdownZooKeeperCluster();
        }
    }

    private boolean doTest( String testName )
    {
        if ( selective )
        {
            return Boolean.getBoolean( "org.neo4j.ha.test." + testName );
        }
        else
        {
            return true;
        }
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
