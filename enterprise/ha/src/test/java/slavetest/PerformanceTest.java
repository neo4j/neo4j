/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package slavetest;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class PerformanceTest
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
        PerformanceTest perf = new PerformanceTest();
        perf.selective = true;
        for ( String test : args )
        {
            PerformanceTest.class.getDeclaredMethod( test ).invoke( perf );
        }
    }

    private boolean selective = false;

    private void testJob( Job<Void> job ) throws Exception
    {
        final boolean noHa = doTest( "NO_HA" );
        final boolean singleJvm = doTest( "SINGLE_JVM" );
        if ( noHa || singleJvm )
        {
            AbstractHaTest single = new SingleJvmTest();
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
            MultiJvmTest multi = new MultiJvmTest();
            multi.initializeDbs( 1 );
            time( "Multi JVM HA", executeOnSlave( multi, job ) );
            multi.shutdownDbs();
        }
        if ( doTest( "FULL_HA" ) )
        {
            MultiJvmWithZooKeeperTest multiZoo = new MultiJvmWithZooKeeperTest();
            multiZoo.startZooKeeperCluster();
            multiZoo.initializeDbs( 1 );
            time( "Multi JVM HA with ZooKeeper", executeOnSlave( multiZoo, job ) );
            multiZoo.shutdownDbs();
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
