package org.neo4j.onlinebackup.net;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class JobEater extends Thread
{
    private final Queue<Job> queue = new ConcurrentLinkedQueue<Job>();
    
    private List<Job> slowList = new LinkedList<Job>();
    
    private volatile boolean run = true;
    
    public boolean addJob( Job job )
    {
        return queue.offer( job );
    }
    
    public void run()
    {
        int count = 0;
        while ( run )
        {
            Job job = queue.poll();
            if ( job != null && count < 200 )
            {
                count++;
                try
                {
                    boolean progressing = job.performJob();
                    if ( job.needsRequeue() ) 
                    {
                        if ( progressing ) 
                        {
                            boolean success = queue.offer( job );
                            assert success;
                        }
                        else
                        {
                            slowList.add( job );
                        }
                    }
                    else
                    {
                        job.executeCallback();
                        Job chainJob = job.getChainJob();
                        if ( chainJob != null )
                        {
                            boolean success = queue.offer( chainJob );
                            assert success;
                        }
                    }
                }
                catch ( Throwable t )
                {
                    System.out.println( "Throwing away " + job );
                    t.printStackTrace();
                    try
                    {
                        job.setNoRequeue();
                        job.executeCallback();
                    }
                    catch ( Throwable tt )
                    {
                        tt.printStackTrace();
                    }
                    if ( job instanceof ConnectionJob )
                    {
                        ((ConnectionJob) job).close();
                    }
                }
            }
            else
            {
                if ( count < 200 )
                {
                    synchronized( this )
                    {
                        try
                        {
                            this.wait( 250 );
                        }
                        catch ( InterruptedException e )
                        {
                            interrupted();
                        }
                    }
                }
                count = 0;  
                List<Job> newSlowList = new LinkedList<Job>();
                for ( Job slowJob : slowList )
                {
                    try
                    {
                        boolean progressing = slowJob.performJob();
                        if ( slowJob.needsRequeue() ) 
                        {
                            if ( progressing )
                            {
                                boolean success = queue.offer( slowJob );
                                assert success;
                            }
                            else
                            {
                                newSlowList.add( slowJob );
                            }
                        }
                        else
                        {
                            slowJob.executeCallback();
                            Job chainJob = slowJob.getChainJob();
                            if ( chainJob != null )
                            {
                                boolean success = queue.offer( chainJob );
                                assert success;
                            }
                        }
                    }
                    catch ( Throwable t )
                    {
                        System.out.println( "Throwing away " + slowJob );
                        t.printStackTrace();
                        try
                        {
                            slowJob.setNoRequeue();
                            slowJob.executeCallback();
                        }
                        catch ( Throwable tt )
                        {
                            tt.printStackTrace();
                        }
                        if ( job instanceof ConnectionJob )
                        {
                            ((ConnectionJob) job).close();
                        }
                    }
                }
                slowList = newSlowList;
            }
        }
    }
    
    public void stopEating()
    {
        // will stop eating in near future
        run = false;
    }
}
