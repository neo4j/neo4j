/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
