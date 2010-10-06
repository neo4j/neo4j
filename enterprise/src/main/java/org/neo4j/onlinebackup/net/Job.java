/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

public abstract class Job
{
    private boolean reQueue = true;
    
    private Callback callback = null;
    
    private Job chainJob = null;
    
    private enum Status implements JobStatus
    {
        NO_STATUS
    }
    
    private JobStatus status = Status.NO_STATUS;
    
    protected Job()
    {
        
    }
    
    protected Job( Callback callback )
    {
        this.callback = callback;
    }
    
    protected void setRequeue()
    {
        reQueue = true;
    }
    
    protected void setNoRequeue()
    {
        reQueue = false;
    }
    
    public boolean needsRequeue()
    {
        return reQueue;
    }
    
    public abstract boolean performJob();
    
    public void executeCallback()
    {
        if ( callback != null )
        {
            callback.jobExecuted( this );
        }
    }
    
    public void setChainJob( Job job )
    {
        this.chainJob = job;
    }
    
    public Job getChainJob()
    {
        return chainJob;
    }
    
    public JobStatus getStatus()
    {
        return status;
    }
    
    protected void setStatus( JobStatus status )
    {
        this.status = status;
    }
    
    protected void log( String message )
    {
        // System.out.println( getStatus() + " " + this + ": " + message );
    }

    protected void log( String message, Throwable cause )
    {
        System.out.println( this + ": " + message );
        cause.printStackTrace();
    }
}