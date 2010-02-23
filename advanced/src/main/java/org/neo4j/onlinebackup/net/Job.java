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