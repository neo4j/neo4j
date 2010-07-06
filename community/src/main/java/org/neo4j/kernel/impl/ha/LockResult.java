package org.neo4j.kernel.impl.ha;

public class LockResult
{
    private final LockStatus status;
    private final String deadlockMessage;
    
    public LockResult( LockStatus status )
    {
        this.status = status;
        this.deadlockMessage = null;
    }
    
    public LockResult( String deadlockMessage )
    {
        this.status = LockStatus.DEAD_LOCKED;
        this.deadlockMessage = deadlockMessage;
    }

    public LockStatus getStatus()
    {
        return status;
    }

    public String getDeadlockMessage()
    {
        return deadlockMessage;
    }
}
