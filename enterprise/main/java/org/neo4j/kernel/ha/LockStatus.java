package org.neo4j.kernel.ha;

public enum LockStatus
{
    OK_LOCKED,
    NOT_LOCKED,
    DEAD_LOCKED
    {
        @Override
        public boolean hasMessage()
        {
            return true;
        }
    };
    
    public boolean hasMessage()
    {
        return false;
    }
}
