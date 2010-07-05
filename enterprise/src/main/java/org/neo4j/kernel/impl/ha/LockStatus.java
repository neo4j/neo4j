package org.neo4j.kernel.impl.ha;

public enum LockStatus
{
    OK_LOCKED,
    NOT_LOCKED,
    DEADLOCK,
    FAILED;
}
