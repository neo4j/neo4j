package org.neo4j.kernel.impl.ha;

/**
 * Represents the status from a slave request to commit a transaction on
 * the master.
 */
public enum TransactionStatus
{
    /**
     * The master did what it was supposed to do, i.e. if the slave requested a
     * commit and it was successful or the slave requested a rollback and it
     * was successful.
     */
    SUCCESSFUL,
    
    /**
     * The master failed in doing what it was supposed to do, i.e. if the slave
     * requested a commit or rollback and it failed in doing so.
     */
    FAILED
}
