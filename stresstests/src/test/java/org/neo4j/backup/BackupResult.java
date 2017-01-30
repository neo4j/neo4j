package org.neo4j.backup;

public class BackupResult
{
    private final boolean consistent;
    private final boolean transientErrorOnBackup;

    public BackupResult( boolean consistent, boolean transientErrorOnBackup )
    {
        this.consistent = consistent;
        this.transientErrorOnBackup = transientErrorOnBackup;
    }

    public boolean isConsistent()
    {
        return consistent;
    }

    public boolean isTransientErrorOnBackup()
    {
        return transientErrorOnBackup;
    }
}
