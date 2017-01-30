package org.neo4j.backup;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Predicate;

import org.neo4j.function.Predicates;
import org.neo4j.helper.IsChannelClosedException;
import org.neo4j.helper.IsConnectionException;
import org.neo4j.helper.IsConnectionRestByPeer;
import org.neo4j.helper.IsStoreClosed;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.configuration.Config;

public class BackupHelper
{

    private static final Predicate<Throwable> isTransientError = Predicates.any(
                    new IsConnectionException(),
                    new IsConnectionRestByPeer(),
                    new IsChannelClosedException(),
                    new IsStoreClosed() );

    public static BackupResult backup( String host, int port, File targetDirectory )
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        boolean consistent = true;
        boolean transientFailure = false;
        boolean failure = false;
        try
        {
            BackupService backupService = new BackupService(outputStream);
            BackupService.BackupOutcome backupOutcome = backupService.doIncrementalBackupOrFallbackToFull( host, port,
                    targetDirectory, ConsistencyCheck.FULL, Config.embeddedDefaults(), BackupClient.BIG_READ_TIMEOUT,
                    false );
            consistent = backupOutcome.isConsistent();
        }
        catch ( Throwable t )
        {
            if ( isTransientError.test( t ) )
            {
                transientFailure = true;
            }
            else
            {
                failure = true;
                throw t;
            }
        }
        finally
        {
            if ( !consistent || failure )
            {
                flushToStandardOutput( outputStream );
            }
            IOUtils.closeAllSilently( outputStream );
        }
        return new BackupResult( consistent, transientFailure );
    }

    private static void flushToStandardOutput( ByteArrayOutputStream outputStream )
    {
        try
        {
            outputStream.writeTo( System.out );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
