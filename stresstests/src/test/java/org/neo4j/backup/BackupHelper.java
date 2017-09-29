/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

    private BackupHelper()
    {
    }

    public static BackupResult backup( String host, int port, File targetDirectory )
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        boolean consistent = true;
        boolean transientFailure = false;
        boolean failure = false;
        try
        {
            BackupProtocolService backupProtocolService = new BackupProtocolService( outputStream );
            BackupOutcome backupOutcome = backupProtocolService.doIncrementalBackupOrFallbackToFull( host, port,
                    targetDirectory, ConsistencyCheck.FULL, Config.defaults(), BackupClient.BIG_READ_TIMEOUT,
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
