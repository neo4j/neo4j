/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.backup.stresstests;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.neo4j.backup.BackupServiceStressTestingBuilder;
import org.neo4j.io.fs.FileUtils;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.BackupServiceStressTestingBuilder.untilTimeExpired;

/**
 * Notice the class name: this is _not_ going to be run as part of the main build.
 */
public class BackupServiceStressTesting
{
    private static final String DEFAULT_DURATION_IN_MINUTES = "1";
    private static final String DEFAULT_WORKING_DIR = new File( getProperty( "java.io.tmpdir" ) ).getPath();
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final String DEFAULT_PORT = "8200";

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        long durationInMinutes = parseLong( fromEnv( "BACKUP_SERVICE_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        String directory = fromEnv( "BACKUP_SERVICE_STRESS_WORKING_DIRECTORY", DEFAULT_WORKING_DIR );
        String backupHostname = fromEnv( "BACKUP_SERVICE_STRESS_BACKUP_HOSTNAME", DEFAULT_HOSTNAME );
        int backupPort = parseInt( fromEnv( "BACKUP_SERVICE_STRESS_BACKUP_PORT", DEFAULT_PORT ) );

        File storeDirectory = new File( directory, "store" );
        File workDirectory = new File( directory, "work" );
        Callable<Integer> callable = new BackupServiceStressTestingBuilder()
                .until( untilTimeExpired( durationInMinutes, MINUTES ) )
                .withStore( ensureExists( storeDirectory ) )
                .withBackupDirectory( ensureExists( workDirectory ) )
                .withBackupAddress( backupHostname, backupPort )
                .build();

        int brokenStores = callable.call();

        assertEquals( 0, brokenStores );

        FileUtils.deleteRecursively( storeDirectory );
        FileUtils.deleteRecursively( workDirectory );
    }

    private static File ensureExists( File directory ) throws IOException
    {
        FileUtils.deleteRecursively( directory );
        if ( !directory.mkdirs() )
        {
            throw new IOException( "Unable to create directory: '" + directory.getAbsolutePath() + "'" );
        }
        return directory;
    }

    private static String fromEnv( String environmentVariableName, String defaultValue )
    {
        String environmentVariableValue = getenv( environmentVariableName );
        return environmentVariableValue == null ? defaultValue : environmentVariableValue;
    }
}
