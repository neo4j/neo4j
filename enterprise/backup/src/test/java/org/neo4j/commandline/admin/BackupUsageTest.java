/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.commandline.admin;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.backup.impl.BackupHelpOutput;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class BackupUsageTest
{
    private static final Path HERE = Paths.get( "." );

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    private final CommandLocator commandLocator = CommandLocator.fromServiceLocator();

    @Test
    public void outputMatchesExpectedForMissingBackupDir()
    {
        // when
        String output = runBackup();

        // then
        String reason = "Missing argument 'backup-dir'";
        assertThat( output, containsString( reason ) );

        // and
        for ( String line : BackupHelpOutput.BACKUP_OUTPUT_LINES )
        {
            assertThat( output, containsString( line ) );
        }
    }

    @Test
    public void missingBackupName()
    {
        // when
        String output = runBackup( "--backup-dir=target" );

        // then
        String reason = "Missing argument 'name'";
        assertThat( output, containsString( reason ) );

        // and
        for ( String line : BackupHelpOutput.BACKUP_OUTPUT_LINES )
        {
            assertThat( "Failed for line: " + line, output, containsString( line ) );
        }
    }

    @Test
    public void incorrectBackupDirectory() throws IOException
    {
        // when
        Path backupDirectoryResolved = HERE.toRealPath().resolve( "non_existing_dir" );
        String output = runBackup( "--backup-dir=non_existing_dir", "--name=mybackup" );

        // then
        String reason = String.format( "command failed: Directory '%s' does not exist.", backupDirectoryResolved );
        assertThat( output, containsString( reason ) );
    }

    private String runBackup( String... args )
    {
        return runBackup( false, args );
    }

    private String runBackup( boolean debug, String... args )
    {
        ParameterisedOutsideWorld outsideWorld = // ParameterisedOutsideWorld used for suppressing #close() doing System.exit()
                new ParameterisedOutsideWorld( System.console(), System.out, System.err, System.in, new DefaultFileSystemAbstraction() );
        AdminTool subject = new AdminTool( commandLocator, cmd -> new ArrayList<>(), outsideWorld, debug );
        Path homeDir = HERE;
        Path configDir = HERE;
        List<String> params = new ArrayList();
        params.add( "backup" );
        params.addAll( Arrays.asList( args ) );
        String[] argArray = params.toArray( new String[params.size()] );
        subject.execute( homeDir, configDir, argArray );

        return suppressOutput.getErrorVoice().toString() + System.lineSeparator() + suppressOutput.getOutputVoice().toString();
    }
}
