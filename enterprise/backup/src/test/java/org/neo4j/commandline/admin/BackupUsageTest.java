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
package org.neo4j.commandline.admin;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.backup.impl.BackupHelpOutput;
import org.neo4j.backup.impl.ParameterisedOutsideWorld;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class BackupUsageTest
{
    private static final Path HERE = Paths.get( "." );

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    private final CommandLocator commandLocator = CommandLocator.fromServiceLocator();

    @Test
    public void outputMatchesExpectedForMissingBackupDir() throws UnsupportedEncodingException
    {
        // when
        String output = runBackup();

        // then
        String reason = "Missing argument 'backup-dir'";
        assertThat( output, containsString( reason ) );

        // and
        assertThat( output, containsString( BackupHelpOutput.BACKUP_OUTPUT ) );
    }

    @Test
    public void missingBackupName() throws UnsupportedEncodingException
    {
        // when
        String output = runBackup( "--backup-dir=target" );

        // then
        String reason = "Missing argument 'name'";
        assertThat( output, containsString( reason ) );

        // and
        assertThat( output, containsString( BackupHelpOutput.BACKUP_OUTPUT ) );
    }

    @Test
    public void incorrectBackupDirectory() throws IOException
    {
        // when
        Path backupDirectoryResolved = HERE.toRealPath().resolve( "non_existing_dir" );
        String output = runBackup( "--backup-dir=non_existing_dir", "--name=mybackup" );

        // then
        String reason = String.format( "command failed: Directory '%s' does not exist.\n", backupDirectoryResolved );
        assertThat( output, containsString( reason ) );
        assertThat( output, not( containsString( BackupHelpOutput.BACKUP_OUTPUT ) ) );
    }

    private String runBackup( String... args ) throws UnsupportedEncodingException
    {
        return runBackup( false, args );
    }

    private String runBackup( boolean debug, String... args ) throws UnsupportedEncodingException
    {
        ParameterisedOutsideWorld outsideWorld = // ParameterisedOutsideWorld used for suppressing #close() doing System.exit()
                new ParameterisedOutsideWorld( System.console(), System.out, System.err, System.in, new DefaultFileSystemAbstraction() );
        AdminTool subject = new AdminTool( commandLocator, cmd -> new ArrayList<>(  ), outsideWorld, debug );
        Path homeDir = HERE;
        Path configDir = HERE;
        List<String> params = new ArrayList();
        params.add( "backup" );
        params.addAll( Arrays.asList( args ) );
        String[] argArray = params.toArray( new String[params.size()] );
        subject.execute( homeDir, configDir, argArray );
        return suppressOutput.getErrorVoice().toString() + suppressOutput.getOutputVoice().toString();
    }
}
