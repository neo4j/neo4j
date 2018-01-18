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

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.backup.impl.BackupHelpOutput;
import org.neo4j.backup.impl.OnlineBackupCommandProvider;
import org.neo4j.backup.impl.ParametrisedOutsideWorld;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;

import static org.junit.Assert.assertEquals;

public class BackupUsageTest
{
    OnlineBackupCommandProvider onlineBackupCommandProvider = new OnlineBackupCommandProvider();
    CommandLocator commandLocator = AugmentedCommandLocator.fromFixedArray( onlineBackupCommandProvider );

    @Test
    public void outputMatchesExpectedForMissingBackupDir() throws UnsupportedEncodingException
    {
        // when
        String output = runBackup();

        // then
        String reason = "Missing argument 'backup-dir'\n\n";
        assertEquals( reason + BackupHelpOutput.BACKUP_OUTPUT, output );
    }

    @Test
    public void missingBackupName() throws UnsupportedEncodingException
    {
        // when
        String output = runBackup( "--backup-dir=target" );

        // then
        String reason = "Missing argument 'name'\n\n";
        assertEquals( reason + BackupHelpOutput.BACKUP_OUTPUT, output );
    }

    @Test
    public void incorrectBackupDirectory() throws IOException
    {
        // when
        Path backupDirectoryResolved = new File( "." ).toPath().toRealPath().resolve( "non_existing_dir" );
        String output = runBackup( "--backup-dir=non_existing_dir", "--name=mybackup" );

        // then
        String reason = String.format( "command failed: Directory '%s' does not exist.\n", backupDirectoryResolved.toFile().toString() );
        assertEquals( reason, output );
    }

    private String runBackup( String... args ) throws UnsupportedEncodingException
    {
        return runBackup( false, args );
    }

    private String runBackup( boolean debug, String... args ) throws UnsupportedEncodingException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream( baos, true, "utf-8" );
        ParametrisedOutsideWorld outsideWorld = capturableOutputOutsideWorld( printStream );
        AdminTool subject = new AdminTool( commandLocator, AugmentedBlockerLocator.fromList(), outsideWorld, debug );
        Path homeDir = new File( "." ).toPath();
        Path configDir = new File( "." ).toPath();
        List<String> params = new ArrayList();
        params.add( "backup" );
        params.addAll( Arrays.asList( args ) );
        String[] argArray = params.toArray( new String[params.size()] );
        subject.execute( homeDir, configDir, argArray );
        return baos.toString();
    }

    private ParametrisedOutsideWorld capturableOutputOutsideWorld( PrintStream printStream )
    {
        Console console = System.console();
        FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
        return new ParametrisedOutsideWorld( console, printStream, printStream, System.in, fileSystemAbstraction );
    }
}
