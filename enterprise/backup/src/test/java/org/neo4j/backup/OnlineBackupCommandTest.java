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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OnlineBackupCommandTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    // Dependencies
    private OutsideWorld outsideWorld = mock( OutsideWorld.class );
    private BackupFlowFactory backupFlowFactory = mock( BackupFlowFactory.class );

    // Behaviour dependencies
    private FileSystemAbstraction fileSystemAbstraction = mock( FileSystemAbstraction.class );
    private BackupFlow backupFlow = mock( BackupFlow.class );

    // Parameters and helpers
    private final Config config = mock( Config.class );
    private final OnlineBackupRequiredArguments requiredArguments = mock( OnlineBackupRequiredArguments.class );
    private final ConsistencyFlags consistencyFlags = mock( ConsistencyFlags.class );
    private final OnlineBackupContext onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags );

    private Path backupDirectory = Paths.get( "backupDirectory/" );
    private Path reportDirectory = Paths.get( "reportDirectory/" );
    private AbstractBackupSupportingClassesFactory backupSupportingClassesFactory = mock( AbstractBackupSupportingClassesFactory.class );

    private OnlineBackupCommand subject;

    @Before
    public void setup() throws Exception
    {
        OnlineBackupContextLoader onlineBackupContextLoader = mock( OnlineBackupContextLoader.class );
        when( onlineBackupContextLoader.fromCommandLineArguments( any() ) ).thenReturn( onlineBackupContext );

        when( outsideWorld.fileSystem() ).thenReturn( fileSystemAbstraction );

        when( fileSystemAbstraction.isDirectory( backupDirectory.toFile() ) ).thenReturn( true );
        when( fileSystemAbstraction.isDirectory( reportDirectory.toFile() ) ).thenReturn( true );

        when( requiredArguments.getFolder() ).thenReturn( backupDirectory );
        when( requiredArguments.getReportDir() ).thenReturn( reportDirectory );
        when( requiredArguments.getName() ).thenReturn( "backup name" );
        when( backupFlowFactory.backupFlow( any(), any(), any() ) ).thenReturn( backupFlow );

        subject = new OnlineBackupCommand( outsideWorld, onlineBackupContextLoader, backupSupportingClassesFactory, backupFlowFactory );
    }

    @Test
    public void nonExistingBackupDirectoryRaisesException() throws CommandFailed, IncorrectUsage
    {
        // given backup directory is not a directory
        when( fileSystemAbstraction.isDirectory( backupDirectory.toFile() ) ).thenReturn( false );

        // then
        expected.expect( CommandFailed.class );
        expected.expectMessage( "Directory 'backupDirectory' does not exist." );

        // when
        execute();
    }

    @Test
    public void nonExistingReportDirectoryRaisesException() throws CommandFailed, IncorrectUsage
    {
        // given report directory is not a directory
        when( fileSystemAbstraction.isDirectory( reportDirectory.toFile() ) ).thenReturn( false );

        // then
        expected.expect( CommandFailed.class );
        expected.expectMessage( "Directory 'reportDirectory' does not exist." );

        // when
        execute();
    }

    @Test
    public void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new OnlineBackupCommandProvider(), ps::println );

            assertEquals(
                    format( "usage: neo4j-admin backup --backup-dir=<backup-path> --name=<graph.db-backup>%n" +
                            "                          [--from=<address>] [--fallback-to-full[=<true|false>]]%n" +
                            "                          [--timeout=<timeout>]%n" +
                            "                          [--check-consistency[=<true|false>]]%n" +
                            "                          [--cc-report-dir=<directory>]%n" +
                            "                          [--additional-config=<config-file-path>]%n" +
                            "                          [--cc-graph[=<true|false>]]%n" +
                            "                          [--cc-indexes[=<true|false>]]%n" +
                            "                          [--cc-label-scan-store[=<true|false>]]%n" +
                            "                          [--cc-property-owners[=<true|false>]]%n" +
                            "%n" +
                            "environment variables:%n" +
                            "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                            "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                            "    NEO4J_HOME    Neo4j home directory.%n" +
                            "    HEAP_SIZE     Set size of JVM heap during command execution.%n" +
                            "                  Takes a number and a unit, for example 512m.%n" +
                            "%n" +
                            "Perform an online backup from a running Neo4j enterprise server. Neo4j's backup%n" +
                            "service must have been configured on the server beforehand.%n" +
                            "%n" +
                            "All consistency checks except 'cc-graph' can be quite expensive so it may be%n" +
                            "useful to turn them off for very large databases. Increasing the heap size can%n" +
                            "also be a good idea. See 'neo4j-admin help' for details.%n" +
                            "%n" +
                            "For more information see:%n" +
                            "https://neo4j.com/docs/operations-manual/current/backup/%n" +
                            "%n" +
                            "options:%n" +
                            "  --backup-dir=<backup-path>               Directory to place backup in.%n" +
                            "  --name=<graph.db-backup>                 Name of backup. If a backup with this%n" +
                            "                                           name already exists an incremental%n" +
                            "                                           backup will be attempted.%n" +
                            "  --from=<address>                         Host and port of Neo4j.%n" +
                            "                                           [default:localhost:6362]%n" +
                            "  --fallback-to-full=<true|false>          If an incremental backup fails backup%n" +
                            "                                           will move the old backup to%n" +
                            "                                           <name>.err.<N> and fallback to a full%n" +
                            "                                           backup instead. [default:true]%n" +
                            "  --timeout=<timeout>                      Timeout in the form <time>[ms|s|m|h],%n" +
                            "                                           where the default unit is seconds.%n" +
                            "                                           [default:20m]%n" +
                            "  --check-consistency=<true|false>         If a consistency check should be%n" +
                            "                                           made. [default:true]%n" +
                            "  --cc-report-dir=<directory>              Directory where consistency report%n" +
                            "                                           will be written. [default:.]%n" +
                            "  --additional-config=<config-file-path>   Configuration file to supply%n" +
                            "                                           additional configuration in. This%n" +
                            "                                           argument is DEPRECATED. [default:]%n" +
                            "  --cc-graph=<true|false>                  Perform consistency checks between%n" +
                            "                                           nodes, relationships, properties,%n" +
                            "                                           types and tokens. [default:true]%n" +
                            "  --cc-indexes=<true|false>                Perform consistency checks on%n" +
                            "                                           indexes. [default:true]%n" +
                            "  --cc-label-scan-store=<true|false>       Perform consistency checks on the%n" +
                            "                                           label scan store. [default:true]%n" +
                            "  --cc-property-owners=<true|false>        Perform additional consistency checks%n" +
                            "                                           on property ownership. This check is%n" +
                            "                                           *very* expensive in time and memory.%n" +
                            "                                           [default:false]%n" ),
                    baos.toString() );
        }
    }

    private void execute() throws IncorrectUsage, CommandFailed
    {
        String[] implementationDoesNotUseArguments = new String[0];
        subject.execute( implementationDoesNotUseArguments );
    }
}
