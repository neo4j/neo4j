/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.commandline.dbms;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.internal.StoreLocker.STORE_LOCK_FILENAME;

public class UnbindFromClusterCommandTest
{

    private final TestDirectory testDir = TestDirectory.testDirectory();
    private final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( testDir );

    @Test
    public void shouldFailIfSpecifiedDatabaseDoesNotExist() throws Exception
    {
        // given
        FileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        UnbindFromClusterCommand command =
                new UnbindFromClusterCommand( testDir.directory().toPath(), testDir.directory( "conf" ).toPath(),
                        mock( OutsideWorld.class ) );

        try
        {
            // when
            command.execute( nonExistentDatabaseArg() );
            fail();
        }
        catch ( IncorrectUsage e )
        {
            // then
            assertThat( e.getMessage(), containsString( "does not contain a database" ) );
        }
    }

    @Test
    public void shouldFailToUnbindLiveDatabase() throws Exception
    {
        // given
        try ( FileSystemAbstraction fsa = new DefaultFileSystemAbstraction() ) // because locking
        {
            fsa.mkdir( testDir.directory() );

            UnbindFromClusterCommand command =
                    new UnbindFromClusterCommand( testDir.directory().toPath(), testDir.directory( "conf" ).toPath(),
                            mock( OutsideWorld.class ) );

            FileLock fileLock = createLockedFakeDbDir( testDir.directory().toPath() );

            try
            {
                // when
                command.execute( databaseName( "graph.db" ) );
                fail();
            }
            catch ( CommandFailed e )
            {
                // then
                assertThat( e.getMessage(), containsString( "Database is currently locked. Please shutdown Neo4j." ) );
            }
            finally
            {
                fileLock.release();
            }
        }
    }

    @Test
    public void shouldRemoveClusterStateDirectoryForGivenDatabase() throws Exception
    {
        // given
        final int numberOfFilesAndDirsInANormalNeo4jStoreDir = 35;

        FileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        Path fakeDbDir = createUnlockedFakeDbDir( testDir.directory().toPath() );
        UnbindFromClusterCommand command =
                new UnbindFromClusterCommand( testDir.directory().toPath(), testDir.directory( "conf" ).toPath(),
                        mock( OutsideWorld.class ) );

        // when
        command.execute( databaseName( "graph.db" ) );

        // then
        assertEquals( numberOfFilesAndDirsInANormalNeo4jStoreDir, Files.list( fakeDbDir ).toArray().length );
    }

    @Test
    public void shouldReportWhenClusterStateDirectoryIsNotPresent() throws Exception
    {
        // given
        FileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        Path fakeDbDir = createUnlockedFakeDbDir( testDir.directory().toPath() );
        Files.delete( Paths.get( fakeDbDir.toString(), "cluster-state" ) );

        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        UnbindFromClusterCommand command =
                new UnbindFromClusterCommand( testDir.directory().toPath(), testDir.directory( "conf" ).toPath(),
                        outsideWorld );

        // when
        command.execute( databaseName( "graph.db" ) );

        verify( outsideWorld ).stdErrLine( startsWith( "No cluster state found in" ) );
    }

    @Test
    public void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new UnbindFromClusterCommand.Provider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin unbind [--database=<name>]%n" +
                            "%n" +
                            "Removes cluster state data from the specified database making it suitable for%n" +
                            "use in single instance database, or for seeding a new cluster.%n" +
                            "%n" +
                            "options:%n" +
                            "  --database=<name>   Name of database. [default:graph.db]%n" ),
                    baos.toString() );
        }
    }

    private String[] databaseName( String databaseName )
    {
        return new String[]{"--database=" + databaseName};
    }

    private Path createUnlockedFakeDbDir( Path parent ) throws IOException
    {
        Path fakeDbDir = createFakeDbDir( parent );
        Files.createFile( Paths.get( fakeDbDir.toString(), STORE_LOCK_FILENAME ) );
        return fakeDbDir;
    }

    private FileLock createLockedFakeDbDir( Path parent ) throws IOException
    {
        return createLockedStoreLockFileIn( createFakeDbDir( parent ) );
    }

    private Path createFakeDbDir( Path parent ) throws IOException
    {
        Path data = createDirectory( parent, "data" );
        Path database = createDirectory( data, "databases" );
        Path graph_db = createDirectory( database, "graph.db" );
        createDirectory( graph_db, "cluster-state" );
        createDirectory( graph_db, "schema" );
        createDirectory( graph_db, "index" );
        createFile( graph_db, "neostore" );
        createFile( graph_db, "neostore.counts.db.a" );
        createFile( graph_db, "neostore.id" );
        createFile( graph_db, "neostore.labeltokenstore.db" );
        createFile( graph_db, "neostore.labeltokenstore.db.id" );
        createFile( graph_db, "neostore.labeltokenstore.db.names" );
        createFile( graph_db, "neostore.labeltokenstore.db.names.id" );
        createFile( graph_db, "neostore.nodestore.db" );
        createFile( graph_db, "neostore.nodestore.db.id" );
        createFile( graph_db, "neostore.nodestore.db.labels" );
        createFile( graph_db, "neostore.nodestore.db.labels.id" );
        createFile( graph_db, "neostore.propertystore.db" );
        createFile( graph_db, "neostore.propertystore.db.arrays" );
        createFile( graph_db, "neostore.propertystore.db.arrays.id" );
        createFile( graph_db, "neostore.propertystore.db.id" );
        createFile( graph_db, "neostore.propertystore.db.index" );
        createFile( graph_db, "neostore.propertystore.db.index.id" );
        createFile( graph_db, "neostore.propertystore.db.index.keys" );
        createFile( graph_db, "neostore.propertystore.db.index.keys.id" );
        createFile( graph_db, "neostore.propertystore.db.strings" );
        createFile( graph_db, "neostore.propertystore.db.strings.id" );
        createFile( graph_db, "neostore.relationshipgroupstore.db" );
        createFile( graph_db, "neostore.relationshipgroupstore.db.id" );
        createFile( graph_db, "neostore.relationshipstore.db" );
        createFile( graph_db, "neostore.relationshipstore.db.id" );
        createFile( graph_db, "neostore.relationshiptypestore.db" );
        createFile( graph_db, "neostore.relationshiptypestore.db.id" );
        createFile( graph_db, "neostore.relationshiptypestore.db.names" );
        createFile( graph_db, "neostore.relationshiptypestore.db.names.id" );
        createFile( graph_db, "neostore.schemastore.db" );
        createFile( graph_db, "neostore.schemastore.db.id" );
        createFile( graph_db, "neostore.transaction.db.0" );

        return graph_db;
    }

    private Path createFile( Path parent, String file ) throws IOException
    {
        return Files.createFile( Paths.get( parent.toString(), file ) );
    }

    private Path createDirectory( Path parent, String subDir ) throws IOException
    {
        return Files.createDirectory( Paths.get( parent.toString(), subDir ) );
    }

    private FileLock createLockedStoreLockFileIn( Path parent ) throws IOException
    {
        Path storeLockFile = Files.createFile( Paths.get( parent.toString(), STORE_LOCK_FILENAME ) );
        FileChannel channel = FileChannel.open( storeLockFile, READ, WRITE );
        return channel.lock( 0, Long.MAX_VALUE, true );
    }

    private String[] nonExistentDatabaseArg()
    {
        return new String[]{"--database=" + UUID.randomUUID().toString()};
    }

    private String[] noArgs()
    {
        return new String[]{};
    }
}
