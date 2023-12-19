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
package org.neo4j.commandline.dbms;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.causalclustering.core.state.ClusterStateException;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.internal.locker.StoreLocker.STORE_LOCK_FILENAME;

public class UnbindFromClusterCommandTest
{
    private final TestDirectory testDir = TestDirectory.testDirectory();
    private final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( testDir );
    private Path homeDir;
    private Path confDir;

    private FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private OutsideWorld outsideWorld = mock( OutsideWorld.class );
    private FileChannel channel;

    @Before
    public void setup()
    {
        homeDir = testDir.directory( "home" ).toPath();
        confDir = testDir.directory( "conf" ).toPath();
        fs.mkdir( homeDir.toFile() );

        when( outsideWorld.fileSystem() ).thenReturn( fs );
    }

    @After
    public void tearDown() throws IOException
    {
        IOUtils.closeAll( channel );
    }

    private File createClusterStateDir( FileSystemAbstraction fs ) throws ClusterStateException
    {
        File dataDir = new File( homeDir.toFile(), "data" );
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir, false );
        clusterStateDirectory.initialize( fs );
        return clusterStateDirectory.get();
    }

    @Test
    public void shouldIgnoreIfSpecifiedDatabaseDoesNotExist() throws Exception
    {
        // given
        File clusterStateDir = createClusterStateDir( fs );
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( homeDir, confDir, outsideWorld );

        // when
        command.execute( databaseNameParameter( "doesnotexist.db" ) );

        // then
        assertFalse( fs.fileExists( clusterStateDir ) );
    }

    @Test
    public void shouldFailToUnbindLiveDatabase() throws Exception
    {
        // given
        createClusterStateDir( fs );
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( homeDir, confDir, outsideWorld );

        FileLock fileLock = createLockedFakeDbDir( homeDir );
        try
        {
            // when
            command.execute( databaseNameParameter( "graph.db" ) );
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

    @Test
    public void shouldRemoveClusterStateDirectoryForGivenDatabase() throws Exception
    {
        // given
        File clusterStateDir = createClusterStateDir( fs );
        createUnlockedFakeDbDir( homeDir );
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( homeDir, confDir, outsideWorld );

        // when
        command.execute( databaseNameParameter( "graph.db" ) );

        // then
        assertFalse( fs.fileExists( clusterStateDir ) );
    }

    @Test
    public void shouldReportWhenClusterStateDirectoryIsNotPresent() throws Exception
    {
        // given
        createUnlockedFakeDbDir( homeDir );
        UnbindFromClusterCommand command = new UnbindFromClusterCommand( homeDir, confDir, outsideWorld );

        // when
        try
        {
            // when
            command.execute( databaseNameParameter( "graph.db" ) );
        }
        catch ( CommandFailed e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Cluster state directory does not exist" ) );
        }
    }

    @Test
    public void shouldPrintUsage() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new UnbindFromClusterCommandProvider(), ps::println );

            assertThat( baos.toString(), containsString( "usage" ) );
        }
    }

    private Path createUnlockedFakeDbDir( Path homeDir ) throws IOException
    {
        Path fakeDbDir = createFakeDbDir( homeDir );
        Files.createFile( Paths.get( fakeDbDir.toString(), STORE_LOCK_FILENAME ) );
        return fakeDbDir;
    }

    private FileLock createLockedFakeDbDir( Path homeDir ) throws IOException
    {
        return createLockedStoreLockFileIn( createFakeDbDir( homeDir ) );
    }

    private Path createFakeDbDir( Path homeDir ) throws IOException
    {
        Path graphDb = homeDir.resolve( "data/databases/graph.db" );
        fs.mkdirs( graphDb.toFile() );
        fs.create( graphDb.resolve( "neostore" ).toFile() ).close();
        return graphDb;
    }

    private FileLock createLockedStoreLockFileIn( Path parent ) throws IOException
    {
        Path storeLockFile = Files.createFile( Paths.get( parent.toString(), STORE_LOCK_FILENAME ) );
        channel = FileChannel.open( storeLockFile, READ, WRITE );
        return channel.lock( 0, Long.MAX_VALUE, true );
    }

    private String[] databaseNameParameter( String databaseName )
    {
        return new String[]{"--database=" + databaseName};
    }
}
