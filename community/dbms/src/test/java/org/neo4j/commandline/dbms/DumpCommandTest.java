/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.commandline.dbms;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Predicate;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.dbms.archive.TestUtils.withPermissions;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.data_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_logs_location;

public class DumpCommandTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public ExpectedException expected = ExpectedException.none();

    private Path homeDir;
    private Path configDir;
    private Path archive;
    private Dumper dumper;
    private Path databaseDirectory;

    @Before
    public void setUp() throws Exception
    {
        homeDir = testDirectory.directory( "home-dir" ).toPath();
        configDir = testDirectory.directory( "config-dir" ).toPath();
        archive = testDirectory.file( "some-archive.dump" ).toPath();
        dumper = mock( Dumper.class );
        putStoreInDirectory( homeDir.resolve( "data/databases/foo.db" ) );
        databaseDirectory = homeDir.resolve( "data/databases/foo.db" );
    }

    @Test
    public void shouldDumpTheDatabaseToTheArchive() throws Exception
    {
        execute( "foo.db" );
        verify( dumper ).dump( eq( homeDir.resolve( "data/databases/foo.db" ) ),
                eq( homeDir.resolve( "data/databases/foo.db" ) ), eq( archive ), any() );
    }

    @Test
    public void shouldCalculateTheDatabaseDirectoryFromConfig() throws Exception
    {
        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo.db" );
        putStoreInDirectory( databaseDir );
        Files.write( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ),
                asList( formatProperty( data_directory, dataDir ) ) );

        execute( "foo.db" );
        verify( dumper ).dump( eq( databaseDir ), eq( databaseDir ), any(), any() );
    }

    @Test
    public void shouldCalculateTheTxLogDirectoryFromConfig() throws Exception
    {
        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path txLogsDir = testDirectory.directory( "txLogsPath" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo.db" );
        putStoreInDirectory( databaseDir );
        Files.write( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ),
                asList( formatProperty( data_directory, dataDir ),
                        formatProperty( logical_logs_location, txLogsDir ) ) );

        execute( "foo.db" );
        verify( dumper ).dump( eq( databaseDir ), eq( txLogsDir ), any(), any() );
    }

    @Test
    public void shouldHandleDatabaseSymlink() throws Exception
    {
        assumeFalse( "Can't reliably create symlinks on windows", SystemUtils.IS_OS_WINDOWS );

        Path symDir = testDirectory.directory( "path-to-links" ).toPath();
        Path realDatabaseDir = symDir.resolve( "foo.db" );

        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo.db" );

        putStoreInDirectory( realDatabaseDir );
        Files.createDirectories( dataDir.resolve( "databases" ) );

        Files.createSymbolicLink( databaseDir, realDatabaseDir );
        Files.write( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ),
                asList( format( "%s=%s", data_directory.name(), dataDir.toString().replace( '\\', '/' ) ) ) );

        execute( "foo.db" );
        verify( dumper ).dump( eq( realDatabaseDir ), eq( realDatabaseDir ), any(), any() );
    }

    @Test
    public void shouldCalculateTheArchiveNameIfPassedAnExistingDirectory()
            throws Exception
    {
        File to = testDirectory.directory( "some-dir" );
        new DumpCommand( homeDir, configDir, dumper ).execute( new String[]{"--database=" + "foo.db", "--to=" + to} );
        verify( dumper ).dump( any( Path.class ), any( Path.class ), eq( to.toPath().resolve( "foo.db.dump" ) ), any() );
    }

    @Test
    public void shouldConvertToCanonicalPath() throws Exception
    {
        new DumpCommand( homeDir, configDir, dumper )
                .execute( new String[]{"--database=" + "foo.db", "--to=foo.dump"} );
        verify( dumper ).dump( any( Path.class ), any( Path.class ),
                eq( Paths.get( new File( "foo.dump" ).getCanonicalPath() ) ), any() );
    }

    @Test
    public void shouldNotCalculateTheArchiveNameIfPassedAnExistingFile()
            throws Exception
    {
        Files.createFile( archive );
        execute( "foo.db" );
        verify( dumper ).dump( any(), any(), eq( archive ), any() );
    }

    @Test
    public void shouldRespectTheStoreLock() throws Exception
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/foo.db" );

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              StoreLocker storeLocker = new StoreLocker( fileSystem, databaseDirectory.toFile() ) )
        {
            storeLocker.checkLock();

            execute( "foo.db" );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "the database is in use -- stop Neo4j and try again" ) );
        }
    }

    @Test
    public void shouldReleaseTheStoreLockAfterDumping() throws Exception
    {
        execute( "foo.db" );
        assertCanLockStore( databaseDirectory );
    }

    @Test
    public void shouldReleaseTheStoreLockEvenIfThereIsAnError() throws Exception
    {
        doThrow( IOException.class ).when( dumper ).dump( any(), any(), any(), any() );

        try
        {
            execute( "foo.db" );
        }
        catch ( CommandFailed ignored )
        {
        }

        assertCanLockStore( databaseDirectory );
    }

    @Test
    public void shouldNotAccidentallyCreateTheDatabaseDirectoryAsASideEffectOfStoreLocking()
            throws Exception
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/accident.db" );

        doAnswer( ignored ->
        {
            assertThat( Files.exists( databaseDirectory ), equalTo( false ) );
            return null;
        } ).when( dumper ).dump( any(), any(), any(), any() );

        execute( "foo.db" );
    }

    @Test
    public void shouldReportAHelpfulErrorIfWeDontHaveWritePermissionsForLock() throws Exception
    {
        assumeFalse( "We haven't found a way to reliably tests permissions on Windows", SystemUtils.IS_OS_WINDOWS );

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              StoreLocker storeLocker = new StoreLocker( fileSystem, databaseDirectory.toFile() ) )
        {
            storeLocker.checkLock();

            try ( Closeable ignored = withPermissions( databaseDirectory.resolve( StoreLocker.STORE_LOCK_FILENAME ),
                    emptySet() ) )
            {
                execute( "foo.db" );
                fail( "expected exception" );
            }
            catch ( CommandFailed e )
            {
                assertThat( e.getMessage(), equalTo( "you do not have permission to dump the database -- is Neo4j " +
                        "running as a different user?" ) );
            }
        }
    }

    @Test
    public void shouldExcludeTheStoreLockFromTheArchiveToAvoidProblemsWithReadingLockedFilesOnWindows()
            throws Exception
    {
        doAnswer( invocation ->
        {
            //noinspection unchecked
            Predicate<Path> exclude = invocation.getArgument( 3 );
            assertThat( exclude.test( Paths.get( StoreLocker.STORE_LOCK_FILENAME ) ), is( true ) );
            assertThat( exclude.test( Paths.get( "some-other-file" ) ), is( false ) );
            return null;
        } ).when( dumper ).dump(any(), any(), any(), any() );

        execute( "foo.db" );
    }

    @Test
    public void shouldDefaultToGraphDB() throws Exception
    {
        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/graph.db" );
        putStoreInDirectory( databaseDir );
        Files.write( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ),
                asList( formatProperty( data_directory, dataDir ) ) );

        new DumpCommand( homeDir, configDir, dumper ).execute( new String[]{"--to=" + archive} );
        verify( dumper ).dump( eq( databaseDir ), eq( databaseDir ), any(), any() );
    }

    @Test
    public void shouldObjectIfTheArchiveArgumentIsMissing() throws Exception
    {
        try
        {
            new DumpCommand( homeDir, configDir, null ).execute( new String[]{"--database=something"} );
            fail( "expected exception" );
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(), equalTo( "Missing argument 'to'" ) );
        }
    }

    @Test
    public void shouldGiveAClearErrorIfTheArchiveAlreadyExists() throws Exception
    {
        doThrow( new FileAlreadyExistsException( "the-archive-path" ) ).when( dumper ).dump( any(), any(), any(), any() );
        try
        {
            execute( "foo.db" );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "archive already exists: the-archive-path" ) );
        }
    }

    @Test
    public void shouldGiveAClearMessageIfTheDatabaseDoesntExist() throws Exception
    {
        try
        {
            execute( "bobo.db" );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "database does not exist: bobo.db" ) );
        }
    }

    @Test
    public void shouldGiveAClearMessageIfTheArchivesParentDoesntExist() throws Exception
    {
        doThrow( new NoSuchFileException( archive.getParent().toString() ) ).when( dumper ).dump(any(), any(), any(), any() );
        try
        {
            execute( "foo.db" );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(),
                    equalTo( "unable to dump database: NoSuchFileException: " + archive.getParent() ) );
        }
    }

    @Test
    public void shouldWrapIOExceptionsCarefullyBecauseCriticalInformationIsOftenEncodedInTheirNameButMissingFromTheirMessage()
            throws Exception
    {
        doThrow( new IOException( "the-message" ) ).when( dumper ).dump(any(), any(), any(), any() );
        try
        {
            execute( "foo.db" );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "unable to dump database: IOException: the-message" ) );
        }
    }

    @Test
    public void shouldPrintNiceHelp() throws Exception
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new DumpCommandProvider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin dump [--database=<name>] --to=<destination-path>%n" +
                            "%n" +
                            "environment variables:%n" +
                            "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                            "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                            "    NEO4J_HOME    Neo4j home directory.%n" +
                            "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                            "                  Takes a number and a unit, for example 512m.%n" +
                            "%n" +
                            "Dump a database into a single-file archive. The archive can be used by the load%n" +
                            "command. <destination-path> can be a file or directory (in which case a file%n" +
                            "called <database>.dump will be created). It is not possible to dump a database%n" +
                            "that is mounted in a running Neo4j server.%n" +
                            "%n" +
                            "options:%n" +
                            "  --database=<name>         Name of database. [default:graph.db]%n" +
                            "  --to=<destination-path>   Destination (file or folder) of database dump.%n" ),
                    baos.toString() );
        }
    }

    private void execute( final String database ) throws IncorrectUsage, CommandFailed
    {
        new DumpCommand( homeDir, configDir, dumper )
                .execute( new String[]{"--database=" + database, "--to=" + archive} );
    }

    private static void assertCanLockStore( Path databaseDirectory ) throws IOException
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              StoreLocker storeLocker = new StoreLocker( fileSystem, databaseDirectory.toFile() ) )
        {
            storeLocker.checkLock();
        }
    }

    private static void putStoreInDirectory( Path storeDir ) throws IOException
    {
        Files.createDirectories( storeDir );
        Path storeFile = storeDir.resolve( StoreFileType.STORE.augment( MetaDataStore.DEFAULT_NAME ) );
        Files.createFile( storeFile );
    }

    private static String formatProperty( Setting setting, Path path )
    {
        return format( "%s=%s", setting.name(), path.toString().replace( '\\', '/' ) );
    }
}
