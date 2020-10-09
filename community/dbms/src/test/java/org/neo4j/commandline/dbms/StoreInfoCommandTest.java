/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.internal.locker.DatabaseLocker;
import org.neo4j.kernel.internal.locker.Locker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

@PageCacheExtension
class StoreInfoCommandTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    private Path fooDbDirectory;
    private StoreInfoCommand command;
    private PrintStream out;
    private DatabaseLayout fooDbLayout;
    private Path homeDir;

    @BeforeEach
    void setUp() throws Exception
    {
        homeDir = testDirectory.directory( "home-dir" );
        fooDbDirectory = homeDir.resolve( "data/databases/foo" );
        fooDbLayout = DatabaseLayout.ofFlat( fooDbDirectory );
        fileSystem.mkdirs( fooDbDirectory );

        out = mock( PrintStream.class );
        command = new StoreInfoCommand( new ExecutionContext( homeDir, homeDir, out, mock( PrintStream.class ), testDirectory.getFileSystem() ) );
    }

    @Test
    void printUsageHelp()
    {
        var baos = new ByteArrayOutputStream();
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ), CommandLine.Help.Ansi.OFF );
        }
        assertThat( baos.toString().trim() ).isEqualTo( String.format(
                        "Print information about a Neo4j database store.%n" +
                        "%n" +
                        "USAGE%n" +
                        "%n" +
                        "store-info [--all] [--structured] [--verbose] <path>%n" +
                        "%n" +
                        "DESCRIPTION%n" +
                        "%n" +
                        "Print information about a Neo4j database store, such as what version of Neo4j%n" +
                        "created it.%n" +
                        "%n" +
                        "PARAMETERS%n" +
                        "%n" +
                        "      <path>         Path to database store files, or databases directory if%n" +
                        "                       --all option is used%n" +
                        "%n" +
                        "OPTIONS%n" +
                        "%n" +
                        "      --verbose      Enable verbose output.%n" +
                        "      --structured   Return result structured as json%n" +
                        "      --all          Return store info for all databases at provided path"
        ) );
    }

    @Test
    void nonExistingDatabaseShouldThrow()
    {
        var notADirArgs = args( Paths.get( "yaba", "daba", "doo" ), false, false );
        CommandLine.populateCommand( command, notADirArgs );
        var notADirException = assertThrows( CommandFailedException.class, () -> command.execute() );
        assertThat( notADirException.getMessage() ).contains( "must point to a directory" );

        var dir = testDirectory.directory( "not-a-db" );
        var notADbArgs = args( dir, false, false );
        CommandLine.populateCommand( command, notADbArgs );
        var notADbException = assertThrows( CommandFailedException.class, () -> command.execute() );
        assertThat( notADbException.getMessage() ).contains( "does not contain the store files of a database" );
    }

    @Test
    void readsLatestStoreVersionCorrectly() throws Exception
    {
        var currentFormat = RecordFormatSelector.defaultFormat();
        prepareNeoStoreFile( currentFormat.storeVersion(), fooDbLayout );
        CommandLine.populateCommand( command, fooDbDirectory.toAbsolutePath().toString() );
        command.execute();

        verify( out ).print( Mockito.<String>argThat( result ->
            result.contains( String.format( "Store format version:         %s", currentFormat.storeVersion() ) ) &&
            result.contains( String.format( "Store format introduced in:   %s", currentFormat.introductionVersion() ) )
        ) );
    }

    @Test
    void readsOlderStoreVersionCorrectly() throws Exception
    {
        prepareNeoStoreFile( StandardV3_4.RECORD_FORMATS.storeVersion(), fooDbLayout );
        CommandLine.populateCommand( command, fooDbDirectory.toAbsolutePath().toString() );
        command.execute();

        verify( out ).print( Mockito.<String>argThat( result ->
            result.contains( "Store format version:         v0.A.9" ) &&
            result.contains( "Store format introduced in:   3.4.0" ) &&
            result.contains( "Store format superseded in:   4.0.0" )
        ) );
    }

    @Test
    void throwsOnUnknownVersion() throws Exception
    {
        prepareNeoStoreFile( "v9.9.9", fooDbLayout );
        CommandLine.populateCommand( command, fooDbDirectory.toAbsolutePath().toString() );
        var exception = assertThrows( Exception.class, () -> command.execute() );
        assertThat( exception ).hasRootCauseInstanceOf( IllegalArgumentException.class );
        assertThat( exception.getMessage() ).contains( "Unknown store version 'v9.9.9'" );
    }

    @Test
    void respectLockFiles() throws IOException
    {
        var currentFormat = RecordFormatSelector.defaultFormat();
        prepareNeoStoreFile( currentFormat.storeVersion(), fooDbLayout );

        try ( Locker locker = new DatabaseLocker( fileSystem, fooDbLayout ) )
        {
            locker.checkLock();
            CommandLine.populateCommand( command, fooDbDirectory.toAbsolutePath().toString() );
            var exception = assertThrows( Exception.class, () -> command.execute() );
            assertEquals( "Failed to execute command as the database 'foo' is in use. Please stop it and try again.", exception.getMessage() );
        }
    }

    @Test
    void doesNotThrowWhenUsingAllAndSomeDatabasesLocked() throws IOException
    {
        // given
        var currentFormat = RecordFormatSelector.defaultFormat();

        var barDbDirectory = homeDir.resolve( "data/databases/bar" );
        var barDbLayout = DatabaseLayout.ofFlat( barDbDirectory );
        fileSystem.mkdirs( barDbDirectory );

        prepareNeoStoreFile( currentFormat.storeVersion(), fooDbLayout );
        prepareNeoStoreFile( currentFormat.storeVersion(), barDbLayout );
        var databasesRoot = homeDir.resolve( "data/databases" );

        var expectedBar = expectedStructuredResult( "bar", false, currentFormat.storeVersion(), currentFormat.introductionVersion(), null );

        var expectedFoo = expectedStructuredResult( "foo", true, null, null, null );

        var expected = String.format( "[%s,%s]", expectedBar, expectedFoo );

        // when
        try ( Locker locker = new DatabaseLocker( fileSystem, fooDbLayout ) )
        {
            locker.checkLock();

            CommandLine.populateCommand( command, args( databasesRoot, true, true ) );
            command.execute();
        }

        verify( out ).print( expected );
    }

    @Test
    void retunsInfoForAllDatabasesInDirectory() throws IOException
    {
        // given
        var currentFormat = RecordFormatSelector.defaultFormat();

        var barDbDirectory = homeDir.resolve( "data/databases/bar" );
        var barDbLayout = DatabaseLayout.ofFlat( barDbDirectory );
        fileSystem.mkdirs( barDbDirectory );

        prepareNeoStoreFile( currentFormat.storeVersion(), fooDbLayout );
        prepareNeoStoreFile( currentFormat.storeVersion(), barDbLayout );
        var databasesRoot = homeDir.resolve( "data/databases" );

        var expectedBar = expectedPrettyResult( "bar", false, currentFormat.storeVersion(), currentFormat.introductionVersion(), null );

        var expectedFoo = expectedPrettyResult( "foo", false, currentFormat.storeVersion(), currentFormat.introductionVersion(), null );

        var expected = expectedBar +
                       System.lineSeparator() +
                       System.lineSeparator() +
                       expectedFoo;

        // when
        CommandLine.populateCommand( command, args( databasesRoot, true, false ) );
        command.execute();

        // then
        verify( out ).print( expected );
    }

    @Test
    void returnsInfoStructuredAsJson() throws IOException
    {
        //given
        var currentFormat = RecordFormatSelector.defaultFormat();
        prepareNeoStoreFile( currentFormat.storeVersion(), fooDbLayout );
        var expectedFoo = expectedStructuredResult( "foo", false, currentFormat.storeVersion(), currentFormat.introductionVersion(), null );

        // when
        CommandLine.populateCommand( command, args( fooDbDirectory, false, true ) );
        command.execute();

        // then
        verify( out ).print( expectedFoo );
    }

    private String expectedPrettyResult( String databaseName, boolean inUse, String version, String introduced, String superseded )
    {
        var nullSafeSuperseded = superseded == null ? "" : "Store format superseded in:   " + superseded + System.lineSeparator();
        return "Database name:                " + databaseName + System.lineSeparator() +
               "Database in use:              " + inUse + System.lineSeparator() +
               "Store format version:         " + version + System.lineSeparator() +
               "Store format introduced in:   " + introduced + System.lineSeparator() +
               nullSafeSuperseded +
               "Last committed transaction id:-1" + System.lineSeparator() +
               "Store needs recovery:         true";
    }

    private String expectedStructuredResult( String databaseName, boolean inUse, String version, String introduced, String superseded )
    {
        return "{" +
               "\"databaseName\":\"" + databaseName + "\"," +
               "\"inUse\":\"" + inUse + "\"," +
               "\"storeFormat\":" + nullSafeField( version ) + "," +
               "\"storeFormatIntroduced\":" + nullSafeField( introduced ) + "," +
               "\"storeFormatSuperseded\":" + nullSafeField( superseded ) + "," +
               "\"lastCommittedTransaction\":\"-1\"," +
               "\"recoveryRequired\":\"true\"" +
               "}";
    }

    private String nullSafeField( String value )
    {
        return value == null ? "null" : "\"" + value + "\"";
    }

    private void prepareNeoStoreFile( String storeVersion, DatabaseLayout dbLayout ) throws IOException
    {
        var neoStoreFile = createNeoStoreFile( dbLayout );
        var value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value, NULL );
    }

    private Path createNeoStoreFile( DatabaseLayout dbLayout ) throws IOException
    {
        var neoStoreFile = dbLayout.metadataStore();
        fileSystem.write( neoStoreFile ).close();
        return neoStoreFile;
    }

    private String[] args( Path path, boolean all, boolean structured )
    {
        var args = new ArrayList<String>();
        args.add( path.toAbsolutePath().toString() );

        if ( all )
        {
            args.add( "--all" );
        }

        if ( structured )
        {
            args.add( "--structured" );
        }

        return args.toArray( new String[0] );
    }
}
