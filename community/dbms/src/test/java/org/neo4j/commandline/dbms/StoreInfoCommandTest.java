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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.internal.locker.DatabaseLocker;
import org.neo4j.kernel.internal.locker.Locker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.mockito.matcher.RootCauseMatcher;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
    private Path databaseDirectory;
    private StoreInfoCommand command;
    private PrintStream out;
    private DatabaseLayout databaseLayout;

    @BeforeEach
    void setUp() throws Exception
    {
        Path homeDir = testDirectory.directory( "home-dir" ).toPath();
        databaseDirectory = homeDir.resolve( "data/databases/foo" );
        databaseLayout = DatabaseLayout.of( databaseDirectory.toFile() );
        Files.createDirectories( databaseDirectory );

        out = mock( PrintStream.class );
        command = new StoreInfoCommand( new ExecutionContext( homeDir, homeDir, out, mock( PrintStream.class ), testDirectory.getFileSystem() ) );
    }

    @Test
    void printUsageHelp()
    {
        var baos = new ByteArrayOutputStream();
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim(), equalTo( String.format(
                        "Print information about a Neo4j database store.%n" +
                        "%n" +
                        "USAGE%n" +
                        "%n" +
                        "store-info [--verbose] <storePath>%n" +
                        "%n" +
                        "DESCRIPTION%n" +
                        "%n" +
                        "Print information about a Neo4j database store, such as what version of Neo4j%n" +
                        "created it.%n" +
                        "%n" +
                        "PARAMETERS%n" +
                        "%n" +
                        "      <storePath>   Path to database store.%n" +
                        "%n" +
                        "OPTIONS%n" +
                        "%n" +
                        "      --verbose     Enable verbose output."
        ) ) );
    }

    @Test
    void nonExistingDatabaseShouldThrow()
    {
        CommandLine.populateCommand( command, Paths.get( "yaba", "daba", "doo" ).toFile().getAbsolutePath() );
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> command.execute() );
        assertThat( exception.getMessage(), containsString( "does not contain a database" ) );
    }

    @Test
    void readsLatestStoreVersionCorrectly() throws Exception
    {
        RecordFormats currentFormat = RecordFormatSelector.defaultFormat();
        prepareNeoStoreFile( currentFormat.storeVersion() );
        CommandLine.populateCommand( command, databaseDirectory.toFile().getAbsolutePath() );
        command.execute();

        verify( out ).println( String.format( "Store format version:         %s", currentFormat.storeVersion() ) );
        verify( out ).println( String.format( "Store format introduced in:   %s", currentFormat.introductionVersion() ) );
        verifyNoMoreInteractions( out );
    }

    @Test
    void readsOlderStoreVersionCorrectly() throws Exception
    {
        prepareNeoStoreFile( StandardV3_4.RECORD_FORMATS.storeVersion() );
        CommandLine.populateCommand( command, databaseDirectory.toFile().getAbsolutePath() );
        command.execute();

        verify( out ).println( "Store format version:         v0.A.9" );
        verify( out ).println( "Store format introduced in:   3.4.0" );
        verify( out ).println( "Store format superseded in:   4.0.0" );
        verifyNoMoreInteractions( out );
    }

    @Test
    void throwsOnUnknownVersion() throws Exception
    {
        prepareNeoStoreFile( "v9.9.9" );
        CommandLine.populateCommand( command, databaseDirectory.toFile().getAbsolutePath() );
        Exception exception = assertThrows( Exception.class, () -> command.execute() );
        assertThat( exception, new RootCauseMatcher( IllegalArgumentException.class ) );
        assertThat( exception.getMessage(), containsString( "Unknown store version 'v9.9.9'" ) );
    }

    @Test
    void respectLockFiles() throws IOException
    {
        RecordFormats currentFormat = RecordFormatSelector.defaultFormat();
        prepareNeoStoreFile( currentFormat.storeVersion() );

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              Locker locker = new DatabaseLocker( fileSystem, databaseLayout ) )
        {
            locker.checkLock();
            CommandLine.populateCommand( command, databaseDirectory.toFile().getAbsolutePath() );
            Exception exception = assertThrows( Exception.class, () -> command.execute() );
            assertEquals( "The database is in use. Stop database 'foo' and try again.", exception.getMessage() );
        }
    }

    private void prepareNeoStoreFile( String storeVersion ) throws IOException
    {
        File neoStoreFile = createNeoStoreFile();
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value );
    }

    private File createNeoStoreFile() throws IOException
    {
        File neoStoreFile = databaseLayout.metadataStore();
        fileSystem.write( neoStoreFile ).close();
        return neoStoreFile;
    }
}
