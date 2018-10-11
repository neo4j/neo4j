/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Consumer;

import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.mockito.matcher.RootCauseMatcher;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    private ArgumentCaptor<String> outCaptor;
    private StoreInfoCommand command;
    private Consumer<String> out;
    private DatabaseLayout databaseLayout;

    @BeforeEach
    void setUp() throws Exception
    {
        Path homeDir = testDirectory.directory( "home-dir" ).toPath();
        databaseDirectory = homeDir.resolve( "data/databases/foo.db" );
        databaseLayout = DatabaseLayout.of( databaseDirectory.toFile() );
        Files.createDirectories( databaseDirectory );

        outCaptor = ArgumentCaptor.forClass( String.class );
        out = mock( Consumer.class );
        command = new StoreInfoCommand( out );
    }

    @Test
    void shouldPrintNiceHelp() throws Exception
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );
            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new StoreInfoCommandProvider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin store-info --store=<path-to-dir>%n" +
                            "%n" +
                            "environment variables:%n" +
                            "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                            "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                            "    NEO4J_HOME    Neo4j home directory.%n" +
                            "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                            "                  Takes a number and a unit, for example 512m.%n" +
                            "%n" +
                            "Prints information about a Neo4j database store, such as what version of Neo4j%n" +
                            "created it. Note that this command expects a path to a store directory, for%n" +
                            "example --store=data/databases/graph.db.%n" +
                            "%n" +
                            "options:%n" +
                            "  --store=<path-to-dir>   Path to database store.%n" ),
                    baos.toString() );
        }
    }

    @Test
    void noArgFails()
    {
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> command.execute( new String[]{} ) );
        assertEquals( "Missing argument 'store'", exception.getMessage() );
    }

    @Test
    void emptyArgFails()
    {
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> command.execute( new String[]{"--store="} ) );
        assertEquals( "Missing argument 'store'", exception.getMessage() );
    }

    @Test
    void nonExistingDatabaseShouldThrow()
    {
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> execute( Paths.get( "yaba", "daba", "doo" ).toString() ) );
        assertThat( exception.getMessage(), containsString( "does not contain a database" ) );
    }

    @Test
    void readsLatestStoreVersionCorrectly() throws Exception
    {
        RecordFormats currentFormat = RecordFormatSelector.defaultFormat();
        prepareNeoStoreFile( currentFormat.storeVersion() );

        execute( databaseDirectory.toString() );

        verify( out, times( 2 ) ).accept( outCaptor.capture() );

        assertEquals(
                Arrays.asList(
                        String.format( "Store format version:         %s", currentFormat.storeVersion() ),
                        String.format( "Store format introduced in:   %s", currentFormat.introductionVersion() ) ),
                outCaptor.getAllValues() );
    }

    @Test
    void readsOlderStoreVersionCorrectly() throws Exception
    {
        prepareNeoStoreFile( StandardV2_3.RECORD_FORMATS.storeVersion() );

        execute( databaseDirectory.toString() );

        verify( out, times( 3 ) ).accept( outCaptor.capture() );

        assertEquals(
                Arrays.asList(
                        "Store format version:         v0.A.6",
                        "Store format introduced in:   2.3.0",
                        "Store format superseded in:   3.0.0" ),
                outCaptor.getAllValues() );
    }

    @Test
    void throwsOnUnknownVersion() throws Exception
    {
        prepareNeoStoreFile( "v9.9.9" );
        Exception exception = assertThrows( Exception.class, () -> execute( databaseDirectory.toString() ) );
        assertThat( exception, new RootCauseMatcher( IllegalArgumentException.class ) );
        assertEquals( "Unknown store version 'v9.9.9'", exception.getMessage() );
    }

    private void execute( String storePath ) throws Exception
    {
        command.execute( new String[]{"--store=" + storePath} );
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
        fileSystem.create( neoStoreFile ).close();
        return neoStoreFile;
    }
}
