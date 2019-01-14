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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

public class StoreInfoCommandTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public ExpectedException expected = ExpectedException.none();
    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    private Path databaseDirectory;
    private ArgumentCaptor<String> outCaptor;
    private StoreInfoCommand command;
    private Consumer<String> out;

    @Before
    public void setUp() throws Exception
    {
        Path homeDir = testDirectory.directory( "home-dir" ).toPath();
        databaseDirectory = homeDir.resolve( "data/databases/foo.db" );
        Files.createDirectories( databaseDirectory );

        outCaptor = ArgumentCaptor.forClass( String.class );
        out = mock( Consumer.class );
        command = new StoreInfoCommand( out );
    }

    @Test
    public void shouldPrintNiceHelp() throws Exception
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
    public void noArgFails() throws Exception
    {
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage( "Missing argument 'store'" );

        command.execute( new String[]{} );
    }

    @Test
    public void emptyArgFails() throws Exception
    {
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage( "Missing argument 'store'" );

        command.execute( new String[]{"--store="} );
    }

    @Test
    public void nonExistingDatabaseShouldThrow() throws Exception
    {
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage( "does not contain a database" );

        execute( Paths.get( "yaba", "daba", "doo" ).toString() );
    }

    @Test
    public void readsLatestStoreVersionCorrectly() throws Exception
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
    public void readsOlderStoreVersionCorrectly() throws Exception
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
    public void throwsOnUnknownVersion() throws Exception
    {
        prepareNeoStoreFile( "v9.9.9" );

        expected.expect( IllegalArgumentException.class );
        expected.expectMessage( "Unknown store version 'v9.9.9'" );

        execute( databaseDirectory.toString() );
    }

    private void execute( String storePath ) throws Exception
    {
        command.execute( new String[]{"--store=" + storePath} );
    }

    private void prepareNeoStoreFile( String storeVersion ) throws IOException
    {
        File neoStoreFile = createNeoStoreFile();
        long value = MetaDataStore.versionStringToLong( storeVersion );
        try ( PageCache pageCache = pageCacheRule.getPageCache( fsRule.get() ) )
        {
            MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value );
        }
    }

    private File createNeoStoreFile() throws IOException
    {
        fsRule.get().mkdir( databaseDirectory.toFile() );
        File neoStoreFile = new File( databaseDirectory.toFile(), MetaDataStore.DEFAULT_NAME );
        fsRule.get().create( neoStoreFile ).close();
        return neoStoreFile;
    }
}
