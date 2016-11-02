/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

public class VersionCommandTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public ExpectedException expected = ExpectedException.none();

    private Path databaseDirectory;
    private ByteArrayOutputStream baos;
    private VersionCommand command;
    private DefaultFileSystemAbstraction fs;
    private PageCache pageCache;

    @Before
    public void setUp() throws Exception
    {
        fs = new DefaultFileSystemAbstraction();
        pageCache = StandalonePageCacheFactory.createPageCache( fs );
        Path homeDir = testDirectory.directory( "home-dir" ).toPath();
        databaseDirectory = homeDir.resolve( "data/databases/foo.db" );
        Files.createDirectories( databaseDirectory );
        baos = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream( baos );
        command = new VersionCommand( printStream::println );
    }

    @After
    public void tearDown() throws Exception
    {
        baos.close();
        pageCache.close();
    }

    @Test
    public void shouldPrintNiceHelp() throws Exception
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );
            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new VersionCommand.Provider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin version --store=<path-to-dir>%n" +
                            "%n" +
                            "Checks the version of a Neo4j database store. Note that this command expects a%n" +
                            "path to a store directory, for example --store=data/databases/graph.db.%n" +
                            "%n" +
                            "options:%n" +
                            "  --store=<path-to-dir>   Path to database store to check version of.%n" ),
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
        expected.expectMessage( matches( "Directory '.+?' does not contain a database" ) );

        execute( Paths.get( "yaba", "daba", "doo" ).toString() );
    }

    @Test
    public void readsLatestStoreVersionCorrectly() throws Exception
    {
        RecordFormats currentFormat = RecordFormatSelector.defaultFormat();
        prepareNeoStoreFile( currentFormat.storeVersion() );

        execute( databaseDirectory.toString() );

        assertEquals(
                String.format( "Store version:      %s%n" +
                                "Introduced in:      %s%n",
                        currentFormat.storeVersion(),
                        currentFormat.neo4jVersion() ),
                baos.toString() );
    }

    @Test
    public void readsOlderStoreVersionCorrectly() throws Exception
    {
        prepareNeoStoreFile( StandardV2_2.RECORD_FORMATS.storeVersion() );

        execute( databaseDirectory.toString() );

        assertEquals(
                String.format( "Store version:      v0.A.5%n" +
                        "Introduced in:      2.2.0%n" +
                        "Superceded in:      2.3.0%n" ),
                baos.toString() );
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
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value );
    }

    private File createNeoStoreFile() throws IOException
    {
        fs.mkdir( databaseDirectory.toFile() );
        File neoStoreFile = new File( databaseDirectory.toFile(), MetaDataStore.DEFAULT_NAME );
        fs.create( neoStoreFile ).close();
        return neoStoreFile;
    }
}
