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

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

public class VersionCommandEnterpriseTest
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
    public void readsEnterpriseStoreVersionCorrectly() throws Exception
    {
        prepareNeoStoreFile( HighLimitV3_0_0.RECORD_FORMATS.storeVersion() );

        execute( databaseDirectory.toString() );

        assertEquals(
                String.format( "Store version:      vE.H.0%n" +
                        "Introduced in:      3.0.0%n" +
                        "Superceded in:      3.0.6%n" ),
                baos.toString() );
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
