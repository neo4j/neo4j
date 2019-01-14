/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.commandline.dbms;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Consumer;

import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

public class StoreInfoCommandEnterpriseTest
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
    public void readsEnterpriseStoreVersionCorrectly() throws Exception
    {
        prepareNeoStoreFile( HighLimitV3_0_0.RECORD_FORMATS.storeVersion() );

        execute( databaseDirectory.toString() );

        verify( out, times( 3 ) ).accept( outCaptor.capture() );

        assertEquals(
                Arrays.asList(
                        "Store format version:         vE.H.0",
                        "Store format introduced in:   3.0.0",
                        "Store format superseded in:   3.0.6" ),
                outCaptor.getAllValues() );
    }

    private void execute( String storePath ) throws Exception
    {
        command.execute( new String[]{"--store=" + storePath} );
    }

    private void prepareNeoStoreFile( String storeVersion ) throws IOException
    {
        File neoStoreFile = createNeoStoreFile();
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCacheRule.getPageCache( fsRule.get() ), neoStoreFile, STORE_VERSION, value );
    }

    private File createNeoStoreFile() throws IOException
    {
        fsRule.get().mkdir( databaseDirectory.toFile() );
        File neoStoreFile = new File( databaseDirectory.toFile(), MetaDataStore.DEFAULT_NAME );
        fsRule.get().create( neoStoreFile ).close();
        return neoStoreFile;
    }
}
