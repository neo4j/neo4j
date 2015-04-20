/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestStore
{
    public static IdGeneratorFactory ID_GENERATOR_FACTORY =
            new DefaultIdGeneratorFactory();
    public static FileSystemAbstraction FILE_SYSTEM =
            new DefaultFileSystemAbstraction();

    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private PageCache pageCache;

    @Before
    public void setUp()
    {
        pageCache = pageCacheRule.getPageCache( FILE_SYSTEM );
    }

    private File path()
    {
        String path = AbstractNeo4jTestCase.getStorePath( "teststore" );
        File file = new File( path );
        file.mkdirs();
        return file;
    }

    private File file( String name )
    {
        return new File( path() , name);
    }

    private File storeFile()
    {
        return file( "testStore.db" );
    }

    private File storeIdFile()
    {
        return file( "testStore.db.id" );
    }

    @Test
    public void testCreateStore() throws IOException
    {
        try
        {
            try
            {
                Store.createStore( null, pageCache );
                fail( "Null fileName should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            Store store = Store.createStore( storeFile(), pageCache );
            try
            {
                Store.createStore( storeFile(), pageCache );
                fail( "Creating existing store should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
            store.close();
        }
        finally
        {
            deleteBothFiles();
        }
    }

    private void deleteBothFiles()
    {
        File file = storeFile();
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = storeIdFile();
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
    }

    @Test
    public void testStickyStore() throws IOException
    {
        try
        {
            Store.createStore( storeFile(), pageCache ).close();
            java.nio.channels.FileChannel fileChannel = new java.io.RandomAccessFile(
                    storeFile(), "rw" ).getChannel();
            fileChannel.truncate( fileChannel.size() - 2 );
            fileChannel.close();
            Store store = new Store( storeFile(), pageCache );
            store.makeStoreOk();
            store.close();
        }
        finally
        {
            deleteBothFiles();
        }
    }

    @Test
    public void testClose() throws IOException
    {
        try
        {
            Store store = Store.createStore( storeFile(), pageCache );
            store.close();
        }
        finally
        {
            deleteBothFiles();
        }
    }

    private static class Store extends AbstractStore
    {
        private final static Config config = new Config( MapUtil.stringMap( "store_dir", "target/var/teststore" ),
                GraphDatabaseSettings.class );
        public static final String TYPE_DESCRIPTOR = "TestVersion";
        private static final int RECORD_SIZE = 1;

        public Store( File fileName, PageCache pageCache ) throws IOException
        {
            super( fileName,
                    config,
                    IdType.NODE,
                    ID_GENERATOR_FACTORY,
                    pageCache,
                    FILE_SYSTEM,
                    StringLogger.DEV_NULL,
                    StoreVersionMismatchHandler.FORCE_CURRENT_VERSION
            );
        }

        @Override
        public int getRecordSize()
        {
            return RECORD_SIZE;
        }

        @Override
        public String getTypeDescriptor()
        {
            return TYPE_DESCRIPTOR;
        }

        public static Store createStore( File fileName, PageCache pageCache ) throws IOException
        {
            new StoreFactory(
                    new Config( Collections.<String, String>emptyMap(), GraphDatabaseSettings.class ),
                    ID_GENERATOR_FACTORY,
                    pageCache,
                    FILE_SYSTEM,
                    StringLogger.DEV_NULL,
                    new Monitors() ).
                    createEmptyStore( fileName, buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ) );
            return new Store( fileName, pageCache );
        }

        @Override
        protected void rebuildIdGenerator()
        {
        }
    }
}
