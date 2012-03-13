/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.*;

public class TestStore
{
    public static IdGeneratorFactory ID_GENERATOR_FACTORY =
            CommonFactories.defaultIdGeneratorFactory();
    public static FileSystemAbstraction FILE_SYSTEM =
            CommonFactories.defaultFileSystemAbstraction();
    
    private String path()
    {
        String path = AbstractNeo4jTestCase.getStorePath( "teststore" );
        new File( path ).mkdirs();
        return path;
    }
    
    private String file( String name )
    {
        return path() + File.separator + name;
    }
    
    private String storeFile()
    {
        return file( "testStore.db" );
    }
    
    private String storeIdFile()
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
                Store.createStore( null );
                fail( "Null fileName should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            Store store = Store.createStore( storeFile() );
            try
            {
                Store.createStore( storeFile() );
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
        File file = new File( storeFile() );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( storeIdFile() );
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
            Store.createStore( storeFile() ).close();
            java.nio.channels.FileChannel fileChannel = new java.io.RandomAccessFile(
                storeFile(), "rw" ).getChannel();
            fileChannel.truncate( fileChannel.size() - 2 );
            fileChannel.close();
            Store store = new Store( storeFile() );
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
            Store store = Store.createStore( storeFile() );
            store.close();
        }
        finally
        {
            deleteBothFiles();
        }
    }

    private static class Store extends AbstractStore
    {
        public static final String TYPE_DESCRIPTOR = "TestVersion";
        private static final int RECORD_SIZE = 1;

        public Store( String fileName ) throws IOException
        {
            super( fileName, new Config( StringLogger.DEV_NULL, MapUtil.stringMap(
                    "store_dir", "target/var/teststore" )), IdType.NODE, ID_GENERATOR_FACTORY, FILE_SYSTEM, StringLogger.DEV_NULL);
        }

        public int getRecordSize()
        {
            return RECORD_SIZE;
        }

        public String getTypeDescriptor()
        {
            return TYPE_DESCRIPTOR;
        }

        public static Store createStore( String fileName) throws IOException
        {
            new StoreFactory(new Config(StringLogger.DEV_NULL, Collections.<String,String>emptyMap()), ID_GENERATOR_FACTORY, FILE_SYSTEM, null, StringLogger.DEV_NULL, null).createEmptyStore(fileName, buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ));
            return new Store( fileName );
        }

        protected void rebuildIdGenerator()
        {
        }

        @Override
        public List<WindowPoolStats> getAllWindowPoolStats()
        {
            // TODO Auto-generated method stub
            return null;
        }
    }
}