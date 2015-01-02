/**
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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.IteratorUtil.first;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;

public class TestDynamicStore
{
    public static IdGeneratorFactory ID_GENERATOR_FACTORY =
            new DefaultIdGeneratorFactory();
    public static WindowPoolFactory WINDOW_POOL_FACTORY =
            new DefaultWindowPoolFactory();
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    private File path()
    {
        String path = "dynamicstore";
        File file = new File( path );
        fs.get().mkdirs( file );
        return file;
    }

    private File file( String name )
    {
        return new File( path(), name);
    }

    private File dynamicStoreFile()
    {
        return file( "testDynamicStore.db" );
    }

    private File dynamicStoreIdFile()
    {
        return file( "testDynamicStore.db.id" );
    }

    @Test
    public void testCreateStore()
    {
        try
        {
            try
            {
                createEmptyStore( null, 1 );
                fail( "Null fileName should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            try
            {
                createEmptyStore( dynamicStoreFile(), 0 );
                fail( "Illegal blocksize should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            createEmptyStore( dynamicStoreFile(), 15 );
            try
            {
                createEmptyStore( dynamicStoreFile(), 15 );
                fail( "Creating existing store should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
        }
        finally
        {
            deleteBothFiles();
        }
    }

    private void createEmptyStore( File fileName, int blockSize )
    {
        new StoreFactory( config(), ID_GENERATOR_FACTORY, new DefaultWindowPoolFactory(), fs.get(),
                StringLogger.DEV_NULL, null ).createDynamicArrayStore( fileName, blockSize );
    }

    private DynamicArrayStore newStore()
    {
        return new DynamicArrayStore( dynamicStoreFile(), config(), IdType.ARRAY_BLOCK, ID_GENERATOR_FACTORY,
                WINDOW_POOL_FACTORY, fs.get(), StringLogger.DEV_NULL );
    }

    private void deleteBothFiles()
    {
        File file = dynamicStoreFile();
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = dynamicStoreIdFile();
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
            createEmptyStore( dynamicStoreFile(), 30 );
            StoreChannel fileChannel = fs.get().open( dynamicStoreFile(), "rw" );
            fileChannel.truncate( fileChannel.size() - 2 );
            fileChannel.close();
            DynamicArrayStore store = newStore();
            store.makeStoreOk();
            store.close();
        }
        finally
        {
            deleteBothFiles();
        }
    }

    private Config config()
    {
        return new Config( MapUtil.stringMap(
                "neo_store", dynamicStoreFile().getPath(),
                "store_dir", path().getPath() ), GraphDatabaseSettings.class );
    }

    @Test
    public void testClose()
    {
        try
        {
            createEmptyStore( dynamicStoreFile(), 30 );
            DynamicArrayStore store = newStore();
            Collection<DynamicRecord> records = store.allocateRecordsFromBytes( new byte[10] );
            long blockId = first( records ).getId();
            for ( DynamicRecord record : records )
            {
                store.updateRecord( record );
            }
            store.close();
            /*
             * try { store.allocateRecords( blockId, new byte[10] ); fail(
             * "Closed store should throw exception" ); } catch (
             * RuntimeException e ) { // good }
             */
            try
            {
                store.getArrayFor( store.getRecords( blockId ) );
                fail( "Closed store should throw exception" );
            }
            catch ( RuntimeException e )
            { // good
            }
            try
            {
                store.getLightRecords( 0 );
                fail( "Closed store should throw exception" );
            }
            catch ( RuntimeException e )
            { // good
            }
        }
        finally
        {
            deleteBothFiles();
        }
    }

    @Test
    public void testStoreGetCharsFromString()
    {
        try
        {
            final String STR = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            createEmptyStore( dynamicStoreFile(), 30 );
            DynamicArrayStore store = newStore();
            char[] chars = new char[STR.length()];
            STR.getChars( 0, STR.length(), chars, 0 );
            Collection<DynamicRecord> records = store.allocateRecords( chars );
            for ( DynamicRecord record : records )
            {
                store.updateRecord( record );
            }
            // assertEquals( STR, new String( store.getChars( blockId ) ) );
            store.close();
        }
        finally
        {
            deleteBothFiles();
        }
    }

    @Test
    public void testRandomTest()
    {
        Random random = new Random( System.currentTimeMillis() );
        createEmptyStore( dynamicStoreFile(), 30 );
        DynamicArrayStore store = newStore();
        ArrayList<Long> idsTaken = new ArrayList<Long>();
        Map<Long, byte[]> byteData = new HashMap<Long, byte[]>();
        float deleteIndex = 0.2f;
        float closeIndex = 0.1f;
        int currentCount = 0;
        int maxCount = 128;
        Set<Long> set = new HashSet<Long>();
        try
        {
            while ( currentCount < maxCount )
            {
                float rIndex = random.nextFloat();
                if ( rIndex < deleteIndex && currentCount > 0 )
                {
                    long blockId = idsTaken.remove(
                            random.nextInt( currentCount ) );
                    store.getLightRecords( blockId );
                    byte[] bytes = (byte[]) store.getArrayFor( store.getRecords( blockId ) );
                    validateData( bytes, byteData.remove( blockId ) );
                    Collection<DynamicRecord> records = store
                            .getLightRecords( blockId );
                    for ( DynamicRecord record : records )
                    {
                        record.setInUse( false );
                        store.updateRecord( record );
                        set.remove( record.getId() );
                    }
                    currentCount--;
                }
                else
                {
                    byte bytes[] = createRandomBytes( random );
                    Collection<DynamicRecord> records = store.allocateRecords( bytes );
                    for ( DynamicRecord record : records )
                    {
                        assert !set.contains( record.getId() );
                        store.updateRecord( record );
                        set.add( record.getId() );
                    }
                    long blockId = first( records ).getId();
                    idsTaken.add( blockId );
                    byteData.put( blockId, bytes );
                    currentCount++;
                }
                if ( rIndex > (1.0f - closeIndex) || rIndex < closeIndex )
                {
                    store.close();
                    store = newStore();
                }
            }
        }
        finally
        {
            store.close();
            deleteBothFiles();
        }
    }

//    private static class ByteStore extends AbstractDynamicStore
//    {
//        // store version, each store ends with this string (byte encoded)
//        private static final String VERSION = "DynamicTestVersion v0.1";
//
//        public ByteStore( String fileName, IdGeneratorFactory idGenerator, String storeDir )
//        {
//            super( fileName, MapUtil.map( "neo_store", fileName,
//                    IdGeneratorFactory.class, idGenerator, "store_dir", storeDir ), IdType.ARRAY_BLOCK );
//        }
//
//        public String getTypeDescriptor()
//        {
//            return VERSION;
//        }
//
//        public static ByteStore createStore( String fileName, int blockSize, String storeDir )
//        {
//            createEmptyStore( fileName, blockSize, VERSION, ID_GENERATOR_FACTORY,
//                    IdType.ARRAY_BLOCK );
//            return new ByteStore( fileName, ID_GENERATOR_FACTORY, storeDir );
//        }
//
//        public byte[] getBytes( long blockId )
//        {
//            return null;
////            return get( blockId );
//        }
//
//        // public char[] getChars( int blockId ) throws IOException
//        // {
//        // return getAsChar( blockId );
//        // }
//
////        public void flush()
////        {
////        }
//    }

    private byte[] createBytes( int length )
    {
        return new byte[length];
    }

    private byte[] createRandomBytes( Random r )
    {
        return new byte[r.nextInt( 1024 )];
    }

    private void validateData( byte data1[], byte data2[] )
    {
        assertEquals( data1.length, data2.length );
        for ( int i = 0; i < data1.length; i++ )
        {
            assertEquals( data1[i], data2[i] );
        }
    }

    private long create( DynamicArrayStore store, Object arrayToStore )
    {
        Collection<DynamicRecord> records = store.allocateRecords( arrayToStore );
        for ( DynamicRecord record : records )
        {
            store.updateRecord( record );
        }
        return first( records ).getId();
    }

    @Test
    public void testAddDeleteSequenceEmptyNumberArray()
    {
        createEmptyStore( dynamicStoreFile(), 30 );
        DynamicArrayStore store = newStore();
        try
        {
            byte[] emptyToWrite = createBytes( 0 );
            long blockId = create( store, emptyToWrite );
            store.getLightRecords( blockId );
            byte[] bytes = (byte[]) store.getArrayFor( store.getRecords( blockId ) );
            assertEquals( 0, bytes.length );

            Collection<DynamicRecord> records = store.getLightRecords( blockId );
            for ( DynamicRecord record : records )
            {
                record.setInUse( false );
                store.updateRecord( record );
            }
        }
        finally
        {
            store.close();
            deleteBothFiles();
        }
    }

    @Test
    public void testAddDeleteSequenceEmptyStringArray()
    {
        createEmptyStore( dynamicStoreFile(), 30 );
        DynamicArrayStore store = newStore();
        try
        {
            long blockId = create( store, new String[0] );
            store.getLightRecords( blockId );
            String[] readBack = (String[]) store.getArrayFor( store.getRecords( blockId ) );
            assertEquals( 0, readBack.length );

            Collection<DynamicRecord> records = store.getLightRecords( blockId );
            for ( DynamicRecord record : records )
            {
                record.setInUse( false );
                store.updateRecord( record );
            }
        }
        finally
        {
            store.close();
            deleteBothFiles();
        }
    }
}
