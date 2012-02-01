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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestDynamicStore
{
    public static IdGeneratorFactory ID_GENERATOR_FACTORY =
            CommonFactories.defaultIdGeneratorFactory();

    private String path()
    {
        String path = AbstractNeo4jTestCase.getStorePath( "dynamicstore" );
        new File( path ).mkdirs();
        return path;
    }

    private String file( String name )
    {
        return path() + File.separator + name;
    }

    private String dynamicStoreFile()
    {
        return file( "testDynamicStore.db" );
    }

    private String dynamicStoreIdFile()
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

    private void createEmptyStore( String fileName, int blockSize )
    {
        DynamicArrayStore.createEmptyStore( fileName, blockSize,
                DynamicArrayStore.VERSION, ID_GENERATOR_FACTORY,
                IdType.ARRAY_BLOCK );
    }

    private DynamicArrayStore newStore()
    {
        return new DynamicArrayStore( dynamicStoreFile(), config(), IdType.ARRAY_BLOCK );
    }

    private void deleteBothFiles()
    {
        File file = new File( dynamicStoreFile() );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( dynamicStoreIdFile() );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
    }

    @Test
    public void testStickyStore() throws IOException
    {
        Logger log = Logger.getLogger( CommonAbstractStore.class.getName() );
        Level level = log.getLevel();
        try
        {
            log.setLevel( Level.OFF );
            createEmptyStore( dynamicStoreFile(), 30 );
            FileChannel fileChannel = new RandomAccessFile( dynamicStoreFile(), "rw" ).getChannel();
            fileChannel.truncate( fileChannel.size() - 2 );
            fileChannel.close();
            DynamicArrayStore store = newStore();
            store.makeStoreOk();
            store.close();
        }
        finally
        {
            log.setLevel( level );
            deleteBothFiles();
        }
    }

    private Map<?, ?> config()
    {
        return map(
                "neo_store", dynamicStoreFile(),
                IdGeneratorFactory.class, ID_GENERATOR_FACTORY,
                "store_dir", path(),
                FileSystemAbstraction.class, CommonFactories.defaultFileSystemAbstraction() );
    }

    @Test
    public void testClose()
    {
        try
        {
            createEmptyStore( dynamicStoreFile(), 30 );
            DynamicArrayStore store = newStore();
            long blockId = store.nextBlockId();
            Collection<DynamicRecord> records = store.allocateRecords( blockId,
                new byte[10] );
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
                PropertyStore.getArrayFor( blockId, store.getRecords( blockId ), store );
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
            long blockId = store.nextBlockId();
            char[] chars = new char[STR.length()];
            STR.getChars( 0, STR.length(), chars, 0 );
            Collection<DynamicRecord> records = store.allocateRecords( blockId,
                chars );
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
        Map<Long,byte[]> byteData = new HashMap<Long,byte[]>();
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
                    byte[] bytes = (byte[]) PropertyStore.getArrayFor( blockId, store.getRecords( blockId ), store );
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
                    long blockId = store.nextBlockId();
                    Collection<DynamicRecord> records = store.allocateRecords(
                        blockId, (Object) bytes );
                    for ( DynamicRecord record : records )
                    {
                        assert !set.contains( record.getId() );
                        store.updateRecord( record );
                        set.add( record.getId() );
                    }
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
        long blockId = store.nextBlockId();
        Collection<DynamicRecord> records = store.allocateRecords( blockId,
                arrayToStore );
        for ( DynamicRecord record : records )
        {
            store.updateRecord( record );
        }
        return blockId;
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
            byte[] bytes = (byte[]) PropertyStore.getArrayFor( blockId, store.getRecords( blockId ), store );
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
            String[] readBack = (String[]) PropertyStore.getArrayFor( blockId,
                    store.getRecords( blockId ), store );
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
