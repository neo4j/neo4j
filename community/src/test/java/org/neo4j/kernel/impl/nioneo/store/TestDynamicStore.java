/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestDynamicStore
{
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
                ByteStore.createStore( null, 1 );
                fail( "Null fileName should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            try
            {
                ByteStore.createStore( dynamicStoreFile(), 0 );
                fail( "Illegal blocksize should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            ByteStore store = ByteStore.createStore( dynamicStoreFile(), 30 );
            try
            {
                ByteStore.createStore( dynamicStoreFile(), 15 );
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
            ByteStore.createStore( dynamicStoreFile(), 30 ).close();
            java.nio.channels.FileChannel fileChannel = new java.io.RandomAccessFile(
                dynamicStoreFile(), "rw" ).getChannel();
            fileChannel.truncate( fileChannel.size() - 2 );
            fileChannel.close();
            ByteStore store = new ByteStore( dynamicStoreFile() );
            store.makeStoreOk();
            store.close();
        }
        finally
        {
            log.setLevel( level );
            deleteBothFiles();
        }
    }

    @Test
    public void testClose()
    {
        try
        {
            ByteStore store = ByteStore.createStore( dynamicStoreFile(), 30 );
            int blockId = store.nextBlockId();
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
                store.getBytes( 0 );
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
            ByteStore store = ByteStore.createStore( dynamicStoreFile(), 30 );
            int blockId = store.nextBlockId();
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
        ByteStore store = ByteStore.createStore( dynamicStoreFile(), 30 );
        java.util.ArrayList<Integer> idsTaken = new java.util.ArrayList<Integer>();
        java.util.Map<Integer,byte[]> byteData = new java.util.HashMap<Integer,byte[]>();
        float deleteIndex = 0.2f;
        float closeIndex = 0.1f;
        int currentCount = 0;
        int maxCount = 128;
        java.util.HashSet<Integer> set = new java.util.HashSet<Integer>();
        try
        {
            while ( currentCount < maxCount )
            {
                float rIndex = random.nextFloat();
                if ( rIndex < deleteIndex && currentCount > 0 )
                {
                    int blockId = idsTaken.remove(
                        random.nextInt( currentCount ) ).intValue();
                    store.getLightRecords( blockId );
                    validateData( store.getBytes( blockId ), byteData
                        .remove( new Integer( blockId ) ) );
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
                    int blockId = store.nextBlockId();
                    Collection<DynamicRecord> records = store.allocateRecords(
                        blockId, bytes );
                    for ( DynamicRecord record : records )
                    {
                        assert !set.contains( record.getId() );
                        store.updateRecord( record );
                        set.add( record.getId() );
                    }
                    idsTaken.add( new Integer( blockId ) );
                    byteData.put( new Integer( blockId ), bytes );
                    currentCount++;
                }
                if ( rIndex > (1.0f - closeIndex) || rIndex < closeIndex )
                {
                    store.close();
                    store = new ByteStore( dynamicStoreFile() );
                }
            }
        }
        finally
        {
            store.close();
            deleteBothFiles();
        }
    }

    private static class ByteStore extends AbstractDynamicStore
    {
        // store version, each store ends with this string (byte encoded)
        private static final String VERSION = "DynamicTestVersion v0.1";

        public ByteStore( String fileName )
        {
            super( fileName );
        }

        public String getTypeAndVersionDescriptor()
        {
            return VERSION;
        }

        public static ByteStore createStore( String fileName, int blockSize )
        {
            createEmptyStore( fileName, blockSize, VERSION, IdGeneratorFactory.DEFAULT,
                    IdType.ARRAY_BLOCK );
            return new ByteStore( fileName );
        }

        public byte[] getBytes( int blockId )
        {
            return get( blockId );
        }
        
        @Override
        protected IdType getIdType()
        {
            return IdType.ARRAY_BLOCK;
        }

        // public char[] getChars( int blockId ) throws IOException
        // {
        // return getAsChar( blockId );
        // }

//        public void flush()
//        {
//        }
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
}