/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.IteratorUtil.first;

public class TestDynamicStore
{
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    private StoreFactory storeFactory;
    private NeoStores neoStores;
    private Config config;

    @Before
    public void setUp()
    {
        File storeDir = new File( "dynamicstore" );
        fs.get().mkdir( storeDir );
        config = config();
        storeFactory = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fs.get() ),
                pageCacheRule.getPageCache( fs.get() ), fs.get(), NullLogProvider.getInstance() );
    }

    @After
    public void tearDown()
    {
        if ( neoStores != null )
        {
            neoStores.close();
        }
    }

    @Test( expected = InvalidSettingException.class )
    public void stringStoreCannotHaveZeroBlockSize()
    {
        config.setProperty( StoreFactory.Configuration.string_block_size.name(), "0" );
        fail( "Illegal blocksize should throw exception" );
    }

    @Test( expected = InvalidSettingException.class )
    public void arrayStoreCannotHaveZeroBlockSize() throws Exception
    {
        config.setProperty( StoreFactory.Configuration.array_block_size.name(), "0" );
    }

    private DynamicArrayStore createDynamicArrayStore()
    {
        neoStores = storeFactory.openAllNeoStores( true );
        return neoStores.getPropertyStore().getArrayStore();
    }

    private Config config()
    {
        return new Config( MapUtil.stringMap(), GraphDatabaseSettings.class );
    }

    @Test
    public void testClose()
    {
        DynamicArrayStore store = createDynamicArrayStore();
        Collection<DynamicRecord> records = new ArrayList<>();
        store.allocateRecordsFromBytes( records, new byte[10] );
        long blockId = first( records ).getId();
        for ( DynamicRecord record : records )
        {
            store.updateRecord( record );
        }
        neoStores.close();
        neoStores = null;
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

    @Test
    public void testStoreGetCharsFromString()
    {
        final String STR = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        DynamicArrayStore store = createDynamicArrayStore();
        char[] chars = new char[STR.length()];
        STR.getChars( 0, STR.length(), chars, 0 );
        Collection<DynamicRecord> records = new ArrayList<>();
        store.allocateRecords( records, chars, IteratorUtil.<DynamicRecord>emptyIterator() );
        for ( DynamicRecord record : records )
        {
            store.updateRecord( record );
        }
        // assertEquals( STR, new String( store.getChars( blockId ) ) );
    }

    @Test
    public void testRandomTest()
    {
        Random random = new Random( System.currentTimeMillis() );
        DynamicArrayStore store = createDynamicArrayStore();
        ArrayList<Long> idsTaken = new ArrayList<>();
        Map<Long, byte[]> byteData = new HashMap<>();
        float deleteIndex = 0.2f;
        float closeIndex = 0.1f;
        int currentCount = 0;
        int maxCount = 128;
        Set<Long> set = new HashSet<>();
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
                Collection<DynamicRecord> records = new ArrayList<>();
                store.allocateRecords( records, bytes, IteratorUtil.<DynamicRecord>emptyIterator() );
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
                neoStores.close();
                store = createDynamicArrayStore();
            }
        }
    }

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
        Collection<DynamicRecord> records = new ArrayList<>();
        store.allocateRecords( records, arrayToStore, IteratorUtil.<DynamicRecord>emptyIterator() );
        for ( DynamicRecord record : records )
        {
            store.updateRecord( record );
        }
        return first( records ).getId();
    }

    @Test
    public void testAddDeleteSequenceEmptyNumberArray()
    {
        DynamicArrayStore store = createDynamicArrayStore();
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

    @Test
    public void testAddDeleteSequenceEmptyStringArray()
    {
        DynamicArrayStore store = createDynamicArrayStore();
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
}
