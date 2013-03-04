/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static java.nio.ByteBuffer.wrap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.util.BytePrinter.hex;
import static org.neo4j.kernel.impl.util.StringLogger.SYSTEM;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule.Kind;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class SchemaStoreTest
{
    @Test
    public void serializationAndDeserialization() throws Exception
    {
        // GIVEN
        long propertyKey = 4;
        int labelId = 0;
        ByteBuffer expected = wrap( new byte[4+1+2+8] );
        expected.putInt( labelId );
        expected.put( Kind.INDEX_RULE.id() );
        expected.putShort( (short) 1 );
        expected.putLong( propertyKey );
        SchemaRule indexRule = new IndexRule( store.nextId(), labelId, propertyKey );

        // WHEN
        Collection<DynamicRecord> records = store.allocateFrom( indexRule );
        long blockId = first( records ).getId();
        for ( DynamicRecord record : records )
            store.updateRecord( record );
        
        // THEN
        byte[] actual = records.iterator().next().getData();

        assertEquals( 1, records.size() );
        assertTrue( "\nExpected: " + hex( expected ) + "\n     Got: " + hex( actual ),
                Arrays.equals( expected.array(), actual ) );

        Collection<DynamicRecord> readRecords = store.getRecords( blockId );
        assertEquals( 1, readRecords.size() );
        assertTrue( Arrays.equals( expected.array(), readRecords.iterator().next().getData() ) );
    }
    
    @Test
    public void storeAndLoadAllShortRules() throws Exception
    {
        // GIVEN
        Collection<SchemaRule> rules = Arrays.<SchemaRule>asList(
                new IndexRule( store.nextId(), 0, 5 ),
                new IndexRule( store.nextId(), 1, 6 ),
                new IndexRule( store.nextId(), 1, 7 ) );
        for ( SchemaRule rule : rules )
            storeRule( rule );

        // WHEN
        Collection<SchemaRule> readRules = asCollection( store.loadAll() );

        // THEN
        assertEquals( rules, readRules );
    }
  
//    ENABLE WHEN MULTIPLE PROPERTY KEYS PER INDEX RULE IS SUPPORTED
//    @Test
//    public void storeAndLoadSingleLongRule() throws Exception
//    {
//        // GIVEN
//
//        Collection<SchemaRule> rules = Arrays.<SchemaRule>asList( createLongIndexRule( 0, 50 ) );
//        for ( SchemaRule rule : rules )
//            storeRule( rule );
//
//        // WHEN
//        Collection<SchemaRule> readRules = asCollection( store.loadAll() );
//
//        // THEN
//        assertEquals( rules, readRules );
//    }
//
//    @Test
//    public void storeAndLoadAllLongRules() throws Exception
//    {
//        // GIVEN
//        Collection<SchemaRule> rules = Arrays.<SchemaRule>asList(
//                createLongIndexRule( 0, 100 ), createLongIndexRule( 1, 6 ), createLongIndexRule( 2, 50 ) );
//        for ( SchemaRule rule : rules )
//            storeRule( rule );
//
//        // WHEN
//        Collection<SchemaRule> readRules = asCollection( store.loadAll() );
//
//        // THEN
//        assertEquals( rules, readRules );
//    }
//
//    private IndexRule createLongIndexRule( long label, int numberOfPropertyKeys )
//    {
//        long[] propertyKeys = new long[numberOfPropertyKeys];
//        for ( int i = 0; i < propertyKeys.length; i++ )
//            propertyKeys[i] = i;
//        return new IndexRule( store.nextId(), label, POPULATING, propertyKeys );
//    }

    private long storeRule( SchemaRule rule )
    {
        Collection<DynamicRecord> records = store.allocateFrom( rule );
        for ( DynamicRecord record : records )
            store.updateRecord( record );
        return first( records ).getId();
    }

    private Config config;
    private SchemaStore store;
    private EphemeralFileSystemAbstraction fileSystemAbstraction;
    private StoreFactory storeFactory;
    
    @Before
    public void before() throws Exception
    {
        config = new Config( stringMap() );
        fileSystemAbstraction = new EphemeralFileSystemAbstraction();
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        DefaultWindowPoolFactory windowPoolFactory = new DefaultWindowPoolFactory();
        storeFactory = new StoreFactory( config, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction, SYSTEM,
                new DefaultTxHook() );
        File file = new File( "schema-store" );
        storeFactory.createSchemaStore( file );
        store = storeFactory.newSchemaStore( file );
    }

    @After
    public void after() throws Exception
    {
        store.close();
        fileSystemAbstraction.shutdown();
    }
}
