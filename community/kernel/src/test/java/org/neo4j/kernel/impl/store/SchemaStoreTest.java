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
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.RecordSerializer;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static java.nio.ByteBuffer.wrap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

public class SchemaStoreTest
{
    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private Config config;
    private SchemaStore store;
    private NeoStores neoStores;
    private StoreFactory storeFactory;

    @Before
    public void before() throws Exception
    {
        File storeDir = new File( "dir" );
        fs.get().mkdirs( storeDir );
        config = new Config();
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs.get() );
        storeFactory = new StoreFactory( storeDir, config, idGeneratorFactory, pageCacheRule.getPageCache( fs.get() ),
                fs.get(), NullLogProvider.getInstance() );
        neoStores = storeFactory.openAllNeoStores( true );
        store = neoStores.getSchemaStore();
    }

    @After
    public void after() throws Exception
    {
        neoStores.close();
    }

    private long storeRule( SchemaRule rule )
    {
        Collection<DynamicRecord> records = store.allocateFrom( rule );
        for ( DynamicRecord record : records )
        {
            store.updateRecord( record );
        }
        return first( records ).getId();
    }

    @Test
    public void serializationAndDeserialization() throws Exception
    {
        // GIVEN
        int propertyKey = 4;
        int labelId = 1;
        IndexRule indexRule = IndexRule.indexRule( store.nextId(), labelId, propertyKey, PROVIDER_DESCRIPTOR );

        // WHEN
        byte[] serialized = new RecordSerializer().append( indexRule ).serialize();
        IndexRule readIndexRule = (IndexRule) SchemaRule.Kind.deserialize( indexRule.getId(), wrap( serialized ) );

        // THEN
        assertEquals( indexRule.getId(), readIndexRule.getId() );
        assertEquals( indexRule.getKind(), readIndexRule.getKind() );
        assertEquals( indexRule.getLabel(), readIndexRule.getLabel() );
        assertEquals( indexRule.getPropertyKey(), readIndexRule.getPropertyKey() );
        assertEquals( indexRule.getProviderDescriptor(), readIndexRule.getProviderDescriptor() );
    }

    @Test
    public void storeAndLoadAllShortRules() throws Exception
    {
        // GIVEN
        Collection<SchemaRule> rules = Arrays.<SchemaRule>asList(
                IndexRule.indexRule( store.nextId(), 0, 5, PROVIDER_DESCRIPTOR ),
                IndexRule.indexRule( store.nextId(), 1, 6, PROVIDER_DESCRIPTOR ),
                IndexRule.indexRule( store.nextId(), 1, 7, PROVIDER_DESCRIPTOR ) );
        for ( SchemaRule rule : rules )
        {
            storeRule( rule );
        }

        // WHEN
        Collection<SchemaRule> readRules = asCollection( store.loadAllSchemaRules() );

        // THEN
        assertEquals( rules, readRules );
    }

//    TODO ENABLE WHEN MULTIPLE PROPERTY KEYS PER INDEX RULE IS SUPPORTED
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
}
