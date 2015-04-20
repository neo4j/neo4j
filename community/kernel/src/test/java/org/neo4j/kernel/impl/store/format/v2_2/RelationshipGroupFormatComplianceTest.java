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
package org.neo4j.kernel.impl.store.format.v2_2;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.impl.TestStoreIdGenerator;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.standard.StandardStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.impl.store.NeoStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_GROUP_STORE_NAME;
import static org.neo4j.kernel.impl.store.impl.StoreMatchers.records;

public class RelationshipGroupFormatComplianceTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    private StoreFactory storeFactory;
    private final File storeDir = new File( "dir" ).getAbsoluteFile();
    private PageCache pageCache;

    @Before
    public void setup()
    {
        pageCache = pageCacheRule.getPageCache( fsRule.get() );
        Config config = StoreFactory.configForStoreDir( new Config(), storeDir );
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        storeFactory = new StoreFactory(
                config, idGeneratorFactory, pageCache, fsRule.get(), StringLogger.DEV_NULL, new Monitors() );
    }

    @Test
    public void readsRecords() throws Throwable
    {
        // Given
        NeoStore neoStore = storeFactory.createNeoStore();
        RelationshipGroupStore groupStore = neoStore.getRelationshipGroupStore();

        RelationshipGroupRecord expectedRecord = new RelationshipGroupRecord( groupStore.nextId(), 12, 13, 14, 15, 16, true );
        groupStore.updateRecord( expectedRecord );
        neoStore.close();

        // When
        StandardStore<RelationshipGroupRecord, RelationshipGroupStoreFormat_v2_2.RelationshipGroupRecordCursor> store = newStore();
        store.init();
        store.start();

        // Then
        assertThat( records( store ), equalTo( asList( expectedRecord ) ) );
        store.stop();
        store.shutdown();
    }

    @Test
    public void writesRecords() throws Throwable
    {
        // Given
        storeFactory.createNeoStore().close(); // RelGroupStore wont start unless it's child stores exist, so create those

        StandardStore<RelationshipGroupRecord, RelationshipGroupStoreFormat_v2_2.RelationshipGroupRecordCursor> store = newStore();
        store.init();
        store.start();

        RelationshipGroupRecord expectedRecord = new RelationshipGroupRecord( store.allocate(), 12, 13, 14, 15, 16, true );

        // When
        store.write( expectedRecord );
        store.stop();
        store.shutdown();

        // Then
        RelationshipGroupStore legacyStore = storeFactory.newRelationshipGroupStore();
        RelationshipGroupRecord record = legacyStore.getRecord( expectedRecord.getId() );
        assertThat( record, equalTo( expectedRecord ) );
        legacyStore.close();
    }

    private StandardStore<RelationshipGroupRecord, RelationshipGroupStoreFormat_v2_2.RelationshipGroupRecordCursor> newStore()
    {
        return new StandardStore<>( new RelationshipGroupStoreFormat_v2_2(),
                new File( storeDir, DEFAULT_NAME + RELATIONSHIP_GROUP_STORE_NAME ),
                new TestStoreIdGenerator(), pageCache,
                fsRule.get(),
                StringLogger.DEV_NULL );
    }
}
