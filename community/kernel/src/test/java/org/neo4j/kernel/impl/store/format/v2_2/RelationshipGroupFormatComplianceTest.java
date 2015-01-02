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
package org.neo4j.kernel.impl.store.format.v2_2;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
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
    private StoreFactory storeFactory;
    private final File storeDir = new File( "dir" ).getAbsoluteFile();

    @Before
    public void setup()
    {
        MuninnPageCache pageCache = new MuninnPageCache( fsRule.get(), 1024, 1024, PageCacheMonitor.NULL );
        storeFactory = new StoreFactory( StoreFactory.configForStoreDir( new Config(), storeDir ), new DefaultIdGeneratorFactory(), pageCache, fsRule.get(), StringLogger.DEV_NULL, new Monitors() );
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
    }

    private StandardStore<RelationshipGroupRecord, RelationshipGroupStoreFormat_v2_2.RelationshipGroupRecordCursor> newStore()
    {
        return new StandardStore<>( new RelationshipGroupStoreFormat_v2_2(),
                new File( storeDir, DEFAULT_NAME + RELATIONSHIP_GROUP_STORE_NAME ),
                new TestStoreIdGenerator(), new MuninnPageCache( fsRule.get(), 1024, 1024, PageCacheMonitor.NULL ),
                fsRule.get(),
                StringLogger.DEV_NULL );
    }
}
