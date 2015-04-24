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
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.impl.TestStoreIdGenerator;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.standard.StandardStore;
import org.neo4j.kernel.impl.logging.NullLogProvider;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.impl.store.NeoStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_STORE_NAME;
import static org.neo4j.kernel.impl.store.impl.StoreMatchers.records;

/**
 * Test that the rel store format can read and write the format generated by
 * {@link org.neo4j.kernel.impl.store.RelationshipStore}.
 */
public class RelationshipFormatComplianceTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    private PageCache pageCache;
    private StoreFactory storeFactory;
    private final File storeDir = new File( "dir" ).getAbsoluteFile();

    @Before
    public void setup()
    {
        pageCache = pageCacheRule.getPageCache( fsRule.get() );
        storeFactory = new StoreFactory( StoreFactory.configForStoreDir( new Config(), storeDir ), new DefaultIdGeneratorFactory(), pageCache, fsRule.get(), NullLogProvider.getInstance(), new Monitors() );
    }

    @Test
    public void readsRecords() throws Throwable
    {
        // Given
        NeoStore neoStore = storeFactory.createNeoStore();
        RelationshipStore relStore = neoStore.getRelationshipStore();

        RelationshipRecord expectedRecord = new RelationshipRecord( relStore.nextId(), 1337, 1, 2 );
        expectedRecord.setInUse( true );
        relStore.updateRecord( expectedRecord );
        neoStore.close();

        // When
        RelationshipStoreFormat_v2_2 format = new RelationshipStoreFormat_v2_2();
        File fileName = new File( storeDir, DEFAULT_NAME + RELATIONSHIP_STORE_NAME );
        TestStoreIdGenerator idGenerator = new TestStoreIdGenerator();
        StandardStore<RelationshipRecord, RelationshipStoreFormat_v2_2.RelationshipRecordCursor> store =
                new StandardStore<>( format, fileName, idGenerator, pageCache, fsRule.get(), NullLogProvider.getInstance() );
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
        storeFactory.createNeoStore().close(); // NodeStore wont start unless it's child stores exist, so create those

        RelationshipStoreFormat_v2_2 format = new RelationshipStoreFormat_v2_2();
        File fileName = new File( storeDir, DEFAULT_NAME + RELATIONSHIP_STORE_NAME );
        TestStoreIdGenerator idGenerator = new TestStoreIdGenerator();
        StandardStore<RelationshipRecord, RelationshipStoreFormat_v2_2.RelationshipRecordCursor> store =
                new StandardStore<>( format, fileName, idGenerator, pageCache, fsRule.get(), NullLogProvider.getInstance() );
        store.init();
        store.start();

        RelationshipRecord expectedRecord = new RelationshipRecord( store.allocate(), 1337, 1, 2 );
        expectedRecord.setInUse( true );

        // When
        store.write( expectedRecord );
        store.stop();
        store.shutdown();

        // Then
        RelationshipStore relStore = storeFactory.newRelationshipStore();
        RelationshipRecord record = relStore.getRecord( expectedRecord.getId() );
        assertThat( record, equalTo( expectedRecord ) );
        relStore.close();
    }
}
