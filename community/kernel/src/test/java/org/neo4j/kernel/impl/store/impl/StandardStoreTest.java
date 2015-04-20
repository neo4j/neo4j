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
package org.neo4j.kernel.impl.store.impl;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.Store;
import org.neo4j.kernel.impl.store.format.TestCursor;
import org.neo4j.kernel.impl.store.format.TestFormatWithHeader;
import org.neo4j.kernel.impl.store.format.TestHeaderlessStoreFormat;
import org.neo4j.kernel.impl.store.format.TestRecord;
import org.neo4j.kernel.impl.store.standard.StandardStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class StandardStoreTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    private LifeSupport life;
    private PageCache pageCache;

    @Before
    public void setup()
    {
        life = new LifeSupport();
        life.start();
        pageCache = pageCacheRule.getPageCache( fsRule.get() );
    }

    @Test
    public void shouldReadRecords() throws Throwable
    {
        // Given
        Store<TestRecord, TestCursor> store = life.add(new StandardStore<>( new TestHeaderlessStoreFormat(), new File("/store"),
                new TestStoreIdGenerator(), pageCache, fsRule.get(), StringLogger.DEV_NULL ));

        long firstId = store.allocate();
        long secondId = store.allocate();

        store.write( new TestRecord( firstId, 1337) );
        store.write( new TestRecord( secondId, 1338) );

        // When
        TestRecord firstRecord = store.read( firstId );
        TestRecord secondRecord = store.read( secondId );

        // Then
        assertThat(firstRecord.value, equalTo(1337l));
        assertThat(secondRecord.value, equalTo(1338l));

        // And when I restart the store
        life.shutdown();

        store = new StandardStore<>( new TestHeaderlessStoreFormat(), new File("/store"),
                new TestStoreIdGenerator(), pageCache, fsRule.get(), StringLogger.DEV_NULL );
        store.init();
        store.start();

        assertThat( StoreMatchers.records( store ),
           equalTo( asList( new TestRecord( firstId, 1337 ), new TestRecord( secondId, 1338 ) ) ) );
        store.stop();
        store.shutdown();
    }

    @Test
    public void shouldAllowStoresWithHeaders() throws Throwable
    {
        // Given
        Store<TestRecord, TestCursor> store = life.add(new StandardStore<>( new TestFormatWithHeader(14), new File("/store"),
                new TestStoreIdGenerator(), pageCache, fsRule.get(), StringLogger.DEV_NULL ));

        long recordId = store.allocate();

        store.write( new TestRecord( recordId, 1338 ) );

        // When
        TestRecord secondRecord = store.read( recordId );

        // Then
        assertThat( secondRecord.value, equalTo( 1338l ) );

        // And when I restart the store
        life.shutdown();

        store = new StandardStore<>( new TestFormatWithHeader(14), new File("/store"),
                new TestStoreIdGenerator(), pageCache, fsRule.get(), StringLogger.DEV_NULL );
        store.init();
        store.start();

        assertThat( StoreMatchers.records( store ), equalTo( asList( new TestRecord( recordId, 1338 ) ) ) );
        store.stop();
        store.shutdown();
    }

    @Test
    public void shouldAllowRunningCursorBackwards() throws Throwable
    {
        // Given
        Store<TestRecord, TestCursor> store = life.add(new StandardStore<>( new TestHeaderlessStoreFormat(), new File("/store"),
                new TestStoreIdGenerator(), pageCache, fsRule.get(), StringLogger.DEV_NULL ));

        long firstId  = store.allocate();
        long secondId = store.allocate();
        long thirdId  = store.allocate(); // One id that we'll leave unused
        long fourthId = store.allocate();

        store.write( new TestRecord( firstId, 1337) );
        store.write( new TestRecord( secondId, 1338) );
        // thirdId missing here on purpose, to include a gap in the store, to make sure we don't see that record
        store.write( new TestRecord( fourthId, 1338) );

        // When
        TestCursor cursor = store.cursor(Store.SF_REVERSE_CURSOR);

        // Then
        assertTrue(cursor.next());
        assertEquals(fourthId, cursor.recordId());
        assertTrue(cursor.next());
        assertEquals(secondId, cursor.recordId());
        assertTrue(cursor.next());
        assertEquals(firstId, cursor.recordId());
        assertFalse(cursor.next());
        store.stop();
        store.shutdown();
    }
}


