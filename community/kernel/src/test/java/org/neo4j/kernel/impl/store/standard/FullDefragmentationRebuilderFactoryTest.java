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
package org.neo4j.kernel.impl.store.standard;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.Store;
import org.neo4j.kernel.impl.store.format.TestCursor;
import org.neo4j.kernel.impl.store.format.TestHeaderlessStoreFormat;
import org.neo4j.kernel.impl.store.format.TestRecord;
import org.neo4j.kernel.impl.store.impl.TestStoreIdGenerator;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class FullDefragmentationRebuilderFactoryTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    @Test
    public void shouldFindHighestInUseWhenThereAreUnusedRecordsAtEndOfFile() throws Throwable
    {
        // Given
        EphemeralFileSystemAbstraction fs = fsRule.get();
        File path = new File( "/store.db" );
        PageCache cache = pageCacheRule.getPageCache( fsRule.get() );
        Store<TestRecord, TestCursor> store = newStore( fs, path, cache );

        // Build our store
        store.write( new TestRecord(2, 1) );
        store.write( new TestRecord(4, 1) ); // This is the high-id record we expect this strategy to find
        store.write( new TestRecord(1001, 0) ); // 0 here marks this record as not in use

        // Make sure file is flushed to disk
        cache.flushAndForce();

        StoreIdGenerator idGenerator = spy( new TestStoreIdGenerator() );
        StoreToolkit toolkit = mock(StoreToolkit.class);
        when(toolkit.fileSize()).thenReturn( 1001 * 8l );
        when(toolkit.recordSize()).thenReturn( 8 );

        IdGeneratorRebuilder.FullDefragmentationRebuilderFactory rebuilder = new IdGeneratorRebuilder.FullDefragmentationRebuilderFactory();

        // When
        rebuilder.newIdGeneratorRebuilder( store, toolkit, idGenerator ).rebuildIdGenerator();
        store.stop();
        store.shutdown();

        // Then
        verify( idGenerator ).free( 0 );
        verify( idGenerator ).free( 1 );
        verify( idGenerator ).free( 3 );
        verify( idGenerator ).rebuild( 4 );
        verify( idGenerator ).highestIdInUse();
        verifyNoMoreInteractions( idGenerator );
    }

    @Test
    public void shouldNotGetTrippedUpIfLastRecordIsInUse() throws Throwable
    {
        // Given
        EphemeralFileSystemAbstraction fs = fsRule.get();
        File path = new File( "/store.db" );
        PageCache cache = pageCacheRule.getPageCache( fsRule.get() );
        Store<TestRecord, TestCursor> store = newStore( fs, path, cache );

        // Build our store
        store.write( new TestRecord(2, 1) );
        store.write( new TestRecord(4, 1) ); // This is the high-id record we expect this strategy to find


        // Make sure file is flushed to disk
        cache.flushAndForce();

        StoreIdGenerator idGenerator = spy( new TestStoreIdGenerator() );
        StoreToolkit toolkit = mock(StoreToolkit.class);
        when(toolkit.fileSize()).thenReturn( 4 * 8l );
        when(toolkit.recordSize()).thenReturn( 8 );

        IdGeneratorRebuilder.FullDefragmentationRebuilderFactory rebuilder = new IdGeneratorRebuilder.FullDefragmentationRebuilderFactory();

        // When
        rebuilder.newIdGeneratorRebuilder( store, toolkit, idGenerator ).rebuildIdGenerator();
        store.stop();
        store.shutdown();

        // Then
        verify( idGenerator ).free( 0 );
        verify( idGenerator ).free( 1 );
        verify( idGenerator ).free( 3 );
        verify( idGenerator ).rebuild( 4 );
        verify( idGenerator ).highestIdInUse();
        verifyNoMoreInteractions( idGenerator );
    }

    @Test
    public void shouldIgnoreHeaderRecords() throws Throwable
    {
        // Given
        EphemeralFileSystemAbstraction fs = fsRule.get();
        File path = new File( "/store.db" );
        PageCache cache = pageCacheRule.getPageCache( fsRule.get() );
        Store<TestRecord, TestCursor> store = newStore( fs, path, cache );

        // Build our store
        store.write( new TestRecord(2, 1) ); // Add a record at id 2, to emulate a header that happens to have a byte that aligns with the in_use byte

        // Make sure file is flushed to disk
        cache.flushAndForce();

        StoreIdGenerator idGenerator = spy( new TestStoreIdGenerator() );

        StoreToolkit toolkit = mock(StoreToolkit.class);
        when(toolkit.fileSize()).thenReturn( 100 * 8l );
        when(toolkit.recordSize()).thenReturn( 8 );
        when(toolkit.firstRecordId()).thenReturn( 4l ); // 0-3 == header records

        IdGeneratorRebuilder.FullDefragmentationRebuilderFactory rebuilder = new IdGeneratorRebuilder.FullDefragmentationRebuilderFactory();

        // When
        rebuilder.newIdGeneratorRebuilder( store, toolkit, idGenerator ).rebuildIdGenerator();
        store.stop();
        store.shutdown();

        // Then
        verify( idGenerator ).rebuild( 4 );
    }

    private Store<TestRecord, TestCursor> newStore( EphemeralFileSystemAbstraction fs, File path, PageCache
            cache ) throws Throwable
    {
        Store<TestRecord, TestCursor> store = new StandardStore<>( new TestHeaderlessStoreFormat(),
                path, new TestStoreIdGenerator(), cache,
                fs, StringLogger.DEV_NULL );
        store.init();
        store.start();
        return store;
    }
}
