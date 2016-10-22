/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.batchinsert.internal;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.StoreLocker;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class BatchInserterImplTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testHonorsPassedInParams() throws Exception
    {
        BatchInserter inserter = BatchInserters.inserter( testDirectory.graphDbDir(), stringMap(
                GraphDatabaseSettings.pagecache_memory.name(), "280K",
                GraphDatabaseSettings.mapped_memory_page_size.name(), "1K" ) );
        NeoStores neoStores = ReflectionUtil.getPrivateField( inserter, "neoStores", NeoStores.class );
        PageCache pageCache = ReflectionUtil.getPrivateField( neoStores, "pageCache", PageCache.class );
        inserter.shutdown();
        int mappedMemoryTotalSize = pageCache.maxCachedPages() * pageCache.pageSize();
        assertThat( "memory mapped config is active", mappedMemoryTotalSize, is( 280 * 1024 ) );
    }

    @Test
    public void testCreatesStoreLockFile() throws Exception
    {
        // Given
        File file = testDirectory.graphDbDir();

        // When
        BatchInserter inserter = BatchInserters.inserter( file.getAbsoluteFile() );

        // Then
        assertThat( new File( file, StoreLocker.STORE_LOCK_FILENAME ).exists(), equalTo( true ) );
        inserter.shutdown();
    }

    @Test
    public void testFailsOnExistingStoreLockFile() throws IOException
    {
        // Given
        File parent = testDirectory.graphDbDir();
        try ( StoreLocker lock = new StoreLocker( new DefaultFileSystemAbstraction() ) )
        {
            lock.checkLock( parent );

            // Then
            expected.expect( StoreLockException.class );
            expected.expectMessage( "Unable to obtain lock on store lock file" );
            // When
            BatchInserters.inserter( parent.getAbsoluteFile() );
        }
    }
}
