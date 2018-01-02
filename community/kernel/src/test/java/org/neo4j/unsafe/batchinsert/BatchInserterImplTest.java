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
package org.neo4j.unsafe.batchinsert;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.StoreLocker;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class BatchInserterImplTest
{
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

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
        BatchInserter inserter = BatchInserters.inserter( file.getAbsolutePath() );

        // Then
        assertThat( new File( file, StoreLocker.STORE_LOCK_FILENAME ).exists(), equalTo( true ) );
        inserter.shutdown();
    }

    @Test
    public void testFailsOnExistingStoreLockFile() throws IOException
    {
        // Given
        File parent = testDirectory.graphDbDir();
        StoreLocker lock = new StoreLocker( new DefaultFileSystemAbstraction() );
        lock.checkLock( parent );

        // When
        try
        {
            BatchInserters.inserter( parent.getAbsolutePath() );

            // Then
            fail();
        }
        catch ( StoreLockException e )
        {
            // OK
        }
        finally
        {
            lock.release();
        }
    }
}
