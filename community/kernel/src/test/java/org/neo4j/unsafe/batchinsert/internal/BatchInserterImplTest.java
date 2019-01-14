/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.ByteUnit.kibiBytes;

public class BatchInserterImplTest
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final ExpectedException expected = ExpectedException.none();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDirectory )
                                                .around( expected ).around( fileSystemRule );

    @Test
    public void testHonorsPassedInParams() throws Exception
    {
        BatchInserter inserter = BatchInserters.inserter( testDirectory.graphDbDir(), fileSystemRule.get(),
                stringMap( GraphDatabaseSettings.pagecache_memory.name(), "280K" ) );
        NeoStores neoStores = ReflectionUtil.getPrivateField( inserter, "neoStores", NeoStores.class );
        PageCache pageCache = ReflectionUtil.getPrivateField( neoStores, "pageCache", PageCache.class );
        inserter.shutdown();
        long mappedMemoryTotalSize = MuninnPageCache.memoryRequiredForPages( pageCache.maxCachedPages() );
        assertThat( "memory mapped config is active", mappedMemoryTotalSize,
                is( allOf( greaterThan( kibiBytes( 270 ) ), lessThan( kibiBytes( 290 ) ) ) ) );
    }

    @Test
    public void testCreatesStoreLockFile() throws Exception
    {
        // Given
        File file = testDirectory.graphDbDir();

        // When
        BatchInserter inserter = BatchInserters.inserter( file.getAbsoluteFile(), fileSystemRule.get() );

        // Then
        assertThat( new File( file, StoreLocker.STORE_LOCK_FILENAME ).exists(), equalTo( true ) );
        inserter.shutdown();
    }

    @Test
    public void testFailsOnExistingStoreLockFile() throws IOException
    {
        // Given
        File parent = testDirectory.graphDbDir();
        try ( FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
              StoreLocker lock = new StoreLocker( fileSystemAbstraction, parent ) )
        {
            lock.checkLock();

            // Then
            expected.expect( StoreLockException.class );
            expected.expectMessage( "Unable to obtain lock on store lock file" );
            // When
            BatchInserters.inserter( parent.getAbsoluteFile(), fileSystemAbstraction );
        }
    }
}
