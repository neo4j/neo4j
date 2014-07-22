/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.StoreLocker;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class BatchInserterImplTest
{
    @Test
    public void testHonorsPassedInParams() throws Exception
    {
        Long mappedMemoryTotalSize = createInserterAndGetMemoryMappingConfig( stringMap(
                GraphDatabaseSettings.mapped_memory_total_size.name(), "16K" ) );
        assertThat( "memory mapped config is active", mappedMemoryTotalSize, is( 16 * 1024L ) );
    }

    @Test
    public void testCreatesStoreLockFile()
    {
        // Given
        File file = TargetDirectory.forTest( getClass() ).makeGraphDbDir();

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
        File parent = TargetDirectory.forTest( getClass() ).makeGraphDbDir();
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
    
    private Long createInserterAndGetMemoryMappingConfig( Map<String, String> initialConfig ) throws Exception
    {
        BatchInserter inserter = BatchInserters.inserter(
                TargetDirectory.forTest( getClass() ).makeGraphDbDir().getAbsolutePath(), initialConfig );
        NeoStore neoStore = ReflectionUtil.getPrivateField( inserter, "neoStore", NeoStore.class );
        Config config = ReflectionUtil.getPrivateField( neoStore, "conf", Config.class );
        inserter.shutdown();
        return config.get( GraphDatabaseSettings.mapped_memory_total_size );
    }
}
