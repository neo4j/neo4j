/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.helpers.Settings.osIsWindows;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class BatchInserterImplTest
{
    private void assumeNotWindows()
    {
        // Windows doesn't work well at all with memory mapping. The problem being that
        // in Java there's no way to unmap a memory mapping from a file, instead that
        // is handed over to GC and GC isn't deterministic. Well, actually there is a way
        // unmap if using reflection. Anyways Windows has problems with truncating a file
        // or similar if a memory mapped section of it is still open, i.e. hasn't yet
        // been GCed... which may happen from time to time.
        assumeTrue( !osIsWindows() );
    }
    
    @Test
    public void testHonorsPassedInParams() throws Exception
    {
        assumeNotWindows();
        
        Boolean memoryMappingConfig = createInserterAndGetMemoryMappingConfig( stringMap( GraphDatabaseSettings
                .use_memory_mapped_buffers.name(), "true" ) );
        assertTrue( "memory mapped config is active", memoryMappingConfig );
    }

    @Test
    public void testDefaultsToNoMemoryMapping() throws Exception
    {
        assumeNotWindows();
        
        Boolean memoryMappingConfig = createInserterAndGetMemoryMappingConfig( stringMap() );
        assertFalse( "memory mapped config is active", memoryMappingConfig );
    }

    @Test
    public void testCreatesStoreLockFile()
    {
        // Given
        File file = TargetDirectory.forTest( getClass() ).graphDbDir( true );

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
        File parent = TargetDirectory.forTest( getClass() ).graphDbDir( true );
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
            // Ok
            e.printStackTrace();
        } finally
        {
            lock.release();
        }
    }
    
    private Boolean createInserterAndGetMemoryMappingConfig( Map<String, String> initialConfig ) throws Exception
    {
        BatchInserter inserter = BatchInserters.inserter(
                TargetDirectory.forTest( getClass() ).graphDbDir( true ).getAbsolutePath(), initialConfig );
        NeoStore neoStore = ReflectionUtil.getPrivateField( inserter, "neoStore", NeoStore.class );
        Config config = ReflectionUtil.getPrivateField( neoStore, "conf", Config.class );
        inserter.shutdown();
        return config.get( GraphDatabaseSettings.use_memory_mapped_buffers );
    }
}
