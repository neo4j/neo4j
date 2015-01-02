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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.nodestore_mapped_memory_size;
import static org.neo4j.helpers.Settings.osIsWindows;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;

import org.junit.Test;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

public class TestGrowingFileMemoryMapping
{
    private static final int MEGA = 1024 * 1024;

    @Test
    public void shouldGrowAFileWhileContinuingToMemoryMapNewRegions() throws Exception
    {
        // don't run on windows because memory mapping doesn't work properly there
        assumeTrue( !osIsWindows() );

        // given
        int NUMBER_OF_RECORDS = 1000000;

        File storeDir = TargetDirectory.forTest( getClass() ).makeGraphDbDir();
        Config config = new Config( stringMap(
                nodestore_mapped_memory_size.name(), mmapSize( NUMBER_OF_RECORDS, NodeStore.RECORD_SIZE ),
                NodeStore.Configuration.use_memory_mapped_buffers.name(), "true",
                NodeStore.Configuration.store_dir.name(), storeDir.getPath() ), NodeStore.Configuration.class );
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        StoreFactory storeFactory = new StoreFactory( config, idGeneratorFactory,
                new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(), StringLogger.DEV_NULL,
                new DefaultTxHook() );

        File fileName = new File( storeDir, NeoStore.DEFAULT_NAME + ".nodestore.db" );
        storeFactory.createEmptyStore( fileName, storeFactory.buildTypeDescriptorAndVersion(
                NodeStore.TYPE_DESCRIPTOR ) );

        NodeStore nodeStore = new NodeStore( fileName, config, idGeneratorFactory, new DefaultWindowPoolFactory(),
                new DefaultFileSystemAbstraction(), StringLogger.DEV_NULL, null );

        // when
        for ( int i = 0; i < 2 * NUMBER_OF_RECORDS; i++ )
        {
            NodeRecord record = new NodeRecord( nodeStore.nextId(), 0, 0 );
            record.setInUse( true );
            nodeStore.updateRecord( record );
        }

        // then
        WindowPoolStats stats = nodeStore.getWindowPoolStats();

        nodeStore.close();

        assertEquals( stats.toString(), 0, stats.getMissCount() );
    }

    private String mmapSize( int numberOfRecords, int recordSize )
    {
        int bytes = numberOfRecords * recordSize;
        if ( bytes < MEGA )
        {
            throw new IllegalArgumentException( "too few records: " + numberOfRecords );
        }
        return bytes / MEGA + "M";
    }
}
