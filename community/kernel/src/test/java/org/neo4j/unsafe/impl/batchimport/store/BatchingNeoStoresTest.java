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
package org.neo4j.unsafe.impl.batchimport.store;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores.calculateOptimalPageSize;

public class BatchingNeoStoresTest
{
    private final EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fsr ).around( pageCacheRule );

    private final File storeDir = new File( "dir" ).getAbsoluteFile();

    @Test
    public void shouldNotOpenStoreWithNodesOrRelationshipsInIt() throws Exception
    {
        // GIVEN
        someDataInTheDatabase();

        // WHEN
        try
        {
            RecordFormats recordFormats = RecordFormatSelector.selectForConfig( Config.empty(),
                    NullLogProvider.getInstance() );
            new BatchingNeoStores( fsr.get(), storeDir, recordFormats, DEFAULT, NullLogService.getInstance(), EMPTY,
                    Config.empty() );
            fail( "Should fail on existing data" );
        }
        catch ( IllegalStateException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "already contains" ) );
        }
    }

    @Test
    public void shouldRespectDbConfig() throws Exception
    {
        // GIVEN
        int size = 10;
        Config config = new Config( stringMap(
                GraphDatabaseSettings.array_block_size.name(), String.valueOf( size ),
                GraphDatabaseSettings.string_block_size.name(), String.valueOf( size ) ) );

        // WHEN
        RecordFormats recordFormats = StandardV3_0.RECORD_FORMATS;
        int headerSize = recordFormats.dynamic().getRecordHeaderSize();
        try ( BatchingNeoStores store = new BatchingNeoStores( fsr.get(), storeDir, recordFormats, DEFAULT,
                NullLogService.getInstance(), EMPTY, config ) )
        {
            // THEN
            assertEquals( size + headerSize, store.getPropertyStore().getArrayStore().getRecordSize() );
            assertEquals( size + headerSize, store.getPropertyStore().getStringStore().getRecordSize() );
        }
    }

    @Test
    public void shouldCalculateBigPageSizeForBiggerMemory() throws Exception
    {
        // GIVEN
        long memorySize = mebiBytes( 240 );

        // WHEN
        int pageSize = calculateOptimalPageSize( memorySize, 60 );

        // THEN
        assertEquals( mebiBytes( 4 ), pageSize );
    }

    @Test
    public void shouldCalculateSmallPageSizeForSmallerMemory() throws Exception
    {
        // GIVEN
        long memorySize = mebiBytes( 100 );

        // WHEN
        int pageSize = calculateOptimalPageSize( memorySize, 60 );

        // THEN
        assertEquals( mebiBytes( 1 ), pageSize );
    }

    @Test
    public void shouldNotGoLowerThan8kPageSizeForSmallMemory() throws Exception
    {
        // GIVEN
        long memorySize = kibiBytes( 8*30 );

        // WHEN
        int pageSize = calculateOptimalPageSize( memorySize, 60 );

        // THEN
        assertEquals( kibiBytes( 8 ), pageSize );
    }

    @Test
    public void shouldNotGoHigherThan8mPageSizeForBigMemory() throws Exception
    {
        // GIVEN
        long memorySize = mebiBytes( 700 );

        // WHEN
        int pageSize = calculateOptimalPageSize( memorySize, 60 );

        // THEN
        assertEquals( mebiBytes( 8 ), pageSize );
    }

    private void someDataInTheDatabase()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fsr.get() )
                .newImpermanentDatabase( storeDir );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }
}
