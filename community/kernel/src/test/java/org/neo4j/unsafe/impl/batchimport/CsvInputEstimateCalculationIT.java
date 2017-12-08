/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyValueRecordSizeCalculator;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Distribution;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.SimpleDataGeneratorBatch;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactory;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.input.csv.StringDeserialization;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

import static java.lang.Integer.parseInt;
import static java.lang.Math.abs;
import static java.lang.Math.toIntExact;
import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_RECORD_FORMATS;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;

public class CsvInputEstimateCalculationIT
{
    private static final long NODE_COUNT = 600_000;
    private static final long RELATIONSHIP_COUNT = 600_000;

    @Rule
    public final RandomRule random = new RandomRule();

    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    @Test
    public void shouldCalculateCorrectEstimates() throws Exception
    {
        // given a couple of input files of various layouts
        Input input = generateData();
        RecordFormats format = LATEST_RECORD_FORMATS;
        Input.Estimates estimates = input.calculateEstimates( new PropertyValueRecordSizeCalculator(
                LATEST_RECORD_FORMATS.property().getRecordSize( NO_STORE_HEADER ),
                parseInt( GraphDatabaseSettings.string_block_size.getDefaultValue() ), 0,
                parseInt( GraphDatabaseSettings.array_block_size.getDefaultValue() ), 0 ) );

        // when
        File storeDir = directory.absolutePath();
        Config config = Config.defaults();
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        new ParallelBatchImporter( storeDir, fs, null, Configuration.DEFAULT,
                NullLogService.getInstance(), ExecutionMonitors.invisible(), AdditionalInitialIds.EMPTY, config,
                format ).doImport( input );

        // then compare estimates with actual disk sizes
        try ( PageCache pageCache = new ConfiguringPageCacheFactory( fs, config, PageCacheTracer.NULL,
                      PageCursorTracerSupplier.NULL, NullLog.getInstance() ).getOrCreatePageCache();
              NeoStores stores = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fs ), pageCache, fs,
                      NullLogProvider.getInstance() ).openAllNeoStores() )
        {
            assertRoughlyEqual( estimates.numberOfNodes(), stores.getNodeStore().getNumberOfIdsInUse() );
            assertRoughlyEqual( estimates.numberOfRelationships(), stores.getRelationshipStore().getNumberOfIdsInUse() );
            assertRoughlyEqual( estimates.numberOfNodeProperties() + estimates.numberOfRelationshipProperties(),
                    calculateNumberOfProperties( stores.getPropertyStore() ) );
        }
        assertRoughlyEqual( propertyStorageSize(), estimates.sizeOfNodeProperties() + estimates.sizeOfRelationshipProperties() );
    }

    private long propertyStorageSize()
    {
        return sizeOf( StoreType.PROPERTY ) + sizeOf( StoreType.PROPERTY_ARRAY ) + sizeOf( StoreType.PROPERTY_STRING );
    }

    private long sizeOf( StoreType type )
    {
        return new File( directory.absolutePath(), DEFAULT_NAME + type.getStoreName() ).length();
    }

    private Input generateData() throws IOException
    {
        List<DataFactory<InputNode>> nodeData = new ArrayList<>();
        MutableLong start = new MutableLong();
        nodeData.add( generateData( defaultFormatNodeFileHeader(),
                start, NODE_COUNT / 3, NODE_COUNT, ":ID", "nodes-1.csv" ) );
        nodeData.add( generateData( defaultFormatNodeFileHeader(),
                start, NODE_COUNT / 3, NODE_COUNT, ":ID,:LABEL,name:String,yearOfBirth:int", "nodes-2.csv" ) );
        nodeData.add( generateData( defaultFormatNodeFileHeader(),
                start, NODE_COUNT - start.longValue(), NODE_COUNT, ":ID,name:String,yearOfBirth:int,other", "nodes-3.csv" ) );
        List<DataFactory<InputRelationship>> relationshipData = new ArrayList<>();
        start.setValue( 0 );
        relationshipData.add( generateData( defaultFormatRelationshipFileHeader(), start, RELATIONSHIP_COUNT / 2, NODE_COUNT,
                ":START_ID,:TYPE,:END_ID", "relationships-1.csv" ) );
        relationshipData.add( generateData( defaultFormatRelationshipFileHeader(), start, RELATIONSHIP_COUNT - start.longValue(),
                NODE_COUNT, ":START_ID,:TYPE,:END_ID,prop1,prop2", "relationships-2.csv" ) );
        return new CsvInput( nodeData, defaultFormatNodeFileHeader(), relationshipData, defaultFormatRelationshipFileHeader(),
                IdType.INTEGER, COMMAS, Collector.EMPTY, 1, false );
    }

    private long calculateNumberOfProperties( PropertyStore propertyStore )
    {
        long count = 0;
        try ( RecordCursor<PropertyRecord> cursor = propertyStore.newRecordCursor( propertyStore.newRecord() ).acquire( 0, CHECK ) )
        {
            long highId = propertyStore.getHighId();
            for ( long id = 0; id < highId; id++ )
            {
                if ( cursor.next( id ) )
                {
                    count += count( cursor.get() );
                }
            }
        }
        return count;
    }

    private void assertRoughlyEqual( long expected, long actual )
    {
        long diff = abs( expected - actual );
        assertThat( expected / 10, greaterThan( diff ) );
    }

    private <ENTITY extends InputEntity> DataFactory<ENTITY> generateData( Header.Factory factory, MutableLong start, long count,
            long nodeCount, String headerString, String fileName ) throws IOException
    {
        File file = directory.file( fileName );
        Header header = factory.create( charSeeker( wrap( headerString ), COMMAS, false ), COMMAS, IdType.INTEGER );
        Distribution<String> distribution = new Distribution<>( new String[] {"Token"} );
        SimpleDataGeneratorBatch<String> generator = new SimpleDataGeneratorBatch<>( header, start.longValue(), random.seed(), nodeCount,
                distribution, distribution, new StringDeserialization( COMMAS ), new String[toIntExact( count )], 0, 0 );
        try ( PrintWriter out = new PrintWriter( new BufferedWriter( new FileWriter( file ) ) ) )
        {
            out.println( headerString );
            for ( String entity : generator.get() )
            {
                out.println( entity );
            }
        }
        start.add( count );
        return DataFactories.data( InputEntityDecorators.noDecorator(), Charsets.UTF_8, file );
    }
}
