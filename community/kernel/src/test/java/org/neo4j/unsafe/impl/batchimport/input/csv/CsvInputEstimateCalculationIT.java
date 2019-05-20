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
package org.neo4j.unsafe.impl.batchimport.input.csv;

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
import java.util.Collection;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyValueRecordSizeCalculator;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Distribution;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators;
import org.neo4j.unsafe.impl.batchimport.input.RandomEntityDataGenerator;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static java.lang.Integer.parseInt;
import static java.lang.Math.abs;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_RECORD_FORMATS;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.input.RandomEntityDataGenerator.convert;
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
                format.property().getRecordSize( NO_STORE_HEADER ),
                parseInt( GraphDatabaseSettings.string_block_size.getDefaultValue() ), 0,
                parseInt( GraphDatabaseSettings.array_block_size.getDefaultValue() ), 0 ) );

        // when
        DatabaseLayout databaseLayout = directory.databaseLayout();
        Config config = Config.defaults();
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler() )
        {
            new ParallelBatchImporter( databaseLayout, fs, null, Configuration.DEFAULT, NullLogService.getInstance(), ExecutionMonitors.invisible(),
                    AdditionalInitialIds.EMPTY, config, format, NO_MONITOR, jobScheduler ).doImport( input );

            // then compare estimates with actual disk sizes
            VersionContextSupplier contextSupplier = EmptyVersionContextSupplier.EMPTY;
            try ( PageCache pageCache = new ConfiguringPageCacheFactory( fs, config, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL, NullLog.getInstance(),
                    contextSupplier, jobScheduler ).getOrCreatePageCache();
                    NeoStores stores = new StoreFactory( databaseLayout, config, new DefaultIdGeneratorFactory( fs ), pageCache, fs,
                            NullLogProvider.getInstance(), contextSupplier ).openAllNeoStores() )
            {
                assertRoughlyEqual( estimates.numberOfNodes(), stores.getNodeStore().getNumberOfIdsInUse() );
                assertRoughlyEqual( estimates.numberOfRelationships(), stores.getRelationshipStore().getNumberOfIdsInUse() );
                assertRoughlyEqual( estimates.numberOfNodeProperties() + estimates.numberOfRelationshipProperties(), calculateNumberOfProperties( stores ) );
            }
            assertRoughlyEqual( propertyStorageSize(), estimates.sizeOfNodeProperties() + estimates.sizeOfRelationshipProperties() );
        }
    }

    @Test
    public void shouldCalculateCorrectEstimatesOnEmptyData() throws Exception
    {
        // given
        Groups groups = new Groups();
        Collection<DataFactory> nodeData = asList( generateData( defaultFormatNodeFileHeader(), new MutableLong(), 0, 0, ":ID", "nodes-1.csv", groups ) );
        Collection<DataFactory> relationshipData = asList( generateData( defaultFormatRelationshipFileHeader(), new MutableLong(),
                0, 0, ":START_ID,:TYPE,:END_ID", "rels-1.csv", groups ) );
        Input input = new CsvInput( nodeData, defaultFormatNodeFileHeader(), relationshipData, defaultFormatRelationshipFileHeader(),
                IdType.INTEGER, COMMAS, Collector.EMPTY, CsvInput.NO_MONITOR, groups );

        // when
        Input.Estimates estimates = input.calculateEstimates( new PropertyValueRecordSizeCalculator(
                LATEST_RECORD_FORMATS.property().getRecordSize( NO_STORE_HEADER ),
                parseInt( GraphDatabaseSettings.string_block_size.getDefaultValue() ), 0,
                parseInt( GraphDatabaseSettings.array_block_size.getDefaultValue() ), 0 ) );

        // then
        assertEquals( 0, estimates.numberOfNodes() );
        assertEquals( 0, estimates.numberOfRelationships() );
        assertEquals( 0, estimates.numberOfRelationshipProperties() );
        assertEquals( 0, estimates.numberOfNodeProperties() );
        assertEquals( 0, estimates.numberOfNodeLabels() );
    }

    private long propertyStorageSize()
    {
        return sizeOf( DatabaseFile.PROPERTY_STORE ) + sizeOf( DatabaseFile.PROPERTY_ARRAY_STORE ) + sizeOf( DatabaseFile.PROPERTY_STRING_STORE );
    }

    private long sizeOf( DatabaseFile file )
    {
        return directory.databaseLayout().file( file ).mapToLong( File::length ).sum();
    }

    private Input generateData() throws IOException
    {
        List<DataFactory> nodeData = new ArrayList<>();
        MutableLong start = new MutableLong();
        Groups groups = new Groups();
        nodeData.add( generateData( defaultFormatNodeFileHeader(),
                start, NODE_COUNT / 3, NODE_COUNT, ":ID", "nodes-1.csv", groups ) );
        nodeData.add( generateData( defaultFormatNodeFileHeader(),
                start, NODE_COUNT / 3, NODE_COUNT, ":ID,:LABEL,name:String,yearOfBirth:int", "nodes-2.csv", groups ) );
        nodeData.add( generateData( defaultFormatNodeFileHeader(),
                start, NODE_COUNT - start.longValue(), NODE_COUNT, ":ID,name:String,yearOfBirth:int,other", "nodes-3.csv", groups ) );
        List<DataFactory> relationshipData = new ArrayList<>();
        start.setValue( 0 );
        relationshipData.add( generateData( defaultFormatRelationshipFileHeader(), start, RELATIONSHIP_COUNT / 2, NODE_COUNT,
                ":START_ID,:TYPE,:END_ID", "relationships-1.csv", groups ) );
        relationshipData.add( generateData( defaultFormatRelationshipFileHeader(), start, RELATIONSHIP_COUNT - start.longValue(),
                NODE_COUNT, ":START_ID,:TYPE,:END_ID,prop1,prop2", "relationships-2.csv", groups ) );
        return new CsvInput( nodeData, defaultFormatNodeFileHeader(), relationshipData, defaultFormatRelationshipFileHeader(),
                IdType.INTEGER, COMMAS, Collector.EMPTY, CsvInput.NO_MONITOR, groups );
    }

    private static long calculateNumberOfProperties( NeoStores stores )
    {
        long count = 0;
        PropertyRecord record = stores.getPropertyStore().newRecord();
        try ( PageCursor cursor = stores.getPropertyStore().openPageCursorForReading( 0 ) )
        {
            long highId = stores.getPropertyStore().getHighId();
            for ( long id = 0; id < highId; id++ )
            {
                stores.getPropertyStore().getRecordByCursor( id, record, CHECK, cursor );
                if ( record.inUse() )
                {
                    count += count( record );
                }
            }
        }
        return count;
    }

    private static void assertRoughlyEqual( long expected, long actual )
    {
        long diff = abs( expected - actual );
        assertThat( expected / 10, greaterThan( diff ) );
    }

    private DataFactory generateData( Header.Factory factory, MutableLong start, long count,
            long nodeCount, String headerString, String fileName, Groups groups ) throws IOException
    {
        File file = directory.file( fileName );
        Header header = factory.create( charSeeker( wrap( headerString ), COMMAS, false ), COMMAS, IdType.INTEGER, groups );
        Distribution<String> distribution = new Distribution<>( new String[] {"Token"} );
        Deserialization<String> deserialization = new StringDeserialization( COMMAS );
        try ( PrintWriter out = new PrintWriter( new BufferedWriter( new FileWriter( file ) ) );
              RandomEntityDataGenerator generator = new RandomEntityDataGenerator( nodeCount, count, toIntExact( count ), random.seed(),
                      start.longValue(), header, distribution, distribution, 0, 0 );
              InputChunk chunk = generator.newChunk();
              InputEntity entity = new InputEntity() )
        {
            out.println( headerString );
            while ( generator.next( chunk ) )
            {
                while ( chunk.next( entity ) )
                {
                    out.println( convert( entity, deserialization, header ) );
                }
            }
        }
        start.add( count );
        return DataFactories.data( InputEntityDecorators.NO_DECORATOR, Charsets.UTF_8, file );
    }
}
