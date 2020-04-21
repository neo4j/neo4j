/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.batchimport.input.csv;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.batchinsert.internal.TransactionLogsInitializer;
import org.neo4j.configuration.Config;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.ParallelBatchImporter;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.InputEntityDecorators;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.util.AutoCreatingHashMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;

import static java.lang.String.format;
import static java.nio.charset.Charset.defaultCharset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.db_timezone;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.nested;
import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.values;

@Neo4jLayoutExtension
@ExtendWith( RandomExtension.class )
class CsvInputBatchImportIT
{
    /** Don't support these counts at the moment so don't compute them */
    private static final boolean COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS = false;

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private RandomRule random;
    @Inject
    private DatabaseLayout databaseLayout;

    private static final Supplier<ZoneId> testDefaultTimeZone = () -> ZoneId.of( "Asia/Shanghai" );

    @Test
    void shouldImportDataComingFromCsvFiles() throws Exception
    {
        // GIVEN
        Config dbConfig = Config.newBuilder()
                .set( db_timezone, LogTimeZone.SYSTEM )
                .set( dense_node_threshold, 5 )
                .build();
        try ( JobScheduler scheduler = new ThreadPoolJobScheduler() )
        {
            BatchImporter importer =
                    new ParallelBatchImporter( databaseLayout, fileSystem, null, PageCacheTracer.NULL,
                            smallBatchSizeConfig(), NullLogService.getInstance(),
                            ExecutionMonitors.invisible(), EMPTY, dbConfig, defaultFormat(), ImportLogic.NO_MONITOR, scheduler, Collector.EMPTY,
                            TransactionLogsInitializer.INSTANCE );
            List<InputEntity> nodeData = randomNodeData();
            List<InputEntity> relationshipData = randomRelationshipData( nodeData );

            // WHEN
            importer.doImport( csv( nodeDataAsFile( nodeData ), relationshipDataAsFile( relationshipData ), IdType.STRING, lowBufferSize( COMMAS ) ) );
            // THEN
            verifyImportedData( nodeData, relationshipData );
        }
    }

    static Input csv( File nodes, File relationships, IdType idType, Configuration configuration )
    {
        return new CsvInput(
                DataFactories.datas( DataFactories.data( InputEntityDecorators.NO_DECORATOR, defaultCharset(), nodes ) ),
                DataFactories.defaultFormatNodeFileHeader( testDefaultTimeZone, false ),
                DataFactories.datas( DataFactories.data( InputEntityDecorators.NO_DECORATOR, defaultCharset(), relationships ) ),
                DataFactories.defaultFormatRelationshipFileHeader( testDefaultTimeZone, false ),
                idType,
                configuration,
                CsvInput.NO_MONITOR );
    }

    private static Configuration lowBufferSize( Configuration actual )
    {
        return actual.toBuilder().withBufferSize( 10_000 ).build();
    }

    // ======================================================
    // Below is code for generating import data
    // ======================================================

    private List<InputEntity> randomNodeData()
    {
        List<InputEntity> nodes = new ArrayList<>();
        for ( int i = 0; i < 300; i++ )
        {
            InputEntity node = new InputEntity();
            node.id( UUID.randomUUID().toString(), Group.GLOBAL );
            node.property( "name", "Node " + i );
            node.property( "pointA", "\"   { x : -4.2, y : " + i % 90 + ", crs: WGS-84 } \"" );
            node.property( "pointB", "\" { x : -8, y : " + i + " } \"" );
            node.property( "date", LocalDate.of( 2018, i % 12 + 1, i % 28 + 1 ) );
            node.property( "time", OffsetTime.of( 1, i % 60, 0, 0, ZoneOffset.ofHours( 9 ) ) );
            node.property( "dateTime",
                    ZonedDateTime.of( 2011, 9, 11, 8, i % 60, 0, 0, ZoneId.of( "Europe/Stockholm" ) ) );
            node.property( "dateTime2",
                    LocalDateTime.of( 2011, 9, 11, 8, i % 60, 0, 0 ) ); // No zone specified
            node.property( "localTime", LocalTime.of( 1, i % 60, 0 ) );
            node.property( "localDateTime", LocalDateTime.of( 2011, 9, 11, 8, i % 60 ) );
            node.property( "duration", Period.of( 2, -3, i % 30 ) );
            node.labels( randomLabels( random.random() ) );
            nodes.add( node );
        }
        return nodes;
    }

    private String[] randomLabels( Random random )
    {
        String[] labels = new String[random.nextInt( 3 )];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = "Label" + random.nextInt( 4 );
        }
        return labels;
    }

    private org.neo4j.internal.batchimport.Configuration smallBatchSizeConfig()
    {
        return org.neo4j.internal.batchimport.Configuration.withBatchSize( org.neo4j.internal.batchimport.Configuration.DEFAULT, 100 );
    }

    private File relationshipDataAsFile( List<InputEntity> relationshipData ) throws IOException
    {
        File file = testDirectory.file( "relationships.csv" );
        try ( Writer writer = fileSystem.openAsWriter( file, StandardCharsets.UTF_8, false ) )
        {
            // Header
            println( writer, ":start_id,:end_id,:type" );

            // Data
            for ( InputEntity relationship : relationshipData )
            {
                println( writer, relationship.startId() + "," + relationship.endId() + "," + relationship.stringType );
            }
        }
        return file;
    }

    private File nodeDataAsFile( List<InputEntity> nodeData ) throws IOException
    {
        File file = testDirectory.file( "nodes.csv" );
        try ( Writer writer = fileSystem.openAsWriter( file, StandardCharsets.UTF_8, false ) )
        {
            // Header
            println( writer, "id:ID,name,pointA:Point{crs:WGS-84},pointB:Point,date:Date,time:Time,dateTime:DateTime,dateTime2:DateTime,localTime:LocalTime," +
                             "localDateTime:LocalDateTime,duration:Duration,some-labels:LABEL" );

            // Data
            for ( InputEntity node : nodeData )
            {
                String csvLabels = csvLabels( node.labels() );
                StringBuilder sb = new StringBuilder( node.id() + "," );
                for ( int i = 0; i < node.propertyCount(); i++ )
                {
                    sb.append( node.propertyValue( i ) + "," );
                }
                sb.append( csvLabels != null && !csvLabels.isEmpty() ? csvLabels : "" );
                println( writer, sb.toString() );
            }
        }
        return file;
    }

    private String csvLabels( String[] labels )
    {
        if ( labels == null || labels.length == 0 )
        {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for ( String label : labels )
        {
            builder.append( builder.length() > 0 ? ";" : "" ).append( label );
        }
        return builder.toString();
    }

    private void println( Writer writer, String string ) throws IOException
    {
        writer.write( string + "\n" );
    }

    private List<InputEntity> randomRelationshipData( List<InputEntity> nodeData )
    {
        List<InputEntity> relationships = new ArrayList<>();
        for ( int i = 0; i < 1000; i++ )
        {
            InputEntity relationship = new InputEntity();
            relationship.startId( nodeData.get( random.nextInt( nodeData.size() ) ).id(), Group.GLOBAL );
            relationship.endId( nodeData.get( random.nextInt( nodeData.size() ) ).id(), Group.GLOBAL );
            relationship.type( "TYPE_" + random.nextInt( 3 ) );
            relationships.add( relationship );
        }
        return relationships;
    }

    // ======================================================
    // Below is code for verifying the imported data
    // ======================================================

    private void verifyImportedData( List<InputEntity> nodeData, List<InputEntity> relationshipData )
    {
        // Build up expected data for the verification below
        Map<String/*id*/, InputEntity> expectedNodes = new HashMap<>();
        Map<String,String[]> expectedNodeNames = new HashMap<>();
        Map<String, Map<String,Consumer<Object>>> expectedNodePropertyVerifiers = new HashMap<>();
        Map<String/*start node name*/, Map<String/*end node name*/, Map<String, AtomicInteger>>> expectedRelationships =
                new AutoCreatingHashMap<>( nested( nested( values( AtomicInteger.class ) ) ) );
        Map<String, AtomicLong> expectedNodeCounts = new AutoCreatingHashMap<>( values( AtomicLong.class ) );
        Map<String, Map<String, Map<String, AtomicLong>>> expectedRelationshipCounts =
                new AutoCreatingHashMap<>( nested( nested( values( AtomicLong.class ) ) ) );
        buildUpExpectedData( nodeData, relationshipData, expectedNodes, expectedNodeNames, expectedNodePropertyVerifiers,
                expectedRelationships, expectedNodeCounts, expectedRelationshipCounts );

        // Do the verification
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            // Verify nodes
            for ( Node node : tx.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                String[] labels = expectedNodeNames.remove( name );
                assertEquals( asSet( labels ), names( node.getLabels() ) );

                // Verify node properties
                Map<String,Consumer<Object>> expectedPropertyVerifiers = expectedNodePropertyVerifiers.remove( name );
                Map<String,Object> actualProperties = node.getAllProperties();
                actualProperties.remove( "id" ); // The id does not exist in expected properties
                for ( Map.Entry actualProperty : actualProperties.entrySet() )
                {
                    Consumer v = expectedPropertyVerifiers.get( actualProperty.getKey() );
                    if ( v != null )
                    {
                        v.accept( actualProperty.getValue() );
                    }
                }
            }
            assertEquals( 0, expectedNodeNames.size() );

            // Verify relationships
            for ( Relationship relationship : tx.getAllRelationships() )
            {
                String startNodeName = (String) relationship.getStartNode().getProperty( "name" );
                Map<String, Map<String, AtomicInteger>> inner = expectedRelationships.get( startNodeName );
                String endNodeName = (String) relationship.getEndNode().getProperty( "name" );
                Map<String, AtomicInteger> innerInner = inner.get( endNodeName );
                String type = relationship.getType().name();
                int countAfterwards = innerInner.get( type ).decrementAndGet();
                assertThat( countAfterwards ).isGreaterThanOrEqualTo( 0 );
                if ( countAfterwards == 0 )
                {
                    innerInner.remove( type );
                    if ( innerInner.isEmpty() )
                    {
                        inner.remove( endNodeName );
                        if ( inner.isEmpty() )
                        {
                            expectedRelationships.remove( startNodeName );
                        }
                    }
                }
            }
            assertEquals( 0, expectedRelationships.size() );

            RecordStorageEngine storageEngine = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( RecordStorageEngine.class );
            NeoStores neoStores = storageEngine.testAccessNeoStores();
            CountsAccessor counts = storageEngine.countsAccessor();
            Function<String, Integer> labelTranslationTable =
                    translationTable( neoStores.getLabelTokenStore(), ANY_LABEL );
            for ( Pair<Integer,Long> count : allNodeCounts( labelTranslationTable, expectedNodeCounts ) )
            {
                assertEquals( count.other().longValue(), counts.nodeCount( count.first(), NULL ), "Label count mismatch for label " + count.first() );
            }

            Function<String, Integer> relationshipTypeTranslationTable =
                    translationTable( neoStores.getRelationshipTypeTokenStore(), ANY_RELATIONSHIP_TYPE );
            for ( Pair<RelationshipCountKey,Long> count : allRelationshipCounts( labelTranslationTable,
                    relationshipTypeTranslationTable, expectedRelationshipCounts ) )
            {
                RelationshipCountKey key = count.first();
                assertEquals( count.other().longValue(), counts.relationshipCount( key.startLabel, key.type, key.endLabel, NULL ),
                        "Label count mismatch for label " + key );
            }

            tx.commit();
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static class RelationshipCountKey
    {
        private final int startLabel;
        private final int type;
        private final int endLabel;

        RelationshipCountKey( int startLabel, int type, int endLabel )
        {
            this.startLabel = startLabel;
            this.type = type;
            this.endLabel = endLabel;
        }

        @Override
        public String toString()
        {
            return format( "[start:%d, type:%d, end:%d]", startLabel, type, endLabel );
        }
    }

    private Iterable<Pair<RelationshipCountKey,Long>> allRelationshipCounts(
            Function<String, Integer> labelTranslationTable,
            Function<String, Integer> relationshipTypeTranslationTable,
            Map<String, Map<String, Map<String, AtomicLong>>> counts )
    {
        Collection<Pair<RelationshipCountKey,Long>> result = new ArrayList<>();
        for ( Map.Entry<String, Map<String, Map<String, AtomicLong>>> startLabel : counts.entrySet() )
        {
            for ( Map.Entry<String, Map<String, AtomicLong>> type : startLabel.getValue().entrySet() )
            {
                for ( Map.Entry<String, AtomicLong> endLabel : type.getValue().entrySet() )
                {
                    RelationshipCountKey key = new RelationshipCountKey(
                            labelTranslationTable.apply( startLabel.getKey() ),
                            relationshipTypeTranslationTable.apply( type.getKey() ),
                            labelTranslationTable.apply( endLabel.getKey() ) );
                    result.add( Pair.of( key, endLabel.getValue().longValue() ) );
                }
            }
        }
        return result;
    }

    private Iterable<Pair<Integer,Long>> allNodeCounts( Function<String, Integer> labelTranslationTable,
            Map<String, AtomicLong> counts )
    {
        Collection<Pair<Integer,Long>> result = new ArrayList<>();
        for ( Map.Entry<String, AtomicLong> count : counts.entrySet() )
        {
            result.add( Pair.of( labelTranslationTable.apply( count.getKey() ), count.getValue().get() ) );
        }
        counts.put( null, new AtomicLong( counts.size() ) );
        return result;
    }

    private Function<String, Integer> translationTable( TokenStore<?> tokenStore, final int anyValue )
    {
        final Map<String, Integer> translationTable = new HashMap<>();
        for ( NamedToken token : tokenStore.getTokens( NULL ) )
        {
            translationTable.put( token.name(), token.id() );
        }
        return from -> from == null ? anyValue : translationTable.get( from );
    }

    private static Set<String> names( Iterable<Label> labels )
    {
        Set<String> names = new HashSet<>();
        for ( Label label : labels )
        {
            names.add( label.name() );
        }
        return names;
    }

    private static void buildUpExpectedData( List<InputEntity> nodeData, List<InputEntity> relationshipData, Map<String,InputEntity> expectedNodes,
            Map<String,String[]> expectedNodeNames, Map<String,Map<String,Consumer<Object>>> expectedNodePropertyVerifiers,
            Map<String,Map<String,Map<String,AtomicInteger>>> expectedRelationships, Map<String,AtomicLong> nodeCounts,
            Map<String,Map<String,Map<String,AtomicLong>>> relationshipCounts )
    {
        for ( InputEntity node : nodeData )
        {
            expectedNodes.put( (String) node.id(), node );
            expectedNodeNames.put( nameOf( node ), node.labels() );

            // Build default verifiers for all the properties that compares the property value using equals
            Assertions.assertFalse( node.hasIntPropertyKeyIds );
            Map<String,Consumer<Object>> propertyVerifiers = new TreeMap<>();
            for ( int i = 0; i < node.propertyCount(); i++ )
            {
                final Object expectedValue = node.propertyValue( i );
                Consumer verify;
                if ( expectedValue instanceof TemporalAmount )
                {
                    // Since there is no straightforward comparison for TemporalAmount we add it to a reference
                    // point in time and compare the result
                    verify = actualValue ->
                    {
                        LocalDateTime referenceTemporal = LocalDateTime.of( 0, 1, 1, 0, 0 );
                        LocalDateTime expected = referenceTemporal.plus( (TemporalAmount) expectedValue );
                        LocalDateTime actual = referenceTemporal.plus( (TemporalAmount) actualValue );
                        assertEquals( expected, actual );
                    };
                }
                else if ( expectedValue instanceof Temporal )
                {
                    final LocalDate expectedDate = ((Temporal) expectedValue).query( TemporalQueries.localDate() );
                    final LocalTime expectedTime = ((Temporal) expectedValue).query( TemporalQueries.localTime() );
                    final ZoneId expectedZoneId = ((Temporal) expectedValue).query( TemporalQueries.zone() );

                    verify = actualValue ->
                    {
                        LocalDate actualDate = ((Temporal) actualValue).query( TemporalQueries.localDate() );
                        LocalTime actualTime = ((Temporal) actualValue).query( TemporalQueries.localTime() );
                        ZoneId actualZoneId = ((Temporal) actualValue).query( TemporalQueries.zone() );

                        assertEquals( expectedDate, actualDate );
                        assertEquals( expectedTime, actualTime );
                        if ( expectedZoneId == null )
                        {
                            if ( actualZoneId != null )
                            {
                                // If the actual value is zoned it should have the default zone
                                assertEquals( testDefaultTimeZone.get(), actualZoneId );
                            }
                        }
                        else
                        {
                            assertEquals( expectedZoneId, actualZoneId );
                        }
                    };
                }
                else
                {
                    verify = actualValue -> assertEquals( expectedValue, actualValue );
                }
                propertyVerifiers.put( (String) node.propertyKey( i ), verify  );
            }

            // Special verifier for pointA property
            Consumer verifyPointA = actualValue ->
            {
                // The y-coordinate should match the node number modulo 90 (so we don't break wgs boundaries)
                PointValue v = (PointValue) actualValue;
                double actualY = v.getCoordinates().get( 0 ).getCoordinate().get( 1 );
                double expectedY = indexOf( node ) % 90;
                String message = actualValue + " does not have y=" + expectedY;
                assertEquals( expectedY, actualY, 0.1, message );
                message = actualValue + " does not have crs=wgs-84";
                assertEquals( CoordinateReferenceSystem.WGS84.getName(), v.getCoordinateReferenceSystem().getName(), message );
            };
            propertyVerifiers.put( "pointA", verifyPointA );

            // Special verifier for pointB property
            Consumer verifyPointB = actualValue ->
            {
                // The y-coordinate should match the node number
                PointValue v = (PointValue) actualValue;
                double actualY = v.getCoordinates().get( 0 ).getCoordinate().get( 1 );
                double expectedY = indexOf( node );
                String message = actualValue + " does not have y=" + expectedY;
                assertEquals( expectedY, actualY, 0.1, message );
                message = actualValue + " does not have crs=cartesian";
                assertEquals( CoordinateReferenceSystem.Cartesian.getName(), v.getCoordinateReferenceSystem().getName(), message );
            };
            propertyVerifiers.put( "pointB", verifyPointB );

            expectedNodePropertyVerifiers.put( nameOf( node ), propertyVerifiers );

            countNodeLabels( nodeCounts, node.labels() );
        }
        for ( InputEntity relationship : relationshipData )
        {
            // Expected relationship counts per node, type and direction
            InputEntity startNode = expectedNodes.get( relationship.startId() );
            InputEntity endNode = expectedNodes.get( relationship.endId() );
            {
                expectedRelationships.get( nameOf( startNode ) )
                                     .get( nameOf( endNode ) )
                                     .get( relationship.stringType )
                                     .incrementAndGet();
            }

            // Expected counts per start/end node label ids
            // Let's do what CountsState#addRelationship does, roughly
            relationshipCounts.get( null ).get( null ).get( null ).incrementAndGet();
            relationshipCounts.get( null ).get( relationship.stringType ).get( null ).incrementAndGet();
            for ( String startNodeLabelName : asSet( startNode.labels() ) )
            {
                Map<String, Map<String, AtomicLong>> startLabelCounts = relationshipCounts.get( startNodeLabelName );
                startLabelCounts.get( null ).get( null ).incrementAndGet();
                Map<String, AtomicLong> typeCounts = startLabelCounts.get( relationship.stringType );
                typeCounts.get( null ).incrementAndGet();
                if ( COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS )
                {
                    for ( String endNodeLabelName : asSet( endNode.labels() ) )
                    {
                        startLabelCounts.get( null ).get( endNodeLabelName ).incrementAndGet();
                        typeCounts.get( endNodeLabelName ).incrementAndGet();
                    }
                }
            }
            for ( String endNodeLabelName : asSet( endNode.labels() ) )
            {
                relationshipCounts.get( null ).get( null ).get( endNodeLabelName ).incrementAndGet();
                relationshipCounts.get( null ).get( relationship.stringType ).get( endNodeLabelName ).incrementAndGet();
            }
        }
    }

    private static void countNodeLabels( Map<String,AtomicLong> nodeCounts, String[] labels )
    {
        Set<String> seen = new HashSet<>();
        for ( String labelName : labels )
        {
            if ( seen.add( labelName ) )
            {
                nodeCounts.get( labelName ).incrementAndGet();
            }
        }
    }

    private static String nameOf( InputEntity node )
    {
        return (String) node.properties()[1];
    }

    private static int indexOf( InputEntity node )
    {
        return Integer.parseInt( ((String) node.properties()[1]).split( "\\s" )[1] );
    }

}
