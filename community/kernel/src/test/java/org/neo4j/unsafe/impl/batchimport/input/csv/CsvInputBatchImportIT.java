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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.util.AutoCreatingHashMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Token;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.Charset.defaultCharset;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.nested;
import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.values;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.silentBadCollector;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.datas;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.invisible;

@EnableRuleMigrationSupport
@ExtendWith( TestDirectoryExtension.class )
public class CsvInputBatchImportIT
{
    /** Don't support these counts at the moment so don't compute them */
    private static final boolean COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS = false;
    private String nameOf( InputEntity node )
    {
        return (String) node.properties()[1];
    }

    @Resource
    public TestDirectory directory;
    @Rule
    public  final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final long seed = currentTimeMillis();
    private final Random random = new Random( seed );

    @Test
    public void shouldImportDataComingFromCsvFiles() throws Exception
    {
        // GIVEN
        Config dbConfig = Config.defaults();
        BatchImporter importer = new ParallelBatchImporter( directory.graphDbDir(), fileSystemRule.get(), null,
                smallBatchSizeConfig(), NullLogService.getInstance(), invisible(), AdditionalInitialIds.EMPTY, dbConfig,
                RecordFormatSelector.defaultFormat(), NO_MONITOR );
        List<InputEntity> nodeData = randomNodeData();
        List<InputEntity> relationshipData = randomRelationshipData( nodeData );

        // WHEN
        boolean success = false;
        try
        {
            importer.doImport( csv( nodeDataAsFile( nodeData ), relationshipDataAsFile( relationshipData ),
                    IdType.STRING, lowBufferSize( COMMAS ), silentBadCollector( 0 ) ) );
            // THEN
            verifyImportedData( nodeData, relationshipData );
            success = true;
        }
        finally
        {
            if ( !success )
            {
                System.err.println( "Seed " + seed );
            }
        }
    }

    public static Input csv( File nodes, File relationships, IdType idType,
            org.neo4j.unsafe.impl.batchimport.input.csv.Configuration configuration, Collector badCollector )
    {
        return new CsvInput(
                datas( data( NO_DECORATOR, defaultCharset(), nodes ) ), defaultFormatNodeFileHeader(),
                datas( data( NO_DECORATOR, defaultCharset(), relationships ) ),
                defaultFormatRelationshipFileHeader(), idType, configuration,
                badCollector );
    }

    private static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration lowBufferSize(
            org.neo4j.unsafe.impl.batchimport.input.csv.Configuration actual )
    {
        return new org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.Overridden( actual )
        {
            @Override
            public int bufferSize()
            {
                return 10_000;
            }
        };
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
            node.labels( randomLabels( random ) );
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

    private Configuration smallBatchSizeConfig()
    {
        return new Configuration()
        {
            @Override
            public int batchSize()
            {
                return 100;
            }

            @Override
            public int denseNodeThreshold()
            {
                return 5;
            }
        };
    }

    private File relationshipDataAsFile( List<InputEntity> relationshipData ) throws IOException
    {
        File file = directory.file( "relationships.csv" );
        try ( Writer writer = fileSystemRule.get().openAsWriter( file, StandardCharsets.UTF_8, false ) )
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
        File file = directory.file( "nodes.csv" );
        try ( Writer writer = fileSystemRule.get().openAsWriter( file, StandardCharsets.UTF_8, false ) )
        {
            // Header
            println( writer, "id:ID,name,some-labels:LABEL" );

            // Data
            for ( InputEntity node : nodeData )
            {
                String csvLabels = csvLabels( node.labels() );
                println( writer, node.id() + "," + node.properties()[1] + "," +
                        (csvLabels != null && csvLabels.length() > 0 ? csvLabels : "") );
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

    private void verifyImportedData( List<InputEntity> nodeData,
            List<InputEntity> relationshipData )
    {
        // Build up expected data for the verification below
        Map<String/*id*/, InputEntity> expectedNodes = new HashMap<>();
        Map<String,String[]> expectedNodeNames = new HashMap<>();
        Map<String/*start node name*/, Map<String/*end node name*/, Map<String, AtomicInteger>>> expectedRelationships =
                new AutoCreatingHashMap<>( nested( String.class, nested( String.class, values( AtomicInteger.class ) ) ) );
        Map<String, AtomicLong> expectedNodeCounts = new AutoCreatingHashMap<>( values( AtomicLong.class ) );
        Map<String, Map<String, Map<String, AtomicLong>>> expectedRelationshipCounts =
                new AutoCreatingHashMap<>( nested( String.class, nested( String.class, values( AtomicLong.class ) ) ) );
        buildUpExpectedData( nodeData, relationshipData, expectedNodes, expectedNodeNames, expectedRelationships,
                expectedNodeCounts, expectedRelationshipCounts );

        // Do the verification
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( directory.graphDbDir() );
        try ( Transaction tx = db.beginTx() )
        {
            // Verify nodes
            for ( Node node : db.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                String[] labels = expectedNodeNames.remove( name );
                assertEquals( asSet( labels ), names( node.getLabels() ) );
            }
            assertEquals( 0, expectedNodeNames.size() );

            // Verify relationships
            for ( Relationship relationship : db.getAllRelationships() )
            {
                String startNodeName = (String) relationship.getStartNode().getProperty( "name" );
                Map<String, Map<String, AtomicInteger>> inner = expectedRelationships.get( startNodeName );
                String endNodeName = (String) relationship.getEndNode().getProperty( "name" );
                Map<String, AtomicInteger> innerInner = inner.get( endNodeName );
                String type = relationship.getType().name();
                int countAfterwards = innerInner.get( type ).decrementAndGet();
                assertThat( countAfterwards, greaterThanOrEqualTo( 0 ) );
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

            // Verify counts, TODO how to get counts store other than this way?
            NeoStores neoStores = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(
                    RecordStorageEngine.class ).testAccessNeoStores();
            Function<String, Integer> labelTranslationTable =
                    translationTable( neoStores.getLabelTokenStore(), ReadOperations.ANY_LABEL );
            for ( Pair<Integer,Long> count : allNodeCounts( labelTranslationTable, expectedNodeCounts ) )
            {
                assertEquals( count.other().longValue(),
                        neoStores.getCounts().nodeCount( count.first().intValue(), newDoubleLongRegister() )
                                .readSecond(), "Label count mismatch for label " + count.first() );
            }

            Function<String, Integer> relationshipTypeTranslationTable =
                    translationTable( neoStores.getRelationshipTypeTokenStore(), ReadOperations.ANY_RELATIONSHIP_TYPE );
            for ( Pair<RelationshipCountKey,Long> count : allRelationshipCounts( labelTranslationTable,
                    relationshipTypeTranslationTable, expectedRelationshipCounts ) )
            {
                RelationshipCountKey key = count.first();
                assertEquals( count.other().longValue(), neoStores.getCounts()
                        .relationshipCount( key.startLabel, key.type, key.endLabel, newDoubleLongRegister() )
                        .readSecond(), "Label count mismatch for label " + key );
            }

            tx.success();
        }
        finally
        {
            db.shutdown();
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

    private Function<String, Integer> translationTable( TokenStore<?, ?> tokenStore, final int anyValue )
    {
        final Map<String, Integer> translationTable = new HashMap<>();
        for ( Token token : tokenStore.getTokens( Integer.MAX_VALUE ) )
        {
            translationTable.put( token.name(), token.id() );
        }
        return from -> from == null ? anyValue : translationTable.get( from );
    }

    private Set<String> names( Iterable<Label> labels )
    {
        Set<String> names = new HashSet<>();
        for ( Label label : labels )
        {
            names.add( label.name() );
        }
        return names;
    }

    private void buildUpExpectedData(
            List<InputEntity> nodeData,
            List<InputEntity> relationshipData,
            Map<String, InputEntity> expectedNodes,
            Map<String, String[]> expectedNodeNames,
            Map<String, Map<String, Map<String, AtomicInteger>>> expectedRelationships,
            Map<String, AtomicLong> nodeCounts,
            Map<String, Map<String, Map<String, AtomicLong>>> relationshipCounts )
    {
        for ( InputEntity node : nodeData )
        {
            expectedNodes.put( (String) node.id(), node );
            expectedNodeNames.put( nameOf( node ), node.labels() );
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

    private void countNodeLabels( Map<String, AtomicLong> nodeCounts, String[] labels )
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

}
