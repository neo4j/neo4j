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
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
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

import org.neo4j.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.kernel.impl.util.AutoCreatingHashMap;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.Configuration.Default;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.nested;
import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.values;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.silentBadCollector;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;
import static org.neo4j.unsafe.impl.batchimport.input.Inputs.csv;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.invisible;

public class CsvInputBatchImportIT
{
    @Test
    public void shouldImportDataComingFromCsvFiles() throws Exception
    {
        // GIVEN
        BatchImporter importer = new ParallelBatchImporter( directory.graphDbDir(),
                smallBatchSizeConfig(), NullLogService.getInstance(), invisible(), new Config() );
        List<InputNode> nodeData = randomNodeData();
        List<InputRelationship> relationshipData = randomRelationshipData( nodeData );

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

    private org.neo4j.unsafe.impl.batchimport.input.csv.Configuration lowBufferSize(
            org.neo4j.unsafe.impl.batchimport.input.csv.Configuration actual )
    {
        return new org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.Overriden( actual )
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

    private List<InputNode> randomNodeData()
    {
        List<InputNode> nodes = new ArrayList<>();
        for ( int i = 0; i < 300; i++ )
        {
            Object[] properties = new Object[] { "name", "Node " + i };
            String id = UUID.randomUUID().toString();
            nodes.add( new InputNode( "source", i, i, id, properties, null,
                    randomLabels( random ), null ) );
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

    private Default smallBatchSizeConfig()
    {
        return new Configuration.Default()
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

    private File relationshipDataAsFile( List<InputRelationship> relationshipData ) throws IOException
    {
        File file = directory.file( "relationships.csv" );
        try ( Writer writer = fs.openAsWriter( file, "utf-8", false ) )
        {
            // Header
            println( writer, ":start_id,:end_id,:type" );

            // Data
            for ( InputRelationship relationship : relationshipData )
            {
                println( writer, relationship.startNode() + "," + relationship.endNode() + "," + relationship.type() );
            }
        }
        return file;
    }

    private File nodeDataAsFile( List<InputNode> nodeData ) throws IOException
    {
        File file = directory.file( "nodes.csv" );
        try ( Writer writer = fs.openAsWriter( file, "utf-8", false ) )
        {
            // Header
            println( writer, "id:ID,name,some-labels:LABEL" );

            // Data
            for ( InputNode node : nodeData )
            {
                String csvLabels = csvLabels( node.labels() );
                println( writer, node.id() + "," + node.properties()[1] +
                        (csvLabels != null && csvLabels.length() > 0 ? "," + csvLabels : "") );
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

    private List<InputRelationship> randomRelationshipData( List<InputNode> nodeData )
    {
        List<InputRelationship> relationships = new ArrayList<>();
        for ( int i = 0; i < 1000; i++ )
        {
            relationships.add( new InputRelationship(
                    "source", i, i,
                    NO_PROPERTIES, null,
                    nodeData.get( random.nextInt( nodeData.size() ) ).id(),
                    nodeData.get( random.nextInt( nodeData.size() ) ).id(),
                    "TYPE_" + random.nextInt( 3 ), null ) );
        }
        return relationships;
    }

    // ======================================================
    // Below is code for verifying the imported data
    // ======================================================

    private void verifyImportedData( List<InputNode> nodeData, List<InputRelationship> relationshipData )
    {
        // Build up expected data for the verification below
        Map<String/*id*/, InputNode> expectedNodes = new HashMap<>();
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
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                String[] labels = expectedNodeNames.remove( name );
                assertEquals( asSet( labels ), names( node.getLabels() ) );
            }
            assertEquals( 0, expectedNodeNames.size() );

            // Verify relationships
            for ( Relationship relationship : GlobalGraphOperations.at( db ).getAllRelationships() )
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
                    NeoStoresSupplier.class ).get();
            Function<String, Integer> labelTranslationTable =
                    translationTable( neoStores.getLabelTokenStore(), ReadOperations.ANY_LABEL );
            for ( Pair<Integer,Long> count : allNodeCounts( labelTranslationTable, expectedNodeCounts ) )
            {
                assertEquals( "Label count mismatch for label " + count.first(),
                        count.other().longValue(),
                        neoStores.getCounts()
                                .nodeCount( count.first().intValue(), newDoubleLongRegister() )
                                .readSecond() );
            }

            Function<String, Integer> relationshipTypeTranslationTable =
                    translationTable( neoStores.getRelationshipTypeTokenStore(), ReadOperations.ANY_RELATIONSHIP_TYPE );
            for ( Pair<RelationshipCountKey,Long> count : allRelationshipCounts( labelTranslationTable,
                    relationshipTypeTranslationTable, expectedRelationshipCounts ) )
            {
                RelationshipCountKey key = count.first();
                assertEquals( "Label count mismatch for label " + key,
                        count.other().longValue(),
                        neoStores.getCounts()
                                .relationshipCount( key.startLabel, key.type, key.endLabel, newDoubleLongRegister() )
                                .readSecond() );
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
        return new Function<String, Integer>()
        {
            @Override
            public Integer apply( String from )
            {
                return from == null ? anyValue : translationTable.get( from );
            }
        };
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
            List<InputNode> nodeData,
            List<InputRelationship> relationshipData,
            Map<String, InputNode> expectedNodes,
            Map<String, String[]> expectedNodeNames,
            Map<String, Map<String, Map<String, AtomicInteger>>> expectedRelationships,
            Map<String, AtomicLong> nodeCounts,
            Map<String, Map<String, Map<String, AtomicLong>>> relationshipCounts )
    {
        for ( InputNode node : nodeData )
        {
            expectedNodes.put( (String) node.id(), node );
            expectedNodeNames.put( nameOf( node ), node.labels() );
            countNodeLabels( nodeCounts, node.labels() );
        }
        for ( InputRelationship relationship : relationshipData )
        {
            // Expected relationship counts per node, type and direction
            InputNode startNode = expectedNodes.get( relationship.startNode() );
            InputNode endNode = expectedNodes.get( relationship.endNode() );
            {
                expectedRelationships.get( nameOf( startNode ) )
                                     .get( nameOf( endNode ) )
                                     .get( relationship.type() )
                                     .incrementAndGet();
            }

            // Expected counts per start/end node label ids
            // Let's do what CountsState#addRelationship does, roughly
            relationshipCounts.get( null ).get( null ).get( null ).incrementAndGet();
            relationshipCounts.get( null ).get( relationship.type() ).get( null ).incrementAndGet();
            for ( String startNodeLabelName : asSet( startNode.labels() ) )
            {
                Map<String, Map<String, AtomicLong>> startLabelCounts = relationshipCounts.get( startNodeLabelName );
                startLabelCounts.get( null ).get( null ).incrementAndGet();
                Map<String, AtomicLong> typeCounts = startLabelCounts.get( relationship.type() );
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
                relationshipCounts.get( null ).get( relationship.type() ).get( endNodeLabelName ).incrementAndGet();
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

    /** Don't support these counts at the moment so don't compute them */
    private static final boolean COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS = false;
    private String nameOf( InputNode node )
    {
        return (String) node.properties()[1];
    }

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final long seed = currentTimeMillis();
    private final Random random = new Random( seed );
}
