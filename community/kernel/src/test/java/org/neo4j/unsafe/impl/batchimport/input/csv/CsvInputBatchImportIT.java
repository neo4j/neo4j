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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.Configuration.Default;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.SilentExecutionMonitor;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;
import static org.neo4j.unsafe.impl.batchimport.input.Inputs.csv;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;

public class CsvInputBatchImportIT
{
    @Test
    public void shouldImportDataComingFromCsvFiles() throws Exception
    {
        // GIVEN
        BatchImporter importer = new ParallelBatchImporter( directory.absolutePath(), fs,
                smallBatchSizeConfig(), new DevNullLoggingService(), new SilentExecutionMonitor() );
        List<InputNode> nodeData = randomNodeData();
        List<InputRelationship> relationshipData = randomRelationshipData( nodeData );

        // WHEN
        importer.doImport( csv(
                nodeDataAsFile( nodeData ),
                relationshipDataAsFile( relationshipData ),
                IdType.STRING, COMMAS ) );

        // THEN
        verifyImportedData( nodeData, relationshipData );
    }

    private void verifyImportedData( List<InputNode> nodeData, List<InputRelationship> relationshipData )
    {
        // Build up expected data for the verification below
        Map<String/*id*/, InputNode> expectedNodes = new HashMap<>();
        Set<String> expectedNodeNames = new HashSet<>();
        Map<String/*start node name*/, Map<String/*end node name*/, Map<String, AtomicInteger>>>
            expectedRelationships = new HashMap<>();
        buildUpExpectedData( nodeData, relationshipData, expectedNodes, expectedNodeNames, expectedRelationships );

        // Do the verification
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try ( Transaction tx = db.beginTx() )
        {
            // Verify nodes
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                assertTrue( expectedNodeNames.remove( name ) );
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
                AtomicInteger count = innerInner.get( type );
                int countAfterwards = count.decrementAndGet();
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

            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    private void buildUpExpectedData(
            List<InputNode> nodeData,
            List<InputRelationship> relationshipData,
            Map<String, InputNode> expectedNodes,
            Set<String> expectedNodeNames,
            Map<String, Map<String, Map<String, AtomicInteger>>> expectedRelationships )
    {
        for ( InputNode node : nodeData )
        {
            expectedNodes.put( (String) node.id(), node );
            expectedNodeNames.add( nameOf( node ) );
        }
        for ( InputRelationship relationship : relationshipData )
        {
            String startNodeName = nameOf( expectedNodes.get( relationship.startNode() ) );
            Map<String, Map<String, AtomicInteger>> inner = expectedRelationships.get( startNodeName );
            if ( inner == null )
            {
                expectedRelationships.put( startNodeName, inner = new HashMap<>() );
            }
            String endNodeName = nameOf( expectedNodes.get( relationship.endNode() ) );
            Map<String, AtomicInteger> innerInner = inner.get( endNodeName );
            if ( innerInner == null )
            {
                inner.put( endNodeName, innerInner = new HashMap<>() );
            }
            AtomicInteger count = innerInner.get( relationship.type() );
            if ( count == null )
            {
                innerInner.put( relationship.type(), count = new AtomicInteger() );
            }
            count.incrementAndGet();
        }
    }

    private String nameOf( InputNode node )
    {
        return (String) node.properties()[1];
    }

    private File relationshipDataAsFile( List<InputRelationship> relationshipData ) throws IOException
    {
        File file = directory.file( "relationships.csv" );
        try ( Writer writer = fs.openAsWriter( file, "utf-8", false ) )
        {
            // Header
            println( writer, "start,end,type" );

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
            println( writer, "id:ID,name" );

            // Data
            for ( InputNode node : nodeData )
            {
                println( writer, node.id() + "," + node.properties()[1] );
            }
        }
        return file;
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
            relationships.add( new InputRelationship( i, NO_PROPERTIES, null,
                    nodeData.get( random.nextInt( nodeData.size() ) ).id(),
                    nodeData.get( random.nextInt( nodeData.size() ) ).id(),
                    "TYPE_" + random.nextInt( 3 ), null ) );
        }
        return relationships;
    }

    private List<InputNode> randomNodeData()
    {
        List<InputNode> nodes = new ArrayList<>();
        for ( int i = 0; i < 300; i++ )
        {
            Object[] properties = new Object[] { "name", "Node " + i };
            nodes.add( new InputNode( UUID.randomUUID().toString(), properties, null,
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

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final Random random = new Random();
}
