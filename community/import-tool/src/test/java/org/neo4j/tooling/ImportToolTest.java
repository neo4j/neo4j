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
package org.neo4j.tooling;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.primitive.PrimitiveIntPredicate;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.tooling.ImportTool;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;

import static java.lang.System.currentTimeMillis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.collection.primitive.PrimitiveIntCollections.alwaysTrue;
import static org.neo4j.tooling.ImportTool.MULTI_DELIMITER;

public class ImportToolTest
{
    @Test
    public void shouldImportWithAsManyDefaultsAsAvailable() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        ImportTool.main( arguments(
                "--into",          directory.absolutePath(),
                "--nodes",         nodeData( true, config, nodeIds, alwaysTrue() ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, nodeIds, alwaysTrue() ).getAbsolutePath() ) );

        // THEN
        verifyData();
    }

    @Test
    public void shouldImportWithHeadersBeingInSeparateFiles() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        ImportTool.main( arguments(
                "--into", directory.absolutePath(),
                "--nodes",
                    nodeHeader( config ).getAbsolutePath() + MULTI_DELIMITER +
                    nodeData( false, config, nodeIds, alwaysTrue() ).getAbsolutePath(),
                "--relationships",
                    relationshipHeader( config ).getAbsolutePath() + MULTI_DELIMITER +
                    relationshipData( false, config, nodeIds, alwaysTrue() ).getAbsolutePath() ) );

        // THEN
        verifyData();
    }

    @Test
    public void shouldImportMultipleInputGroups() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        ImportTool.main( arguments(
                "--into", directory.absolutePath(),
                "--nodes", // One group with one header file and one data file
                    nodeHeader( config ).getAbsolutePath() + MULTI_DELIMITER +
                    nodeData( false, config, nodeIds, lines( 0, NODE_COUNT/2 ) ).getAbsolutePath(),
                "--nodes", // One group with two data files, where the header sits in the first file
                    nodeData( true, config, nodeIds,
                              lines( NODE_COUNT/2, NODE_COUNT*3/4 ) ).getAbsolutePath() + MULTI_DELIMITER +
                    nodeData( false, config, nodeIds, lines( NODE_COUNT*3/4, NODE_COUNT ) ).getAbsolutePath(),
                "--relationships",
                    relationshipHeader( config ).getAbsolutePath() + MULTI_DELIMITER +
                    relationshipData( false, config, nodeIds, alwaysTrue() ).getAbsolutePath() ) );

        // THEN
        verifyData();
    }

    private void verifyData()
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try ( Transaction tx = db.beginTx() )
        {
            int nodeCount = 0, relationshipCount = 0;
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                assertTrue( node.hasProperty( "name" ) );
                nodeCount++;
            }
            assertEquals( NODE_COUNT, nodeCount );
            for ( Relationship relationship : GlobalGraphOperations.at( db ).getAllRelationships() )
            {
                assertTrue( relationship.hasProperty( "created" ) );
                relationshipCount++;
            }
            assertEquals( RELATIONSHIP_COUNT, relationshipCount );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    private List<String> nodeIds()
    {
        List<String> ids = new ArrayList<>();
        for ( int i = 0; i < NODE_COUNT; i++ )
        {
            ids.add( UUID.randomUUID().toString() );
        }
        return ids;
    }

    private File nodeData( boolean includeHeader, Configuration config, List<String> nodeIds,
                           PrimitiveIntPredicate linePredicate ) throws FileNotFoundException
    {
        File file = directory.file( fileName( "nodes.csv" ) );
        try ( PrintStream writer = new PrintStream( file ) )
        {
            if ( includeHeader )
            {
                writeNodeHeader( writer, config );
            }
            writeNodeData( writer, config, nodeIds, linePredicate );
        }
        return file;
    }

    private File nodeHeader( Configuration config ) throws FileNotFoundException
    {
        File file = directory.file( fileName( "nodes-header.csv" ) );
        try ( PrintStream writer = new PrintStream( file ) )
        {
            writeNodeHeader( writer, config );
        }
        return file;
    }

    private void writeNodeHeader( PrintStream writer, Configuration config )
    {
        char delimiter = config.delimiter();
        writer.println( "id:ID" + delimiter + "name" + delimiter + "labels:LABEL" );
    }

    private void writeNodeData( PrintStream writer, Configuration config, List<String> nodeIds,
                                PrimitiveIntPredicate linePredicate )
    {
        char delimiter = config.delimiter();
        char arrayDelimiter = config.arrayDelimiter();
        for ( int i = 0; i < nodeIds.size(); i++ )
        {
            if ( linePredicate.accept( i ) )
            {
                writer.println( nodeIds.get( i ) + delimiter + randomName() +
                                delimiter + randomLabels( arrayDelimiter ) );
            }
        }
    }

    private String randomLabels( char arrayDelimiter )
    {
        int length = random.nextInt( 3 );
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < length; i++ )
        {
            if ( i > 0 )
            {
                builder.append( arrayDelimiter );
            }
            builder.append( "LABEL_" + random.nextInt( 4 ) );
        }
        return builder.toString();
    }

    private String randomName()
    {
        int length = random.nextInt( 10 )+5;
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < length; i++ )
        {
            builder.append( (char) ('a' + random.nextInt( 20 )) );
        }
        return builder.toString();
    }

    private File relationshipData( boolean includeHeader, Configuration config, List<String> nodeIds,
                                   PrimitiveIntPredicate linePredicate ) throws FileNotFoundException
    {
        File file = directory.file( fileName( "relationships.csv" ) );
        try ( PrintStream writer = new PrintStream( file ) )
        {
            if ( includeHeader )
            {
                writeRelationshipHeader( writer, config );
            }
            writeRelationshipData( writer, config, nodeIds, linePredicate );
        }
        return file;
    }

    private File relationshipHeader( Configuration config ) throws FileNotFoundException
    {
        File file = directory.file( fileName( "relationships-header.csv" ) );
        try ( PrintStream writer = new PrintStream( file ) )
        {
            writeRelationshipHeader( writer, config );
        }
        return file;
    }

    private String fileName( String name )
    {
        return dataIndex++ + "-" + name;
    }

    private void writeRelationshipHeader( PrintStream writer, Configuration config )
    {
        char delimiter = config.delimiter();
        writer.println( ":" + Type.START_ID + delimiter + ":" + Type.END_ID + delimiter +
                        ":" + Type.TYPE + delimiter + "created:long" );
    }

    private void writeRelationshipData( PrintStream writer, Configuration config, List<String> nodeIds,
                                        PrimitiveIntPredicate linePredicate )
    {
        char delimiter = config.delimiter();
        for ( int i = 0; i < RELATIONSHIP_COUNT; i++ )
        {
            if ( linePredicate.accept( i ) )
            {
                writer.println( nodeIds.get( random.nextInt( nodeIds.size() ) ) + delimiter +
                        nodeIds.get( random.nextInt( nodeIds.size() ) ) + delimiter + randomType() + delimiter +
                        currentTimeMillis() );
            }
        }
    }

    private String randomType()
    {
        return "TYPE_" + random.nextInt( 4 );
    }

    private String[] arguments( String... arguments )
    {
        return arguments;
    }

    private PrimitiveIntPredicate lines( final int startingAt, final int endingAt /*excluded*/ )
    {
        return new PrimitiveIntPredicate()
        {
            @Override
            public boolean accept( int line )
            {
                return line >= startingAt && line < endingAt;
            }
        };
    }

    private static final int RELATIONSHIP_COUNT = 10_000;
    private static final int NODE_COUNT = 100;

    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final Random random = new Random();
    private int dataIndex;
}
