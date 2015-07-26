/*
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
package org.neo4j.tooling;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.neo4j.csv.reader.IllegalMultilineFieldException;
import org.neo4j.function.primitive.PrimitiveIntPredicate;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.Mute;
import org.neo4j.test.RandomRule;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;

import static org.neo4j.collection.primitive.PrimitiveIntCollections.alwaysTrue;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.ArrayUtil.join;
import static org.neo4j.helpers.Exceptions.contains;
import static org.neo4j.helpers.Exceptions.withMessage;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.IteratorUtil.singleOrNull;
import static org.neo4j.tooling.ImportTool.MULTI_FILE_DELIMITER;

public class ImportToolTest
{
    @Test
    public void shouldImportWithAsManyDefaultsAsAvailable() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        importTool(
                "--into",          dbRule.getStoreDirAbsolutePath(),
                "--nodes",         nodeData( true, config, nodeIds, alwaysTrue() ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, nodeIds, alwaysTrue(), true ).getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    public void shouldImportWithHeadersBeingInSeparateFiles() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--delimiter", "TAB",
                "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                "--nodes",
                    nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                    nodeData( false, config, nodeIds, alwaysTrue() ).getAbsolutePath(),
                "--relationships",
                    relationshipHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                    relationshipData( false, config, nodeIds, alwaysTrue(), true ).getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    public void shouldImportSplitInputFiles() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", // One group with one header file and one data file
                    nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                    nodeData( false, config, nodeIds, lines( 0, NODE_COUNT/2 ) ).getAbsolutePath(),
                "--nodes", // One group with two data files, where the header sits in the first file
                    nodeData( true, config, nodeIds,
                            lines( NODE_COUNT / 2, NODE_COUNT * 3 / 4 ) ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                    nodeData( false, config, nodeIds, lines( NODE_COUNT * 3 / 4, NODE_COUNT ) ).getAbsolutePath(),
                "--relationships",
                    relationshipHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                    relationshipData( false, config, nodeIds, alwaysTrue(), true ).getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    public void shouldImportMultipleInputsWithAddedLabelsAndDefaultRelationshipType() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        final String[] firstLabels = { "AddedOne", "AddedTwo" };
        final String[] secondLabels = { "AddedThree" };
        final String firstType = "TYPE_1";
        final String secondType = "TYPE_2";

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes:" + join( firstLabels, ":" ),
                    nodeData( true, config, nodeIds, lines( 0, NODE_COUNT/2 ) ).getAbsolutePath(),
                "--nodes:" + join( secondLabels, ":" ),
                    nodeData( true, config, nodeIds, lines( NODE_COUNT/2, NODE_COUNT ) ).getAbsolutePath(),
                "--relationships:" + firstType,
                    relationshipData( true, config, nodeIds, lines( 0, RELATIONSHIP_COUNT/2 ), false ).getAbsolutePath(),
                "--relationships:" + secondType,
                    relationshipData( true, config, nodeIds,
                            lines( RELATIONSHIP_COUNT/2, RELATIONSHIP_COUNT ), false ).getAbsolutePath() );

        // THEN
        verifyData(
                new Validator<Node>()
                {
                    @Override
                    public void validate( Node node )
                    {
                        if ( node.getId() < NODE_COUNT/2 )
                        {
                            assertNodeHasLabels( node, firstLabels );
                        }
                        else
                        {
                            assertNodeHasLabels( node, secondLabels );
                        }
                    }
                },
                new Validator<Relationship>()
                {
                    @Override
                    public void validate( Relationship relationship )
                    {
                        if ( relationship.getId() < RELATIONSHIP_COUNT/2 )
                        {
                            assertEquals( firstType, relationship.getType().name() );
                        }
                        else
                        {
                            assertEquals( secondType, relationship.getType().name() );
                        }
                    }
                } );
    }

    @Test
    public void shouldImportOnlyNodes() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        importTool(
                "--into",          dbRule.getStoreDirAbsolutePath(),
                "--nodes",         nodeData( true, config, nodeIds, alwaysTrue() ).getAbsolutePath() );
                // no relationships

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            int nodeCount = 0;
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                assertTrue( node.hasProperty( "name" ) );
                nodeCount++;
                assertFalse( node.hasRelationship() );
            }
            assertEquals( NODE_COUNT, nodeCount );
            tx.success();
        }
    }

    @Test
    public void shouldImportGroupsOfOverlappingIds() throws Exception
    {
        // GIVEN
        List<String> groupOneNodeIds = asList( "1", "2", "3" );
        List<String> groupTwoNodeIds = asList( "4", "5", "2" );
        List<RelationshipDataLine> rels = asList(
                relationship( "1", "4", "TYPE" ),
                relationship( "2", "5", "TYPE" ),
                relationship( "3", "2", "TYPE" ) );
        Configuration config = Configuration.COMMAS;
        String groupOne = "Actor";
        String groupTwo = "Movie";

        // WHEN
        importTool(
                "--into",  dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeHeader( config, groupOne ) + MULTI_FILE_DELIMITER +
                           nodeData( false, config, groupOneNodeIds, alwaysTrue() ),
                "--nodes", nodeHeader( config, groupTwo ) + MULTI_FILE_DELIMITER +
                           nodeData( false, config, groupTwoNodeIds, alwaysTrue() ),
                "--relationships", relationshipHeader( config, groupOne, groupTwo, true ) + MULTI_FILE_DELIMITER +
                                   relationshipData( false, config, rels.iterator(), alwaysTrue(), true ) );

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            int nodeCount = 0;
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                assertTrue( node.hasProperty( "name" ) );
                nodeCount++;
                assertEquals( 1, count( node.getRelationships() ) );
            }
            assertEquals( 6, nodeCount );
            tx.success();
        }
    }

    @Test
    public void shouldNotBeAbleToMixSpecifiedAndUnspecifiedGroups() throws Exception
    {
        // GIVEN
        List<String> groupOneNodeIds = asList( "1", "2", "3" );
        List<String> groupTwoNodeIds = asList( "4", "5", "2" );
        Configuration config = Configuration.COMMAS;

        // WHEN
        try
        {
            importTool(
                    "--into",          dbRule.getStoreDirAbsolutePath(),
                    "--nodes",         nodeHeader( config, "MyGroup" ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                    nodeData( false, config, groupOneNodeIds, alwaysTrue() ).getAbsolutePath(),
                    "--nodes",         nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                    nodeData( false, config, groupTwoNodeIds, alwaysTrue() ).getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            assertExceptionContains( e, "Mixing specified", IllegalStateException.class );
        }
    }

    @Test
    public void shouldImportWithoutTypeSpecifiedInRelationshipHeaderbutWithDefaultTypeInArgument() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        String type = randomType();

        // WHEN
        importTool(
                "--into",          dbRule.getStoreDirAbsolutePath(),
                "--nodes",         nodeData( true, config, nodeIds, alwaysTrue() ).getAbsolutePath(),
                                   // there will be no :TYPE specified in the header of the relationships below
                "--relationships:" + type,
                                   relationshipData( true, config, nodeIds, alwaysTrue(), false ).getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    public void shouldIncludeSourceInformationInNodeIdCollisionError() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c", "d", "e", "f", "a", "g" );
        Configuration config = Configuration.COMMAS;
        File nodeHeaderFile = nodeHeader( config );
        File nodeData1 = nodeData( false, config, nodeIds, lines( 0, 4 ) );
        File nodeData2 = nodeData( false, config, nodeIds, lines( 4, nodeIds.size() ) );

        // WHEN
        try
        {
            importTool(
                    "--into",  dbRule.getStoreDirAbsolutePath(),
                    "--nodes", nodeHeaderFile.getAbsolutePath() + MULTI_FILE_DELIMITER +
                               nodeData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                               nodeData2.getAbsolutePath() );
            fail( "Should have failed with duplicate node IDs" );
        }
        catch ( Exception e )
        {
            // THEN
            assertExceptionContains( e, nodeData1.getPath() + ":" + 1, DuplicateInputIdException.class );
            assertExceptionContains( e, nodeData2.getPath() + ":" + 3, DuplicateInputIdException.class );
        }
    }

    @Test
    public void shouldSkipDuplicateNodesIfToldTo() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c", "d", "e", "f", "a", "g" );
        Configuration config = Configuration.COMMAS;
        File nodeHeaderFile = nodeHeader( config );
        File nodeData1 = nodeData( false, config, nodeIds, lines( 0, 4 ) );
        File nodeData2 = nodeData( false, config, nodeIds, lines( 4, nodeIds.size() ) );

        // WHEN
        importTool(
                "--into",  dbRule.getStoreDirAbsolutePath(),
                "--skip-duplicate-nodes",
                "--nodes", nodeHeaderFile.getAbsolutePath() + MULTI_FILE_DELIMITER +
                nodeData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                nodeData2.getAbsolutePath() );

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            Iterator<Node> nodes = GlobalGraphOperations.at( db ).getAllNodes().iterator();
            Iterator<String> expectedIds = FilteringIterator.noDuplicates( nodeIds.iterator() );
            while ( expectedIds.hasNext() )
            {
                assertTrue( nodes.hasNext() );
                assertEquals( expectedIds.next(), nodes.next().getProperty( "id" ) );
            }
            assertFalse( nodes.hasNext() );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldLogRelationshipsReferingToMissingNode() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c" );
        Configuration config = Configuration.COMMAS;
        File nodeData = nodeData( true, config, nodeIds, alwaysTrue() );
        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship( "a", "b", "TYPE", "aa" ), //          line 2 of file1
                relationship( "c", "bogus", "TYPE", "bb" ), //      line 3 of file1
                relationship( "b", "c", "KNOWS", "cc" ), //         line 1 of file2
                relationship( "c", "a", "KNOWS", "dd" ), //         line 2 of file2
                relationship( "missing", "a", "KNOWS", "ee" ) ); // line 3 of file2
        File relationshipData1 = relationshipData( true, config, relationships.iterator(), lines( 0, 2 ), true );
        File relationshipData2 = relationshipData( false, config, relationships.iterator(), lines( 2, 5 ), true );
        File bad = file( "bad.log" );

        // WHEN importing data where some relationships refer to missing nodes
        importTool(
                "--into",          dbRule.getStoreDirAbsolutePath(),
                "--nodes",         nodeData.getAbsolutePath(),
                "--bad",           bad.getAbsolutePath(),
                "--bad-tolerance", "2",
                "--relationships", relationshipData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                                   relationshipData2.getAbsolutePath() );

        // THEN
        String badContents = FileUtils.readTextFile( bad, Charset.defaultCharset() );
        assertTrue( "Didn't contain first bad relationship",
                badContents.contains( relationshipData1.getAbsolutePath() + ":3" ) );
        assertTrue( "Didn't contain second bad relationship",
                badContents.contains( relationshipData2.getAbsolutePath() + ":3" ) );
        verifyRelationships( relationships );
    }

    @Test
    public void shouldFailIfTooManyBadRelationships() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c" );
        Configuration config = Configuration.COMMAS;
        File nodeData = nodeData( true, config, nodeIds, alwaysTrue() );
        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship( "a", "b", "TYPE" ), //          line 2 of file1
                relationship( "c", "bogus", "TYPE" ), //      line 3 of file1
                relationship( "b", "c", "KNOWS" ), //         line 1 of file2
                relationship( "c", "a", "KNOWS" ), //         line 2 of file2
                relationship( "missing", "a", "KNOWS" ) ); // line 3 of file2
        File relationshipData1 = relationshipData( true, config, relationships.iterator(), lines( 0, 2 ), true );
        File relationshipData2 = relationshipData( false, config, relationships.iterator(), lines( 2, 5 ), true );
        File bad = file( "bad.log" );

        // WHEN importing data where some relationships refer to missing nodes
        try
        {
            importTool(
                    "--into",          dbRule.getStoreDirAbsolutePath(),
                    "--nodes",         nodeData.getAbsolutePath(),
                    "--bad",           bad.getAbsolutePath(),
                    "--bad-tolerance", "1",
                    "--relationships", relationshipData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                                       relationshipData2.getAbsolutePath() ) ;
        }
        catch ( Exception e )
        {
            // THEN
            assertExceptionContains( e, relationshipData2.getAbsolutePath() + ":3", InputException.class );
        }
    }

    @Test
    public void shouldBeAbleToDisableSkippingOfBadRelationships() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c" );
        Configuration config = Configuration.COMMAS;
        File nodeData = nodeData( true, config, nodeIds, alwaysTrue() );

        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship( "a", "b", "TYPE" ), //          line 2 of file1
                relationship( "c", "bogus", "TYPE" ) ); //    line 3 of file1

        File relationshipData1 = relationshipData( true, config, relationships.iterator(), lines( 0, 2 ), true );
        File relationshipData2 = relationshipData( false, config, relationships.iterator(), lines( 2, 5 ), true );
        File bad = file( "bad.log" );

        // WHEN importing data where some relationships refer to missing nodes
        try
        {
            importTool(
                    "--into",          dbRule.getStoreDirAbsolutePath(),
                    "--nodes",         nodeData.getAbsolutePath(),
                    "--bad",           bad.getAbsolutePath(),
                    "--skip-bad-relationships", "false",
                    "--relationships", relationshipData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                    relationshipData2.getAbsolutePath() ) ;
        }
        catch ( Exception e )
        {
            // THEN
            assertExceptionContains( e, relationshipData1.getAbsolutePath() + ":3", InputException.class );
        }
    }

    @Test
    public void shouldHandleAdditiveLabelsWithSpaces() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        final Label label1 = DynamicLabel.label( "My First Label" );
        final Label label2 = DynamicLabel.label( "My Other Label" );

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes:My First Label:My Other Label",
                        nodeData( true, config, nodeIds, alwaysTrue() ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, nodeIds, alwaysTrue(), true ).getAbsolutePath() );

        // THEN
        verifyData( new Validator<Node>()
        {
            @Override
            public void validate( Node node )
            {
                assertTrue( node.hasLabel( label1 ) );
                assertTrue( node.hasLabel( label2 ) );
            }
        }, Validators.<Relationship>emptyValidator() );
    }

    @Test
    public void shouldImportFromInputDataEncodedWithSpecificCharset() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        Charset charset = Charset.forName( "UTF-16" );

        // WHEN
        importTool(
                "--into",           dbRule.getStoreDirAbsolutePath(),
                "--input-encoding", charset.name(),
                "--nodes",          nodeData( true, config, nodeIds, alwaysTrue(), charset ).getAbsolutePath(),
                "--relationships",  relationshipData( true, config, nodeIds, alwaysTrue(), true, charset )
                                           .getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    public void shouldDisallowImportWithoutNodesInput() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        try
        {
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--relationships", relationshipData( true, config, nodeIds, alwaysTrue(), true ).getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "No node input" ) );
        }
    }

    @Test
    public void shouldBeAbleToImportAnonymousNodes() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "1", "", "", "", "3", "", "", "", "", "", "5" );
        Configuration config = Configuration.COMMAS;
        List<RelationshipDataLine> relationshipData = asList( relationship( "1", "3", "KNOWS" ) );

        // WHEN
        importTool(
                "--into",          dbRule.getStoreDirAbsolutePath(),
                "--nodes",         nodeData( true, config, nodeIds, alwaysTrue() ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, relationshipData.iterator(),
                                   alwaysTrue(), true ).getAbsolutePath() );

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            Iterable<Node> allNodes = GlobalGraphOperations.at( db ).getAllNodes();
            int anonymousCount = 0;
            for ( final String id : nodeIds )
            {
                if ( id.isEmpty() )
                {
                    anonymousCount++;
                }
                else
                {
                    assertNotNull( single( filter( nodeFilter( id ), allNodes.iterator() ) ) );
                }
            }
            assertEquals( anonymousCount, count( filter( nodeFilter( "" ), allNodes.iterator() ) ) );
            tx.success();
        }
    }

    @Test
    public void shouldDisallowMultilineFieldsByDefault() throws Exception
    {
        // GIVEN
        File data = data( ":ID,name", "1,\"This is a line with\nnewlines in\"" );

        // WHEN
        try
        {
            importTool(
                    "--into",  dbRule.getStoreDirAbsolutePath(),
                    "--nodes", data.getAbsolutePath() );
        }
        catch ( Exception e )
        {
            // THEN
            assertExceptionContains( e, "Multi-line", IllegalMultilineFieldException.class );
        }
    }

    @Test
    public void shouldSkipEmptyFiles() throws Exception
    {
        // GIVEN
        File data = data( "" );

        // WHEN
        importTool( "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", data.getAbsolutePath() );

        // THEN
        GraphDatabaseService graphDatabaseService = dbRule.getGraphDatabaseService();
        try ( Transaction tx = graphDatabaseService.beginTx() )
        {
            ResourceIterator<Node> allNodes = GlobalGraphOperations.at( graphDatabaseService ).getAllNodes().iterator();
            assertFalse( "Expected database to be empty", allNodes.hasNext() );
            tx.success();
        }
    }

    private File data( String... lines ) throws Exception
    {
        File file = file( fileName( "data.csv" ) );
        try ( PrintStream writer = writer( file, Charset.defaultCharset() ) )
        {
            for ( String line : lines )
            {
                writer.println( line );
            }
        }
        return file;
    }

    private Predicate<Node> nodeFilter( final String id )
    {
        return new Predicate<Node>()
        {
            @Override
            public boolean accept( Node node )
            {
                return node.getProperty( "id", "" ).equals( id );
            }
        };
    }

    protected void assertNodeHasLabels( Node node, String[] names )
    {
        for ( String name : names )
        {
            assertTrue( node + " didn't have label " + name + ", it had labels " + node.getLabels(),
                    node.hasLabel( label( name ) ) );
        }
    }

    private void verifyData()
    {
        verifyData( Validators.<Node>emptyValidator(), Validators.<Relationship>emptyValidator() );
    }

    private void verifyData(
            Validator<Node> nodeAdditionalValidation,
            Validator<Relationship> relationshipAdditionalValidation )
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            int nodeCount = 0, relationshipCount = 0;
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                assertTrue( node.hasProperty( "name" ) );
                nodeAdditionalValidation.validate( node );
                nodeCount++;
            }
            assertEquals( NODE_COUNT, nodeCount );
            for ( Relationship relationship : GlobalGraphOperations.at( db ).getAllRelationships() )
            {
                assertTrue( relationship.hasProperty( "created" ) );
                relationshipAdditionalValidation.validate( relationship );
                relationshipCount++;
            }
            assertEquals( RELATIONSHIP_COUNT, relationshipCount );
            tx.success();
        }
    }

    private void verifyRelationships( List<RelationshipDataLine> relationships )
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        Map<String,Node> nodesById = allNodesById( db );
        try ( Transaction tx = db.beginTx() )
        {
            for ( RelationshipDataLine relationship : relationships )
            {
                Node startNode = nodesById.get( relationship.startNodeId );
                Node endNode = nodesById.get( relationship.endNodeId );
                if ( startNode == null || endNode == null )
                {
                    // OK this is a relationship refering to a missing node, skip it
                    continue;
                }
                assertNotNull( relationship.toString(), findRelationship( startNode, endNode, relationship ) );
            }
            tx.success();
        }
    }

    private Relationship findRelationship( Node startNode, final Node endNode, final RelationshipDataLine relationship )
    {
        return singleOrNull( filter( new Predicate<Relationship>()
        {
            @Override
            public boolean accept( Relationship item )
            {
                return item.getEndNode().equals( endNode ) && item.getProperty( "name" ).equals( relationship.name );
            }
        }, startNode.getRelationships( withName( relationship.type ) ).iterator() ) );
    }

    private Map<String,Node> allNodesById( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Map<String,Node> nodes = new HashMap<>();
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                nodes.put( idOf( node ), node );
            }
            tx.success();
            return nodes;
        }
    }

    private String idOf( Node node )
    {
        return (String) node.getProperty( "id" );
    }

    private List<String> nodeIds()
    {
        return nodeIds( NODE_COUNT );
    }

    private List<String> nodeIds( int count )
    {
        List<String> ids = new ArrayList<>();
        for ( int i = 0; i < count; i++ )
        {
            ids.add( randomNodeId() );
        }
        return ids;
    }

    private String randomNodeId()
    {
        return UUID.randomUUID().toString();
    }

    private File nodeData( boolean includeHeader, Configuration config, List<String> nodeIds,
            PrimitiveIntPredicate linePredicate ) throws Exception
    {
        return nodeData( includeHeader, config, nodeIds, linePredicate, Charset.defaultCharset() );
    }

    private File nodeData( boolean includeHeader, Configuration config, List<String> nodeIds,
            PrimitiveIntPredicate linePredicate, Charset encoding ) throws Exception
    {
        File file = file( fileName( "nodes.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            if ( includeHeader )
            {
                writeNodeHeader( writer, config, null );
            }
            writeNodeData( writer, config, nodeIds, linePredicate );
        }
        return file;
    }

    private PrintStream writer( File file, Charset encoding ) throws Exception
    {
        return new PrintStream( file, encoding.name() );
    }

    private File nodeHeader( Configuration config ) throws Exception
    {
        return nodeHeader( config, null );
    }

    private File nodeHeader( Configuration config, String idGroup ) throws Exception
    {
        return nodeHeader( config, idGroup, Charset.defaultCharset() );
    }

    private File nodeHeader( Configuration config, String idGroup, Charset encoding ) throws Exception
    {
        File file = file( fileName( "nodes-header.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            writeNodeHeader( writer, config, idGroup );
        }
        return file;
    }

    private void writeNodeHeader( PrintStream writer, Configuration config, String idGroup )
    {
        char delimiter = config.delimiter();
        writer.println( idEntry( "id", Type.ID, idGroup ) + delimiter + "name" + delimiter + "labels:LABEL" );
    }

    private String idEntry( String name, Type type, String idGroup )
    {
        return (name != null ? name : "") + ":" + type.name() + (idGroup != null ? "(" + idGroup + ")" : "");
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
            PrimitiveIntPredicate linePredicate, boolean specifyType ) throws Exception
    {
        return relationshipData( includeHeader, config, nodeIds, linePredicate, specifyType, Charset.defaultCharset() );
    }

    private File relationshipData( boolean includeHeader, Configuration config, List<String> nodeIds,
            PrimitiveIntPredicate linePredicate, boolean specifyType, Charset encoding ) throws Exception
    {
        return relationshipData( includeHeader, config, randomRelationships( nodeIds ), linePredicate,
                specifyType, encoding );
    }

    private File relationshipData( boolean includeHeader, Configuration config,
            Iterator<RelationshipDataLine> data, PrimitiveIntPredicate linePredicate,
            boolean specifyType ) throws Exception
    {
        return relationshipData( includeHeader, config, data, linePredicate, specifyType, Charset.defaultCharset() );
    }

    private File relationshipData( boolean includeHeader, Configuration config,
            Iterator<RelationshipDataLine> data, PrimitiveIntPredicate linePredicate,
            boolean specifyType, Charset encoding ) throws Exception
    {
        File file = file( fileName( "relationships.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            if ( includeHeader )
            {
                writeRelationshipHeader( writer, config, null, null, specifyType );
            }
            writeRelationshipData( writer, config, data, linePredicate, specifyType );
        }
        return file;
    }

    private File relationshipHeader( Configuration config ) throws Exception
    {
        return relationshipHeader( config, Charset.defaultCharset() );
    }

    private File relationshipHeader( Configuration config, Charset encoding ) throws Exception
    {
        return relationshipHeader( config, null, null, true, encoding );
    }

    private File relationshipHeader( Configuration config, String startIdGroup, String endIdGroup, boolean specifyType )
            throws Exception
    {
        return relationshipHeader( config, startIdGroup, endIdGroup, specifyType, Charset.defaultCharset() );
    }

    private File relationshipHeader( Configuration config, String startIdGroup, String endIdGroup, boolean specifyType,
            Charset encoding ) throws Exception
    {
        File file = file( fileName( "relationships-header.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            writeRelationshipHeader( writer, config, startIdGroup, endIdGroup, specifyType );
        }
        return file;
    }

    private String fileName( String name )
    {
        return dataIndex++ + "-" + name;
    }

    private File file( String localname )
    {
        return new File( dbRule.getStoreDir(), localname );
    }

    private void writeRelationshipHeader( PrintStream writer, Configuration config,
            String startIdGroup, String endIdGroup, boolean specifyType )
    {
        char delimiter = config.delimiter();
        writer.println(
                idEntry( null, Type.START_ID, startIdGroup ) + delimiter +
                idEntry( null, Type.END_ID, endIdGroup ) +
                (specifyType ? (delimiter + ":" + Type.TYPE) : "") +
                delimiter + "created:long" +
                delimiter + "name:String" );
    }

    private static class RelationshipDataLine
    {
        private final String startNodeId;
        private final String endNodeId;
        private final String type;
        private final String name;

        RelationshipDataLine( String startNodeId, String endNodeId, String type, String name )
        {
            this.startNodeId = startNodeId;
            this.endNodeId = endNodeId;
            this.type = type;
            this.name = name;
        }

        @Override
        public String toString()
        {
            return "RelationshipDataLine [startNodeId=" + startNodeId + ", endNodeId=" + endNodeId + ", type=" + type
                    + ", name=" + name + "]";
        }
    }

    private static RelationshipDataLine relationship( String startNodeId, String endNodeId, String type )
    {
        return relationship( startNodeId, endNodeId, type, null );
    }

    private static RelationshipDataLine relationship( String startNodeId, String endNodeId, String type, String name )
    {
        return new RelationshipDataLine( startNodeId, endNodeId, type, name );
    }

    private void writeRelationshipData( PrintStream writer, Configuration config,
            Iterator<RelationshipDataLine> data, PrimitiveIntPredicate linePredicate, boolean specifyType )
    {
        char delimiter = config.delimiter();
        for ( int i = 0; i < RELATIONSHIP_COUNT; i++ )
        {
            if ( !data.hasNext() )
            {
                break;
            }
            RelationshipDataLine entry = data.next();
            if ( linePredicate.accept( i ) )
            {
                writer.println( entry.startNodeId +
                        delimiter + entry.endNodeId +
                        (specifyType ? (delimiter + entry.type) : "") +
                        delimiter + currentTimeMillis() +
                        delimiter + (entry.name != null ? entry.name : "")
                        );
            }
        }
    }

    private Iterator<RelationshipDataLine> randomRelationships( final List<String> nodeIds )
    {
        return new PrefetchingIterator<RelationshipDataLine>()
        {
            @Override
            protected RelationshipDataLine fetchNextOrNull()
            {
                return new RelationshipDataLine(
                        nodeIds.get( random.nextInt( nodeIds.size() ) ),
                        nodeIds.get( random.nextInt( nodeIds.size() ) ),
                        randomType(),
                        null );
            }
        };
    }

    private void assertExceptionContains( Exception e, String message, Class<? extends Exception> type )
            throws Exception
    {
        if ( !contains( e, message, type ) )
        {   // Rethrow the exception since we'd like to see what it was instead
            throw withMessage( e,
                    format( "Expected exception to contain cause '%s', %s. but was %s", message, type, e ) );
        }
    }

    private String randomType()
    {
        return "TYPE_" + random.nextInt( 4 );
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

    private void importTool( String... arguments )
    {
        ImportTool.main( arguments, true );
    }

    private static final int RELATIONSHIP_COUNT = 10_000;
    private static final int NODE_COUNT = 100;

    @Rule
    public final EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( getClass() ).startLazily();
    public final @Rule RandomRule random = new RandomRule();
    public final @Rule Mute mute = Mute.mute( Mute.System.values() );
    private int dataIndex;
}
