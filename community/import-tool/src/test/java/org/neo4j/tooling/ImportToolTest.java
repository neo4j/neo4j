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
package org.neo4j.tooling;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import org.neo4j.csv.reader.IllegalMultilineFieldException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.Version;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.repeat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.helpers.ArrayUtil.join;
import static org.neo4j.helpers.Exceptions.contains;
import static org.neo4j.helpers.Exceptions.withMessage;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.tooling.ImportTool.MULTI_FILE_DELIMITER;
import static org.neo4j.unsafe.impl.batchimport.Configuration.BAD_FILE_NAME;

public class ImportToolTest
{
    private static final int MAX_LABEL_ID = 4;
    private static final int RELATIONSHIP_COUNT = 10_000;
    private static final int NODE_COUNT = 100;
    private static final IntPredicate TRUE = i -> true;

    @Rule
    public final EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule().startLazily();
    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppress( SuppressOutput.System.values() );
    private int dataIndex;

    @Test
    public void usageMessageIncludeExample() throws Exception
    {
        SuppressOutput.Voice outputVoice = suppressOutput.getOutputVoice();
        importTool( "?" );
        assertTrue( "Usage message should include example section, but was:" + outputVoice,
                outputVoice.containsMessage( "Example:" ) );
    }

    @Test
    public void usageMessagePrintedOnEmptyInputParameters() throws Exception
    {
        SuppressOutput.Voice outputVoice = suppressOutput.getOutputVoice();
        importTool();
        assertTrue( "Output should include usage section, but was:" + outputVoice,
                outputVoice.containsMessage( "Example:" ) );
    }

    @Test
    public void shouldImportWithAsManyDefaultsAsAvailable() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, nodeIds, TRUE, true ).getAbsolutePath() );

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
                nodeData( false, config, nodeIds, TRUE ).getAbsolutePath(),
                "--relationships",
                relationshipHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                relationshipData( false, config, nodeIds, TRUE, true ).getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    public void import4097Labels() throws Exception
    {
        // GIVEN
        File header = file( fileName( "4097labels-header.csv" ) );
        try ( PrintStream writer = new PrintStream( header )  )
        {
            writer.println( ":LABEL" );
        }
        File data = file( fileName( "4097labels.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            // Need to have unique names in order to get unique ids for labels. Want 4096 unique label ids present.
            for ( int i = 0; i < 4096; i++ )
            {
                writer.println( "SIMPLE" + i );
            }
            // Then insert one with 3 array entries which will get ids greater than 4096. These cannot be inlined
            // due 36 bits being divided into 3 parts of 12 bits each and 4097 > 2^12, thus these labels will be
            // need to be dynamic records.
            writer.println( "FIRST 4096|SECOND 4096|" );
        }

        // WHEN
        importTool( "--into", dbRule.getStoreDirAbsolutePath(),
                "--delimiter", "TAB",
                "--array-delimiter", "|",
                "--nodes", header.getAbsolutePath() + MULTI_FILE_DELIMITER + data.getAbsolutePath() );

        // THEN
        try ( Transaction tx = dbRule.beginTx() )
        {
            long nodeCount = Iterables.count( dbRule.getAllNodes() );
            assertEquals( 4097, nodeCount );

            tx.success();
            ResourceIterator<Node> nodes = dbRule.findNodes( label( "FIRST 4096" ) );
            assertEquals( 1, Iterators.asList( nodes ).size() );
            nodes = dbRule.findNodes( label( "SECOND 4096" ) );
            assertEquals( 1, Iterators.asList( nodes ).size() );
        }
    }

    @Test
    public void shouldIgnoreWhitespaceAroundIntegers() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        List<String> values = Arrays.asList( "17", "    21", "99   ", "  34  ", "-34", "        -12", "-92 " );

        File data = file( fileName( "whitespace.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            writer.println( ":LABEL,name,s:short,b:byte,i:int,l:long,f:float,d:double" );

            // For each test value
            for ( String value : values )
            {
                // Save value as a String in name
                writer.print( "PERSON,'" + value + "'" );
                // For each numerical type
                for ( int j = 0; j < 6; j++ )
                {
                    writer.print( "," + value );
                }
                // End line
                writer.println();
            }
        }

        // WHEN
        importTool( "--into", dbRule.getStoreDirAbsolutePath(), "--quote", "'", "--nodes", data.getAbsolutePath() );

        // THEN
        int nodeCount = 0;
        try ( Transaction tx = dbRule.beginTx() )
        {
            for ( Node node : dbRule.getAllNodes() )
            {
                nodeCount++;
                String name = (String) node.getProperty( "name" );

                String expected = name.trim();

                assertEquals( 7, node.getAllProperties().size() );
                for ( String key : node.getPropertyKeys() )
                {
                    if ( key.equals( "name" ) )
                    {
                        continue;
                    }
                    else if ( key.equals( "f" ) || key.equals( "d" ) )
                    {
                        // Floating points have decimals
                        expected = String.valueOf( Double.parseDouble( expected ) );
                    }

                    assertEquals( "Wrong value for " + key, expected, node.getProperty( key ).toString() );
                }
            }

            tx.success();
        }

        assertEquals( values.size(), nodeCount );
    }

    @Test
    public void shouldIgnoreWhitespaceAroundDecimalNumbers() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        List<String> values = Arrays.asList( "1.0", "   3.5", "45.153    ", "   925.12   ", "-2.121", "   -3.745",
                "-412.153    ", "   -5.12   " );

        File data = file( fileName( "whitespace.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            writer.println( ":LABEL,name,f:float,d:double" );

            // For each test value
            for ( String value : values )
            {
                // Save value as a String in name
                writer.print( "PERSON,'" + value + "'" );
                // For each numerical type
                for ( int j = 0; j < 2; j++ )
                {
                    writer.print( "," + value );
                }
                // End line
                writer.println();
            }
        }

        // WHEN
        importTool( "--into", dbRule.getStoreDirAbsolutePath(), "--quote", "'", "--nodes", data.getAbsolutePath() );

        // THEN
        int nodeCount = 0;
        try ( Transaction tx = dbRule.beginTx() )
        {
            for ( Node node : dbRule.getAllNodes() )
            {
                nodeCount++;
                String name = (String) node.getProperty( "name" );

                double expected = Double.parseDouble( name.trim() );

                assertEquals( 3, node.getAllProperties().size() );
                for ( String key : node.getPropertyKeys() )
                {
                    if ( key.equals( "name" ) )
                    {
                        continue;
                    }

                    assertTrue( "Wrong value for " + key,
                            expected == Double.valueOf( node.getProperty( key ).toString() ) );
                }
            }

            tx.success();
        }

        assertEquals( values.size(), nodeCount );
    }

    @Test
    public void shouldIgnoreWhitespaceAroundBooleans() throws Exception
    {
        // GIVEN
        File data = file( fileName( "whitespace.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            writer.println( ":LABEL,name,adult:boolean" );

            writer.println( "PERSON,'t1',true" );
            writer.println( "PERSON,'t2',  true" );
            writer.println( "PERSON,'t3',true  " );
            writer.println( "PERSON,'t4',  true  " );

            writer.println( "PERSON,'f1',false" );
            writer.println( "PERSON,'f2',  false" );
            writer.println( "PERSON,'f3',false  " );
            writer.println( "PERSON,'f4',  false  " );
            writer.println( "PERSON,'f5',  truebutactuallyfalse  " );

            writer.println( "PERSON,'f6',  non true things are interpreted as false  " );
        }

        // WHEN
        importTool( "--into", dbRule.getStoreDirAbsolutePath(), "--quote", "'", "--nodes", data.getAbsolutePath() );

        // THEN
        try ( Transaction tx = dbRule.beginTx() )
        {
            for ( Node node : dbRule.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                if ( name.startsWith( "t" ) )
                {
                    assertTrue( "Wrong value on " + name, (boolean) node.getProperty( "adult" ) );
                }
                else
                {
                    assertFalse( "Wrong value on " + name, (boolean) node.getProperty( "adult" ) );
                }
            }

            long nodeCount = Iterables.count( dbRule.getAllNodes() );
            assertEquals( 10, nodeCount );
            tx.success();
        }
    }

    @Test
    public void shouldIgnoreWhitespaceInAndAroundIntegerArrays() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values = new String[]{ "   17", "21", "99   ", "  34  ", "-34", "        -12", "-92 " };

        File data = writeArrayCsv(
                new String[]{ "s:short[]", "b:byte[]", "i:int[]", "l:long[]", "f:float[]", "d:double[]" }, values );

        // WHEN
        importTool( "--into", dbRule.getStoreDirAbsolutePath(), "--quote", "'", "--nodes", data.getAbsolutePath() );

        // THEN
        // Expected value for integer types
        String iExpected = joinStringArray( values );

        // Expected value for floating point types
        String fExpected = Arrays.stream( values ).map( String::trim )
                                                  .map( Double::valueOf )
                                                  .map( String::valueOf )
                                                  .collect( joining( ", ", "[", "]")  );

        int nodeCount = 0;
        try ( Transaction tx = dbRule.beginTx() )
        {
            for ( Node node : dbRule.getAllNodes() )
            {
                nodeCount++;

                assertEquals( 6, node.getAllProperties().size() );
                for ( String key : node.getPropertyKeys() )
                {
                    Object things = node.getProperty( key );
                    String result = "";
                    String expected = iExpected;
                    switch ( key )
                    {
                    case "s":
                        result = Arrays.toString( (short[]) things );
                        break;
                    case "b":
                        result = Arrays.toString( (byte[]) things );
                        break;
                    case "i":
                        result = Arrays.toString( (int[]) things );
                        break;
                    case "l":
                        result = Arrays.toString( (long[]) things );
                        break;
                    case "f":
                        result = Arrays.toString( (float[]) things );
                        expected = fExpected;
                        break;
                    case "d":
                        result = Arrays.toString( (double[]) things );
                        expected = fExpected;
                        break;
                    default:
                        break;
                    }

                    assertEquals( expected, result );
                }
            }

            tx.success();
        }

        assertEquals( 1, nodeCount );
    }

    @Test
    public void shouldIgnoreWhitespaceInAndAroundDecimalArrays() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values =
                new String[]{ "1.0", "   3.5", "45.153    ", "   925.12   ", "-2.121", "   -3.745", "-412.153    ",
                        "   -5.12   " };

        File data = writeArrayCsv( new String[]{ "f:float[]", "d:double[]" }, values );

        // WHEN
        importTool( "--into", dbRule.getStoreDirAbsolutePath(), "--quote", "'", "--nodes", data.getAbsolutePath() );

        // THEN
        String expected = joinStringArray( values );

        int nodeCount = 0;
        try ( Transaction tx = dbRule.beginTx() )
        {
            for ( Node node : dbRule.getAllNodes() )
            {
                nodeCount++;

                assertEquals( 2, node.getAllProperties().size() );
                for ( String key : node.getPropertyKeys() )
                {
                    Object things = node.getProperty( key );
                    String result = "";
                    switch ( key )
                    {
                    case "f":
                        result = Arrays.toString( (float[]) things );
                        break;
                    case "d":
                        result = Arrays.toString( (double[]) things );
                        break;
                    default:
                        break;
                    }

                    assertEquals( expected, result );
                }
            }

            tx.success();
        }

        assertEquals( 1, nodeCount );
    }

    @Test
    public void shouldIgnoreWhitespaceInAndAroundBooleanArrays() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values =
                new String[]{ "true", "  true", "true   ", "  true  ", " false ", "false ", " false", "false ",
                        " false" };
        String expected = joinStringArray( values );

        File data = writeArrayCsv( new String[]{ "b:boolean[]" }, values );

        // WHEN
        importTool( "--into", dbRule.getStoreDirAbsolutePath(), "--quote", "'", "--nodes", data.getAbsolutePath() );

        // THEN
        int nodeCount = 0;
        try ( Transaction tx = dbRule.beginTx() )
        {
            for ( Node node : dbRule.getAllNodes() )
            {
                nodeCount++;

                assertEquals( 1, node.getAllProperties().size() );
                for ( String key : node.getPropertyKeys() )
                {
                    Object things = node.getProperty( key );
                    String result = Arrays.toString( (boolean[]) things );

                    assertEquals( expected, result );
                }
            }

            tx.success();
        }

        assertEquals( 1, nodeCount );
    }

    @Test
    public void shouldFailIfHeaderHasLessColumnsThanData() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN data file contains more columns than header file
        int extraColumns = 3;
        try
        {
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--delimiter", "TAB",
                    "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                    "--nodes", nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                            nodeData( false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns )
                                    .getAbsolutePath(),
                    "--relationships", relationshipHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                            relationshipData( false, config, nodeIds, TRUE, true ).getAbsolutePath() );

            fail( "Should have thrown exception" );
        }
        catch ( InputException e )
        {
            // THEN
            assertFalse( suppressOutput.getErrorVoice().containsMessage( e.getClass().getName() ) );
            assertTrue( e.getMessage().contains( "Extra column not present in header on line" ) );
        }
    }

    @Test
    public void shouldWarnIfHeaderHasLessColumnsThanDataWhenToldTo() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;
        File bad = badFile();

        // WHEN data file contains more columns than header file
        int extraColumns = 3;
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--bad", bad.getAbsolutePath(),
                "--bad-tolerance", Integer.toString( nodeIds.size() * extraColumns ),
                "--ignore-extra-columns",
                "--delimiter", "TAB",
                "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                "--nodes", nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                        nodeData( false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns )
                                .getAbsolutePath(),
                "--relationships", relationshipHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                        relationshipData( false, config, nodeIds, TRUE, true ).getAbsolutePath() );

        // THEN
        String badContents = FileUtils.readTextFile( bad, Charset.defaultCharset() );
        assertTrue( badContents.contains( "Extra column not present in header on line" ) );
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
                nodeData( false, config, nodeIds, lines( 0, NODE_COUNT / 2 ) ).getAbsolutePath(),
                "--nodes", // One group with two data files, where the header sits in the first file
                nodeData( true, config, nodeIds,
                        lines( NODE_COUNT / 2, NODE_COUNT * 3 / 4 ) ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                nodeData( false, config, nodeIds, lines( NODE_COUNT * 3 / 4, NODE_COUNT ) ).getAbsolutePath(),
                "--relationships",
                relationshipHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                relationshipData( false, config, nodeIds, TRUE, true ).getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    public void shouldImportMultipleInputsWithAddedLabelsAndDefaultRelationshipType() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        final String[] firstLabels = {"AddedOne", "AddedTwo"};
        final String[] secondLabels = {"AddedThree"};
        final String firstType = "TYPE_1";
        final String secondType = "TYPE_2";

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes:" + join( firstLabels, ":" ),
                nodeData( true, config, nodeIds, lines( 0, NODE_COUNT / 2 ) ).getAbsolutePath(),
                "--nodes:" + join( secondLabels, ":" ),
                nodeData( true, config, nodeIds, lines( NODE_COUNT / 2, NODE_COUNT ) ).getAbsolutePath(),
                "--relationships:" + firstType,
                relationshipData( true, config, nodeIds, lines( 0, RELATIONSHIP_COUNT / 2 ), false ).getAbsolutePath(),
                "--relationships:" + secondType,
                relationshipData( true, config, nodeIds,
                        lines( RELATIONSHIP_COUNT / 2, RELATIONSHIP_COUNT ), false ).getAbsolutePath() );

        // THEN
        verifyData(
                node ->
                {
                    if ( node.getId() < NODE_COUNT / 2 )
                    {
                        assertNodeHasLabels( node, firstLabels );
                    }
                    else
                    {
                        assertNodeHasLabels( node, secondLabels );
                    }
                },
                relationship ->
                {
                    if ( relationship.getId() < RELATIONSHIP_COUNT / 2 )
                    {
                        assertEquals( firstType, relationship.getType().name() );
                    }
                    else
                    {
                        assertEquals( secondType, relationship.getType().name() );
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
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath() );
        // no relationships

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            int nodeCount = 0;
            for ( Node node : db.getAllNodes() )
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
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeHeader( config, groupOne ) + MULTI_FILE_DELIMITER +
                           nodeData( false, config, groupOneNodeIds, TRUE ),
                "--nodes", nodeHeader( config, groupTwo ) + MULTI_FILE_DELIMITER +
                           nodeData( false, config, groupTwoNodeIds, TRUE ),
                "--relationships", relationshipHeader( config, groupOne, groupTwo, true ) + MULTI_FILE_DELIMITER +
                                   relationshipData( false, config, rels.iterator(), TRUE, true ) );

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            int nodeCount = 0;
            for ( Node node : db.getAllNodes() )
            {
                assertTrue( node.hasProperty( "name" ) );
                nodeCount++;
                assertEquals( 1, Iterables.count( node.getRelationships() ) );
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
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--nodes", nodeHeader( config, "MyGroup" ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                               nodeData( false, config, groupOneNodeIds, TRUE ).getAbsolutePath(),
                    "--nodes", nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                               nodeData( false, config, groupTwoNodeIds, TRUE ).getAbsolutePath() );
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
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                // there will be no :TYPE specified in the header of the relationships below
                "--relationships:" + type,
                relationshipData( true, config, nodeIds, TRUE, false ).getAbsolutePath() );

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
                    "--into", dbRule.getStoreDirAbsolutePath(),
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
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--skip-duplicate-nodes",
                "--nodes", nodeHeaderFile.getAbsolutePath() + MULTI_FILE_DELIMITER +
                           nodeData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                           nodeData2.getAbsolutePath() );

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            // there should not be duplicates of any node
            Iterator<Node> nodes = db.getAllNodes().iterator();
            Iterator<String> expectedIds = FilteringIterator.noDuplicates( nodeIds.iterator() );
            while ( expectedIds.hasNext() )
            {
                assertTrue( nodes.hasNext() );
                assertEquals( expectedIds.next(), nodes.next().getProperty( "id" ) );
            }
            assertFalse( nodes.hasNext() );

            // also all nodes in the label index should exist
            for ( int i = 0; i < MAX_LABEL_ID; i++ )
            {
                Label label = label( labelName( i ) );
                try ( ResourceIterator<Node> nodesByLabel = db.findNodes( label ) )
                {
                    while ( nodesByLabel.hasNext() )
                    {
                        assertTrue( nodesByLabel.next().hasLabel( label ) );
                    }
                }
            }

            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldLogRelationshipsReferringToMissingNode() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c" );
        Configuration config = Configuration.COMMAS;
        File nodeData = nodeData( true, config, nodeIds, TRUE );
        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship( "a", "b", "TYPE", "aa" ), //          line 2 of file1
                relationship( "c", "bogus", "TYPE", "bb" ), //      line 3 of file1
                relationship( "b", "c", "KNOWS", "cc" ), //         line 1 of file2
                relationship( "c", "a", "KNOWS", "dd" ), //         line 2 of file2
                relationship( "missing", "a", "KNOWS", "ee" ) ); // line 3 of file2
        File relationshipData1 = relationshipData( true, config, relationships.iterator(), lines( 0, 2 ), true );
        File relationshipData2 = relationshipData( false, config, relationships.iterator(), lines( 2, 5 ), true );
        File bad = badFile();

        // WHEN importing data where some relationships refer to missing nodes
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeData.getAbsolutePath(),
                "--bad", bad.getAbsolutePath(),
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
    public void skipLoggingOfBadEntries() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c" );
        Configuration config = Configuration.COMMAS;
        File nodeData = nodeData( true, config, nodeIds, TRUE );
        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship( "a", "b", "TYPE", "aa" ), //          line 2 of file1
                relationship( "c", "bogus", "TYPE", "bb" ), //      line 3 of file1
                relationship( "b", "c", "KNOWS", "cc" ), //         line 1 of file2
                relationship( "c", "a", "KNOWS", "dd" ), //         line 2 of file2
                relationship( "missing", "a", "KNOWS", "ee" ) ); // line 3 of file2
        File relationshipData1 = relationshipData( true, config, relationships.iterator(), lines( 0, 2 ), true );
        File relationshipData2 = relationshipData( false, config, relationships.iterator(), lines( 2, 5 ), true );

        // WHEN importing data where some relationships refer to missing nodes
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeData.getAbsolutePath(),
                "--bad-tolerance", "2",
                "--skip-bad-entries-logging", "true",
                "--relationships", relationshipData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                        relationshipData2.getAbsolutePath() );

        assertFalse( badFile().exists() );
        verifyRelationships( relationships );
    }

    @Test
    public void shouldFailIfTooManyBadRelationships() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c" );
        Configuration config = Configuration.COMMAS;
        File nodeData = nodeData( true, config, nodeIds, TRUE );
        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship( "a", "b", "TYPE" ), //          line 2 of file1
                relationship( "c", "bogus", "TYPE" ), //      line 3 of file1
                relationship( "b", "c", "KNOWS" ), //         line 1 of file2
                relationship( "c", "a", "KNOWS" ), //         line 2 of file2
                relationship( "missing", "a", "KNOWS" ) ); // line 3 of file2
        File relationshipData1 = relationshipData( true, config, relationships.iterator(), lines( 0, 2 ), true );
        File relationshipData2 = relationshipData( false, config, relationships.iterator(), lines( 2, 5 ), true );
        File bad = badFile();

        // WHEN importing data where some relationships refer to missing nodes
        try
        {
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--nodes", nodeData.getAbsolutePath(),
                    "--bad", bad.getAbsolutePath(),
                    "--bad-tolerance", "1",
                    "--relationships", relationshipData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                                       relationshipData2.getAbsolutePath() );
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
        File nodeData = nodeData( true, config, nodeIds, TRUE );

        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship( "a", "b", "TYPE" ), //          line 2 of file1
                relationship( "c", "bogus", "TYPE" ) ); //    line 3 of file1

        File relationshipData1 = relationshipData( true, config, relationships.iterator(), lines( 0, 2 ), true );
        File relationshipData2 = relationshipData( false, config, relationships.iterator(), lines( 2, 5 ), true );
        File bad = badFile();

        // WHEN importing data where some relationships refer to missing nodes
        try
        {
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--nodes", nodeData.getAbsolutePath(),
                    "--bad", bad.getAbsolutePath(),
                    "--stacktrace", // trying to find flaky test origin
                    "--skip-bad-relationships", "false",
                    "--relationships", relationshipData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                                       relationshipData2.getAbsolutePath() );
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
        final Label label1 = label( "My First Label" );
        final Label label2 = label( "My Other Label" );

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes:My First Label:My Other Label",
                nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, nodeIds, TRUE, true ).getAbsolutePath() );

        // THEN
        verifyData( node ->
        {
            assertTrue( node.hasLabel( label1 ) );
            assertTrue( node.hasLabel( label2 ) );
        }, Validators.emptyValidator() );
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
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--input-encoding", charset.name(),
                "--nodes", nodeData( true, config, nodeIds, TRUE, charset ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, nodeIds, TRUE, true, charset )
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
                    "--relationships",
                    relationshipData( true, config, nodeIds, TRUE, true ).getAbsolutePath() );
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
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, relationshipData.iterator(),
                        TRUE, true ).getAbsolutePath() );

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            Iterable<Node> allNodes = db.getAllNodes();
            int anonymousCount = 0;
            for ( final String id : nodeIds )
            {
                if ( id.isEmpty() )
                {
                    anonymousCount++;
                }
                else
                {
                    assertNotNull( Iterators.single( Iterators.filter( nodeFilter( id ), allNodes.iterator() ) ) );
                }
            }
            assertEquals( anonymousCount, count( Iterators.filter( nodeFilter( "" ), allNodes.iterator() ) ) );
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
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--nodes", data.getAbsolutePath() );
        }
        catch ( Exception e )
        {
            // THEN
            assertExceptionContains( e, "Multi-line", IllegalMultilineFieldException.class );
        }
    }

    @Test
    public void shouldNotTrimStringsByDefault() throws Exception
    {
        // GIVEN
        String name = "  This is a line with leading and trailing whitespaces   ";
        File data = data( ":ID,name", "1,\"" + name + "\"");

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", data.getAbsolutePath() );

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> allNodes = db.getAllNodes().iterator();
            Node node = Iterators.single( allNodes );
            allNodes.close();

            assertEquals( name, node.getProperty( "name" ) );

            tx.success();
        }
    }

    @Test
    public void shouldTrimStringsIfConfiguredTo() throws Exception
    {
        // GIVEN
        String name = "  This is a line with leading and trailing whitespaces   ";
        File data = data( ":ID,name", "1,\"" + name + "\"");

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--trim-strings", "true" );

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> allNodes = db.getAllNodes().iterator();
            Node node = Iterators.single( allNodes );
            allNodes.close();

            assertEquals( name.trim(), node.getProperty( "name" ) );

            tx.success();
        }
    }

    @Test
    public void shouldPrintReferenceLinkOnDataImportErrors() throws Exception
    {
        String[] versionParts = Version.getNeo4jVersion().split("-");
        versionParts[0] = versionParts[0].substring(0, 3);
        String docsVersion = String.join("-", versionParts);

        shouldPrintReferenceLinkAsPartOfErrorMessage( nodeIds(),
                Iterators.iterator( new RelationshipDataLine( "1", "", "type", "name" ) ),
                "Relationship missing mandatory field 'END_ID', read more about relationship " +
                "format in the manual:  https://neo4j.com/docs/operations-manual/" +
                docsVersion +
                "/tools/import/file-header-format/#import-tool-header-format-rels" );
        shouldPrintReferenceLinkAsPartOfErrorMessage( nodeIds(),
                Iterators.iterator( new RelationshipDataLine( "", "1", "type", "name" ) ),
                "Relationship missing mandatory field 'START_ID', read more about relationship " +
                "format in the manual:  https://neo4j.com/docs/operations-manual/" +
                docsVersion +
                "/tools/import/file-header-format/#import-tool-header-format-rels" );
        shouldPrintReferenceLinkAsPartOfErrorMessage( nodeIds(),
                Iterators.iterator( new RelationshipDataLine( "1", "2", "", "name" ) ),
                "Relationship missing mandatory field 'TYPE', read more about relationship " +
                "format in the manual:  https://neo4j.com/docs/operations-manual/" +
                 docsVersion +
                "/tools/import/file-header-format/#import-tool-header-format-rels" );
        shouldPrintReferenceLinkAsPartOfErrorMessage( Arrays.asList( "1", "1" ),
                Iterators.iterator( new RelationshipDataLine( "1", "2", "type", "name" ) ),
                "Duplicate input ids that would otherwise clash can be put into separate id space, read more " +
                "about how to use id spaces in the manual: https://neo4j.com/docs/operations-manual/" +
                docsVersion +
                "/tools/import/file-header-format/#import-tool-id-spaces" );
    }

    @Test
    public void shouldCollectUnlimitedNumberOfBadEntries() throws Exception
    {
        // GIVEN
        List<String> nodeIds = Collections.nCopies( 10_000, "A" );

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(),
                "--skip-duplicate-nodes",
                "--bad-tolerance", "true" );

        // THEN
        // all those duplicates should just be accepted using the - for specifying bad tolerance
    }

    private void shouldPrintReferenceLinkAsPartOfErrorMessage( List<String> nodeIds,
            Iterator<RelationshipDataLine> relationshipDataLines, String message ) throws Exception
    {
        Configuration config = Configuration.COMMAS;
        try
        {
            // WHEN
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                    "--skip-bad-relationships", "false",
                    "--relationships", relationshipData( true, config, relationshipDataLines,
                            TRUE, true ).getAbsolutePath() );
            fail( " Should fail during import." );
        }
        catch ( Exception e )
        {
            // EXPECT
            assertTrue( suppressOutput.getErrorVoice().containsMessage( message ) );
        }

        for ( StoreType storeType : StoreType.values() )
        {
            if ( storeType.isRecordStore() )
            {
                new File( dbRule.getStoreDirFile(), DEFAULT_NAME + storeType.getStoreName() ).delete();
            }
        }
    }

    @Test
    public void shouldAllowMultilineFieldsWhenEnabled() throws Exception
    {
        // GIVEN
        File data = data( ":ID,name", "1,\"This is a line with\nnewlines in\"" );

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--multiline-fields", "true" );

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> allNodes = db.getAllNodes().iterator();
            Node node = Iterators.single( allNodes );
            allNodes.close();

            assertEquals( "This is a line with\nnewlines in", node.getProperty( "name" ) );

            tx.success();
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
        GraphDatabaseService graphDatabaseService = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = graphDatabaseService.beginTx() )
        {
            ResourceIterator<Node> allNodes = graphDatabaseService.getAllNodes().iterator();
            assertFalse( "Expected database to be empty", allNodes.hasNext() );
            tx.success();
        }
    }

    @Test
    public void shouldIgnoreEmptyQuotedStringsIfConfiguredTo() throws Exception
    {
        // GIVEN
        File data = data(
                ":ID,one,two,three",
                "1,\"\",,value" );

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--ignore-empty-strings", "true" );

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = Iterables.single( db.getAllNodes() );
            assertEquals( "three", Iterables.single( node.getPropertyKeys() ) );
            tx.success();
        }
    }

    @Test
    public void shouldPrintUserFriendlyMessageAboutUnsupportedMultilineFields() throws Exception
    {
        // GIVEN
        File data = data(
                ":ID,name",
                "1,\"one\ntwo\nthree\"",
                "2,four" );

        try
        {
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--nodes", data.getAbsolutePath(),
                    "--multiline-fields", "false" );
            fail( "Should have failed" );
        }
        catch ( InputException e )
        {
            // THEN
            assertTrue( suppressOutput.getErrorVoice().containsMessage( "Detected field which spanned multiple lines" ) );
            assertTrue( suppressOutput.getErrorVoice().containsMessage( "multiline-fields" ) );
        }
    }

    @Test
    public void shouldAcceptRawAsciiCharacterCodeAsQuoteConfiguration() throws Exception
    {
        // GIVEN
        char weirdDelimiter = 1; // not '1', just the character represented with code 1, which seems to be SOH
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        File data = data(
                ":ID,name",
                "1," + name1,
                "2," + name2 );

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--quote", String.valueOf( weirdDelimiter ) );

        // THEN
        Set<String> names = asSet( "Weird", name2 );
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                assertTrue( "Didn't expect node with name '" + name + "'", names.remove( name ) );
            }
            assertTrue( names.isEmpty() );
            tx.success();
        }
    }

    @Test
    public void shouldAcceptSpecialTabCharacterAsDelimiterConfiguration() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--delimiter", "\\t",
                "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, nodeIds, TRUE, true ).getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    public void shouldReportBadDelimiterConfiguration() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN
        try
        {
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--delimiter", "\\bogus",
                    "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                    "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                    "--relationships", relationshipData( true, config, nodeIds, TRUE, true ).getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "bogus" ) );
        }
    }

    @Test
    public void shouldFailAndReportStartingLineForUnbalancedQuoteInMiddle() throws Exception
    {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        try
        {
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--nodes", nodeDataWithMissingQuote( 2 * unbalancedStartLine, unbalancedStartLine )
                            .getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( InputException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( String.format( "See line %d", unbalancedStartLine ) ) );
        }
    }

    @Test
    public void shouldAcceptRawEscapedAsciiCodeAsQuoteConfiguration() throws Exception
    {
        // GIVEN
        char weirdDelimiter = 1; // not '1', just the character represented with code 1, which seems to be SOH
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        File data = data(
                ":ID,name",
                "1," + name1,
                "2," + name2 );

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--quote", "\\1" );

        // THEN
        Set<String> names = asSet( "Weird", name2 );
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                assertTrue( "Didn't expect node with name '" + name + "'", names.remove( name ) );
            }
            assertTrue( names.isEmpty() );
            tx.success();
        }
    }

    @Test
    public void shouldFailAndReportStartingLineForUnbalancedQuoteAtEnd() throws Exception
    {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        try
        {
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--nodes", nodeDataWithMissingQuote( unbalancedStartLine, unbalancedStartLine ).getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( InputException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( String.format( "See line %d", unbalancedStartLine ) ) );
        }
    }

    @Test
    public void shouldBeEquivalentToUseRawAsciiOrCharacterAsQuoteConfiguration1() throws Exception
    {
        // GIVEN
        char weirdDelimiter = 126; // 126 ~ (tilde)
        String weirdStringDelimiter = "\\126";
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        File data = data(
                ":ID,name",
                "1," + name1,
                "2," + name2 );

        // WHEN given as raw ascii
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--quote", weirdStringDelimiter );

        // THEN
        assertEquals( "~", "" + weirdDelimiter );
        assertEquals( "~".charAt( 0 ), weirdDelimiter );

        Set<String> names = asSet( "Weird", name2 );
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                assertTrue( "Didn't expect node with name '" + name + "'", names.remove( name ) );
            }
            assertTrue( names.isEmpty() );
            tx.success();
        }
    }

    @Test
    public void shouldFailAndReportStartingLineForUnbalancedQuoteWithMultilinesEnabled() throws Exception
    {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        try
        {
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--multiline-fields", "true",
                    "--nodes",
                    nodeDataWithMissingQuote( 2 * unbalancedStartLine, unbalancedStartLine ).getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( InputException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( String.format( "started on line %d", unbalancedStartLine ) ) );
            // make sure end was reached
            assertThat( e.getMessage(), containsString( String.format( "line:%d", 2 * unbalancedStartLine + 1 ) ) );
        }
    }

    private File nodeDataWithMissingQuote( int totalLines, int unbalancedStartLine ) throws Exception
    {
        String[] lines = new String[totalLines + 1];

        lines[0] = "ID,:LABEL";

        for ( int i = 1; i <= totalLines; i++ )
        {
            StringBuilder line = new StringBuilder( String.format( "%d,", i ) );
            if ( i == unbalancedStartLine )
            {
                // Missing the end quote
                line.append( "\"Secret Agent" );
            }
            else
            {
                line.append( "Agent" );
            }
            lines[i] = line.toString();
        }

        return data( lines );
    }

    @Test
    public void shouldBeEquivalentToUseRawAsciiOrCharacterAsQuoteConfiguration2() throws Exception
    {
        // GIVEN
        char weirdDelimiter = 126; // 126 ~ (tilde)
        String weirdStringDelimiter = "~";
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        File data = data(
                ":ID,name",
                "1," + name1,
                "2," + name2 );

        // WHEN given as string
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--quote", weirdStringDelimiter );

        // THEN
        assertEquals( weirdStringDelimiter, "" + weirdDelimiter );
        assertEquals( weirdStringDelimiter.charAt( 0 ), weirdDelimiter );

        Set<String> names = asSet( "Weird", name2 );
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                assertTrue( "Didn't expect node with name '" + name + "'", names.remove( name ) );
            }
            assertTrue( names.isEmpty() );
            tx.success();
        }
    }

    @Test
    public void shouldRespectDbConfig() throws Exception
    {
        // GIVEN
        int arrayBlockSize = 10;
        int stringBlockSize = 12;
        File dbConfig = file( "neo4j.properties" );
        store( stringMap(
                GraphDatabaseSettings.array_block_size.name(), String.valueOf( arrayBlockSize ),
                GraphDatabaseSettings.string_block_size.name(), String.valueOf( stringBlockSize ) ), dbConfig );
        List<String> nodeIds = nodeIds();

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--db-config", dbConfig.getAbsolutePath(),
                "--nodes", nodeData( true, Configuration.COMMAS, nodeIds, value -> true ).getAbsolutePath() );

        // THEN
        NeoStores stores = dbRule.getGraphDatabaseAPI().getDependencyResolver()
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        int headerSize = Standard.LATEST_RECORD_FORMATS.dynamic().getRecordHeaderSize();
        assertEquals( arrayBlockSize + headerSize, stores.getPropertyStore().getArrayStore().getRecordSize() );
        assertEquals( stringBlockSize + headerSize, stores.getPropertyStore().getStringStore().getRecordSize() );
    }

    @Test
    public void useProvidedAdditionalConfig() throws Exception
    {
        // GIVEN
        int arrayBlockSize = 10;
        int stringBlockSize = 12;
        File dbConfig = file( "neo4j.properties" );
        store( stringMap(
                GraphDatabaseSettings.array_block_size.name(), String.valueOf( arrayBlockSize ),
                GraphDatabaseSettings.string_block_size.name(), String.valueOf( stringBlockSize ) ), dbConfig );
        List<String> nodeIds = nodeIds();

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", nodeData( true, Configuration.COMMAS, nodeIds, value -> true ).getAbsolutePath() );

        // THEN
        NeoStores stores = dbRule.getGraphDatabaseAPI().getDependencyResolver()
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        int headerSize = Standard.LATEST_RECORD_FORMATS.dynamic().getRecordHeaderSize();
        assertEquals( arrayBlockSize + headerSize, stores.getPropertyStore().getArrayStore().getRecordSize() );
        assertEquals( stringBlockSize + headerSize, stores.getPropertyStore().getStringStore().getRecordSize() );
    }

    @Test
    public void combineProvidedDbAndAdditionalConfig() throws Exception
    {
        // GIVEN
        int arrayBlockSize = 10;
        int stringBlockSize = 12;
        File dbConfig = file( "neo4j.properties" );
        File additionalConfig = file( "additional.properties" );
        store( stringMap(
                GraphDatabaseSettings.string_block_size.name(), String.valueOf( stringBlockSize ) ), dbConfig );
        store( stringMap(
                GraphDatabaseSettings.array_block_size.name(), String.valueOf( arrayBlockSize ) ), additionalConfig );
        List<String> nodeIds = nodeIds();

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--db-config", dbConfig.getAbsolutePath(),
                "--additional-config", additionalConfig.getAbsolutePath(),
                "--nodes", nodeData( true, Configuration.COMMAS, nodeIds, value -> true ).getAbsolutePath() );

        // THEN
        NeoStores stores = dbRule.getGraphDatabaseAPI().getDependencyResolver()
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        int headerSize = Standard.LATEST_RECORD_FORMATS.dynamic().getRecordHeaderSize();
        assertEquals( arrayBlockSize + headerSize, stores.getPropertyStore().getArrayStore().getRecordSize() );
        assertEquals( stringBlockSize + headerSize, stores.getPropertyStore().getStringStore().getRecordSize() );
    }

    @Test
    public void shouldPrintStackTraceOnInputExceptionIfToldTo() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN data file contains more columns than header file
        int extraColumns = 3;
        try
        {
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--nodes", nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                            nodeData( false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns )
                                    .getAbsolutePath(),
                    "--stacktrace" );

            fail( "Should have thrown exception" );
        }
        catch ( InputException e )
        {
            // THEN
            assertTrue( suppressOutput.getErrorVoice().containsMessage( e.getClass().getName() ) );
            assertTrue( e.getMessage().contains( "Extra column not present in header on line" ) );
        }
    }

    @Test
    public void shouldDisableLegacyStyleQuotingIfToldTo() throws Exception
    {
        // GIVEN
        String nodeId = "me";
        String labelName = "Alive";
        List<String> lines = new ArrayList<>();
        lines.add( ":ID,name,:LABEL" );
        lines.add( nodeId + "," + "\"abc\"\"def\\\"\"ghi\"" + "," + labelName );

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", data( lines.toArray( new String[lines.size()] ) ).getAbsolutePath(),
                "--legacy-style-quoting", "false",
                "--stacktrace" );

        // THEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            assertNotNull( db.findNode( label( labelName ), "name", "abc\"def\\\"ghi" ) );
        }
    }

    @Test
    public void shouldRespectBufferSizeSetting() throws Exception
    {
        // GIVEN
        List<String> lines = new ArrayList<>();
        lines.add( ":ID,name,:LABEL" );
        lines.add( "id," + repeat( 'l', 2_000 ) + ",Person" );

        // WHEN
        try
        {
            importTool(
                    "--into", dbRule.getStoreDirAbsolutePath(),
                    "--nodes", data( lines.toArray( new String[lines.size()] ) ).getAbsolutePath(),
                    "--read-buffer-size", "1k"
                    );
            fail( "Should've failed" );
        }
        catch ( IllegalStateException e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "input data" ) );
        }
    }

    @Test
    public void shouldRespectMaxMemoryPercentageSetting() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds( 10 );

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(),
                "--max-memory", "60%" );
    }

    @Test
    public void shouldFailOnInvalidMaxMemoryPercentageSetting() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds( 10 );

        try
        {
            // WHEN
            importTool( "--into", dbRule.getStoreDirAbsolutePath(), "--nodes",
                    nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(), "--max-memory", "110%" );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "percent" ) );
        }
    }

    @Test
    public void shouldRespectMaxMemorySuffixedSetting() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds( 10 );

        // WHEN
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(),
                "--max-memory", "100M" );
    }

    @Test
    public void shouldTreatRelationshipWithMissingStartOrEndIdOrTypeAsBadRelationship() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c" );
        Configuration config = Configuration.COMMAS;
        File nodeData = nodeData( true, config, nodeIds, TRUE );

        List<RelationshipDataLine> relationships = Arrays.asList(
                relationship( "a", null, "TYPE" ),
                relationship( null, "b", "TYPE" ),
                relationship( "a", "b", null ) );

        File relationshipData = relationshipData( true, config, relationships.iterator(), TRUE, true );
        File bad = badFile();

        // WHEN importing data where some relationships refer to missing nodes
        importTool(
                "--into", dbRule.getStoreDirAbsolutePath(),
                "--nodes", nodeData.getAbsolutePath(),
                "--bad", bad.getAbsolutePath(),
                "--skip-bad-relationships", "true",
                "--relationships", relationshipData.getAbsolutePath() );

        String badContents = FileUtils.readTextFile( bad, Charset.defaultCharset() );
        assertEquals( badContents, 3, occurencesOf( badContents, "is missing data" ) );
    }

    @Test
    public void shouldKeepStoreFilesAfterFailedImport() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN data file contains more columns than header file
        int extraColumns = 3;
        String storeDir = dbRule.getStoreDirAbsolutePath();
        try
        {
            importTool(
                    "--into", storeDir,
                    "--nodes", nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                            nodeData( false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns ).getAbsolutePath() );
            fail( "Should have thrown exception" );
        }
        catch ( InputException e )
        {
            // THEN the store files should be there
            for ( StoreType storeType : StoreType.values() )
            {
                if ( storeType.isRecordStore() )
                {
                    assertTrue( new File( storeDir, MetaDataStore.DEFAULT_NAME + storeType.getStoreName() ).exists() );
                }
            }

            List<String> errorLines = suppressOutput.getErrorVoice().lines();
            assertContains( errorLines, "Starting a database on these store files will likely fail or observe inconsistent records" );
        }
    }

    private void assertContains( List<String> errorLines, String string )
    {
        for ( String line : errorLines )
        {
            if ( line.contains( string ) )
            {
                return;
            }
        }
        fail( "Expected error lines " + join( errorLines.toArray( new String[errorLines.size()] ), format( "%n" ) ) +
                " to have at least one line containing the string '" + string + "'" );
    }

    private static int occurencesOf( String text, String lookFor )
    {
        int index = -1;
        int count = -1;
        do
        {
            count++;
            index = text.indexOf( lookFor, index + 1 );
        }
        while ( index != -1 );
        return count;
    }

    private File writeArrayCsv( String[] headers, String[] values ) throws FileNotFoundException
    {
        File data = file( fileName( "whitespace.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            writer.print( ":LABEL" );
            for ( String header : headers )
            {
                writer.print( "," + header );
            }
            // End line
            writer.println();

            // Save value as a String in name
            writer.print( "PERSON" );
            // For each type
            for ( String ignored : headers )
            {
                boolean comma = true;
                for ( String value : values )
                {
                    if ( comma )
                    {
                        writer.print( "," );
                        comma = false;
                    }
                    else
                    {
                        writer.print( ";" );
                    }
                    writer.print( value );
                }
            }
            // End line
            writer.println();
        }
        return data;
    }

    private String joinStringArray( String[] values )
    {
        return Arrays.stream( values ).map( String::trim ).collect( joining( ", ", "[", "]" ) );
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
        return node -> node.getProperty( "id", "" ).equals( id );
    }

    private void assertNodeHasLabels( Node node, String[] names )
    {
        for ( String name : names )
        {
            assertTrue( node + " didn't have label " + name + ", it had labels " + node.getLabels(),
                    node.hasLabel( label( name ) ) );
        }
    }

    private void verifyData()
    {
        verifyData( Validators.emptyValidator(), Validators.emptyValidator() );
    }

    private void verifyData(
            Validator<Node> nodeAdditionalValidation,
            Validator<Relationship> relationshipAdditionalValidation )
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            int nodeCount = 0;
            int relationshipCount = 0;
            for ( Node node : db.getAllNodes() )
            {
                assertTrue( node.hasProperty( "name" ) );
                nodeAdditionalValidation.validate( node );
                nodeCount++;
            }
            assertEquals( NODE_COUNT, nodeCount );
            for ( Relationship relationship : db.getAllRelationships() )
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
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
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
        return Iterators.singleOrNull( Iterators.filter(
                item -> item.getEndNode().equals( endNode ) &&
                        item.getProperty( "name" ).equals( relationship.name ),
                startNode.getRelationships( withName( relationship.type ) ).iterator() ) );
    }

    private Map<String,Node> allNodesById( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Map<String,Node> nodes = new HashMap<>();
            for ( Node node : db.getAllNodes() )
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
            IntPredicate linePredicate ) throws Exception
    {
        return nodeData( includeHeader, config, nodeIds, linePredicate, Charset.defaultCharset() );
    }

    private File nodeData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, Charset encoding ) throws Exception
    {
        return nodeData( includeHeader, config, nodeIds, linePredicate, encoding, 0 );
    }

    private File nodeData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, Charset encoding, int extraColumns ) throws Exception
    {
        File file = file( fileName( "nodes.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            if ( includeHeader )
            {
                writeNodeHeader( writer, config, null );
            }
            writeNodeData( writer, config, nodeIds, linePredicate, extraColumns );
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
            IntPredicate linePredicate, int extraColumns )
    {
        char delimiter = config.delimiter();
        char arrayDelimiter = config.arrayDelimiter();
        for ( int i = 0; i < nodeIds.size(); i++ )
        {
            if ( linePredicate.test( i ) )
            {
                writer.println( getLine( nodeIds.get( i ), delimiter, arrayDelimiter, extraColumns ) );
            }
        }
    }

    private String getLine( String nodeId, char delimiter, char arrayDelimiter, int extraColumns )
    {
        StringBuilder stringBuilder = new StringBuilder().append( nodeId ).append( delimiter ).append( randomName() )
                .append( delimiter ).append( randomLabels( arrayDelimiter ) );

        for ( int i = 0; i < extraColumns; i++ )
        {
            stringBuilder.append( delimiter ).append( "ExtraColumn" ).append( i );
        }

        return stringBuilder.toString();
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
            builder.append( labelName( random.nextInt( MAX_LABEL_ID ) ) );
        }
        return builder.toString();
    }

    private String labelName( int number )
    {
        return "LABEL_" + number;
    }

    private String randomName()
    {
        int length = random.nextInt( 10 ) + 5;
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < length; i++ )
        {
            builder.append( (char) ('a' + random.nextInt( 20 )) );
        }
        return builder.toString();
    }

    private File relationshipData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, boolean specifyType ) throws Exception
    {
        return relationshipData( includeHeader, config, nodeIds, linePredicate, specifyType, Charset.defaultCharset() );
    }

    private File relationshipData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, boolean specifyType, Charset encoding ) throws Exception
    {
        return relationshipData( includeHeader, config, randomRelationships( nodeIds ), linePredicate,
                specifyType, encoding );
    }

    private File relationshipData( boolean includeHeader, Configuration config,
            Iterator<RelationshipDataLine> data, IntPredicate linePredicate,
            boolean specifyType ) throws Exception
    {
        return relationshipData( includeHeader, config, data, linePredicate, specifyType, Charset.defaultCharset() );
    }

    private File relationshipData( boolean includeHeader, Configuration config,
            Iterator<RelationshipDataLine> data, IntPredicate linePredicate,
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

    private File badFile()
    {
        return new File( dbRule.getStoreDirFile(), BAD_FILE_NAME );
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
            Iterator<RelationshipDataLine> data, IntPredicate linePredicate, boolean specifyType )
    {
        char delimiter = config.delimiter();
        for ( int i = 0; i < RELATIONSHIP_COUNT; i++ )
        {
            if ( !data.hasNext() )
            {
                break;
            }
            RelationshipDataLine entry = data.next();
            if ( linePredicate.test( i ) )
            {
                writer.println( nullSafeString( entry.startNodeId ) +
                                delimiter + nullSafeString( entry.endNodeId ) +
                                (specifyType ? (delimiter + nullSafeString( entry.type )) : "") +
                                delimiter + currentTimeMillis() +
                                delimiter + (entry.name != null ? entry.name : "")
                );
            }
        }
    }

    private static String nullSafeString( String endNodeId )
    {
        return endNodeId != null ? endNodeId : "";
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

    static void assertExceptionContains( Exception e, String message, Class<? extends Exception> type )
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

    private IntPredicate lines( final int startingAt, final int endingAt /*excluded*/ )
    {
        return line -> line >= startingAt && line < endingAt;
    }

    static void importTool( String... arguments ) throws IOException
    {
        ImportTool.main( arguments, true );
    }
}
