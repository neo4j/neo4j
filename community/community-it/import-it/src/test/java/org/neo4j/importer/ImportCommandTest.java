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
package org.neo4j.importer;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import picocli.CommandLine;
import picocli.CommandLine.MissingParameterException;
import picocli.CommandLine.ParameterException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.common.Validator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.IllegalMultilineFieldException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.input.csv.Type;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.repeat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.ArrayUtil.join;
import static org.neo4j.internal.helpers.Exceptions.contains;
import static org.neo4j.internal.helpers.Exceptions.withMessage;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.internal.helpers.collection.MapUtil.store;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.fs.FileUtils.writeToFile;

@TestDirectoryExtension
@ExtendWith( { RandomExtension.class, SuppressOutputExtension.class} )
@ResourceLock( Resources.SYSTEM_OUT )
class ImportCommandTest
{
    private static final int MAX_LABEL_ID = 4;
    private static final int RELATIONSHIP_COUNT = 10_000;
    private static final int NODE_COUNT = 100;
    private static final IntPredicate TRUE = i -> true;

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private RandomRule random;
    @Inject
    private SuppressOutput suppressOutput;
    private DatabaseManagementService managementService;
    private int dataIndex;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldImportWithAsManyDefaultsAsAvailable() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, nodeIds, TRUE, true ).getAbsolutePath() );

        // THEN
        assertTrue( suppressOutput.getOutputVoice().containsMessage( "IMPORT DONE" ) );
        verifyData();
    }

    @Test
    void shouldImportWithHeadersBeingInSeparateFiles() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--delimiter", "TAB",
                "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                "--nodes",
                nodeHeader( config ).getAbsolutePath() + "," +
                nodeData( false, config, nodeIds, TRUE ).getAbsolutePath(),
                "--relationships",
                relationshipHeader( config ).getAbsolutePath() + "," +
                relationshipData( false, config, nodeIds, TRUE, true ).getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    void import4097Labels() throws Exception
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
            writer.println( "FIRST 4096|SECOND 4096|THIRD 4096" );
        }

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--delimiter", "TAB",
                "--array-delimiter", "|",
                "--nodes", header.getAbsolutePath() + "," + data.getAbsolutePath() );

        // THEN
        GraphDatabaseService databaseService = getDatabaseApi();
        try ( Transaction tx = databaseService.beginTx() )
        {
            long nodeCount = Iterables.count( databaseService.getAllNodes() );
            assertEquals( 4097, nodeCount );

            ResourceIterator<Node> nodes = databaseService.findNodes( label( "FIRST 4096" ) );
            assertEquals( 1, Iterators.asList( nodes ).size() );
            nodes = databaseService.findNodes( label( "SECOND 4096" ) );
            assertEquals( 1, Iterators.asList( nodes ).size() );
            tx.commit();
        }
    }

    @Test
    void shouldIgnoreWhitespaceAroundIntegers() throws Exception
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

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--quote", "'",
                "--nodes", data.getAbsolutePath() );

        // THEN
        int nodeCount = 0;
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try ( Transaction tx = databaseApi.beginTx() )
        {
            for ( Node node : databaseApi.getAllNodes() )
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

                    assertEquals( expected, node.getProperty( key ).toString(), "Wrong value for " + key );
                }
            }

            tx.commit();
        }

        assertEquals( values.size(), nodeCount );
    }

    @Test
    void shouldIgnoreWhitespaceAroundDecimalNumbers() throws Exception
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

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport( "--quote", "'",
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", data.getAbsolutePath() );

        // THEN
        int nodeCount = 0;
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try ( Transaction tx = databaseApi.beginTx() )
        {
            for ( Node node : databaseApi.getAllNodes() )
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

                    assertEquals( expected, Double.valueOf( node.getProperty( key ).toString() ), 0.0, "Wrong value for " + key );
                }
            }

            tx.commit();
        }

        assertEquals( values.size(), nodeCount );
    }

    @Test
    void shouldIgnoreWhitespaceAroundBooleans() throws Exception
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
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--quote", "'",
                "--nodes", data.getAbsolutePath() );

        // THEN
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try ( Transaction tx = databaseApi.beginTx() )
        {
            for ( Node node : databaseApi.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                if ( name.startsWith( "t" ) )
                {
                    assertTrue( (boolean) node.getProperty( "adult" ), "Wrong value on " + name );
                }
                else
                {
                    assertFalse( (boolean) node.getProperty( "adult" ), "Wrong value on " + name );
                }
            }

            long nodeCount = Iterables.count( databaseApi.getAllNodes() );
            assertEquals( 10, nodeCount );
            tx.commit();
        }
    }

    @Test
    void shouldIgnoreWhitespaceInAndAroundIntegerArrays() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values = new String[]{ "   17", "21", "99   ", "  34  ", "-34", "        -12", "-92 " };

        File data = writeArrayCsv(
                new String[]{ "s:short[]", "b:byte[]", "i:int[]", "l:long[]", "f:float[]", "d:double[]" }, values );
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--quote", "'",
                "--nodes", data.getAbsolutePath() );

        // THEN
        // Expected value for integer types
        String iExpected = joinStringArray( values );

        // Expected value for floating point types
        String fExpected = Arrays.stream( values ).map( String::trim )
                                                  .map( Double::valueOf )
                                                  .map( String::valueOf )
                                                  .collect( joining( ", ", "[", "]")  );

        int nodeCount = 0;
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try ( Transaction tx = databaseApi.beginTx() )
        {
            for ( Node node : databaseApi.getAllNodes() )
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

            tx.commit();
        }

        assertEquals( 1, nodeCount );
    }

    @Test
    void shouldIgnoreWhitespaceInAndAroundDecimalArrays() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values =
                new String[]{ "1.0", "   3.5", "45.153    ", "   925.12   ", "-2.121", "   -3.745", "-412.153    ",
                        "   -5.12   " };

        File data = writeArrayCsv( new String[]{ "f:float[]", "d:double[]" }, values );
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--quote", "'",
                "--nodes", data.getAbsolutePath() );

        // THEN
        String expected = joinStringArray( values );

        int nodeCount = 0;
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try ( Transaction tx = databaseApi.beginTx() )
        {
            for ( Node node : databaseApi.getAllNodes() )
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

            tx.commit();
        }

        assertEquals( 1, nodeCount );
    }

    @Test
    void shouldIgnoreWhitespaceInAndAroundBooleanArrays() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values =
                new String[]{ "true", "  true", "true   ", "  true  ", " false ", "false ", " false", "false ",
                        " false" };
        String expected = joinStringArray( values );

        File data = writeArrayCsv( new String[]{ "b:boolean[]" }, values );

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--quote", "'",
                "--nodes", data.getAbsolutePath() );

        // THEN
        int nodeCount = 0;
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try ( Transaction tx = databaseApi.beginTx() )
        {
            for ( Node node : databaseApi.getAllNodes() )
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

            tx.commit();
        }

        assertEquals( 1, nodeCount );
    }

    @Test
    void shouldFailIfHeaderHasLessColumnsThanData() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN data file contains more columns than header file
        int extraColumns = 3;
        try
        {
            runImport(
                    "--delimiter", "TAB",
                    "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                    "--nodes", nodeHeader( config ).getAbsolutePath() + "," +
                            nodeData( false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns )
                                    .getAbsolutePath(),
                    "--relationships", relationshipHeader( config ).getAbsolutePath() + "," +
                            relationshipData( false, config, nodeIds, TRUE, true ).getAbsolutePath() );

            fail( "Should have thrown exception" );
        }
        catch ( InputException e )
        {
            // THEN
            assertTrue( suppressOutput.getOutputVoice().containsMessage( "IMPORT FAILED" ) );
            assertFalse( suppressOutput.getErrorVoice().containsMessage( e.getClass().getName() ) );
            assertTrue( e.getMessage().contains( "Extra column not present in header on line" ) );
        }
    }

    @Test
    void shouldWarnIfHeaderHasLessColumnsThanDataWhenToldTo() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;
        File bad = badFile();

        // WHEN data file contains more columns than header file
        int extraColumns = 3;
        runImport(
                "--report-file", bad.getAbsolutePath(),
                "--bad-tolerance", Integer.toString( nodeIds.size() * extraColumns ),
                "--ignore-extra-columns",
                "--delimiter", "TAB",
                "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                "--nodes=" + nodeHeader( config ).getAbsolutePath() + "," +
                        nodeData( false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns )
                                .getAbsolutePath(),
                "--relationships", relationshipHeader( config ).getAbsolutePath() + "," +
                        relationshipData( false, config, nodeIds, TRUE, true ).getAbsolutePath() );

        // THEN
        String badContents = Files.readString( bad.toPath(), Charset.defaultCharset() );
        assertTrue( badContents.contains( "Extra column not present in header on line" ) );
    }

    @Test
    void shouldImportSplitInputFiles() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", // One group with one header file and one data file
                nodeHeader( config ).getAbsolutePath() + "," +
                nodeData( false, config, nodeIds, lines( 0, NODE_COUNT / 2 ) ).getAbsolutePath(),
                "--nodes", // One group with two data files, where the header sits in the first file
                nodeData( true, config, nodeIds,
                        lines( NODE_COUNT / 2, NODE_COUNT * 3 / 4 ) ).getAbsolutePath() + "," +
                nodeData( false, config, nodeIds, lines( NODE_COUNT * 3 / 4, NODE_COUNT ) ).getAbsolutePath(),
                "--relationships",
                relationshipHeader( config ).getAbsolutePath() + "," +
                relationshipData( false, config, nodeIds, TRUE, true ).getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    void shouldImportMultipleInputsWithAddedLabelsAndDefaultRelationshipType() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        final String[] firstLabels = {"AddedOne", "AddedTwo"};
        final String[] secondLabels = {"AddedThree"};
        final String firstType = "TYPE_1";
        final String secondType = "TYPE_2";

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes=" + join( firstLabels, ":" ) + "=" +
                nodeData( true, config, nodeIds, lines( 0, NODE_COUNT / 2 ) ).getAbsolutePath(),
                "--nodes=" + join( secondLabels, ":" ) + "=" +
                nodeData( true, config, nodeIds, lines( NODE_COUNT / 2, NODE_COUNT ) ).getAbsolutePath(),
                "--relationships=" + firstType + "=" +
                relationshipData( true, config, nodeIds, lines( 0, RELATIONSHIP_COUNT / 2 ), false ).getAbsolutePath(),
                "--relationships=" + secondType + "=" +
                relationshipData( true, config, nodeIds,
                        lines( RELATIONSHIP_COUNT / 2, RELATIONSHIP_COUNT ), false ).getAbsolutePath() );

        // THEN
        MutableInt numberOfNodesWithFirstSetOfLabels = new MutableInt();
        MutableInt numberOfNodesWithSecondSetOfLabels = new MutableInt();
        MutableInt numberOfRelationshipsWithFirstType = new MutableInt();
        MutableInt numberOfRelationshipsWithSecondType = new MutableInt();
        verifyData(
                node ->
                {
                    if ( nodeHasLabels( node, firstLabels ) )
                    {
                        numberOfNodesWithFirstSetOfLabels.increment();
                    }
                    else if ( nodeHasLabels( node, secondLabels ) )
                    {
                        numberOfNodesWithSecondSetOfLabels.increment();
                    }
                    else
                    {
                        fail( node + " has neither set of labels, it has " + labelsOf( node ) );
                    }
                },
                relationship ->
                {
                    if ( relationship.isType( RelationshipType.withName( firstType ) ) )
                    {
                        numberOfRelationshipsWithFirstType.increment();
                    }
                    else if ( relationship.isType( RelationshipType.withName( secondType ) ) )
                    {
                        numberOfRelationshipsWithSecondType.increment();
                    }
                    else
                    {
                        fail( relationship + " didn't have either type, it has " + relationship.getType().name() );
                    }
                } );
        assertEquals( NODE_COUNT / 2, numberOfNodesWithFirstSetOfLabels.intValue() );
        assertEquals( NODE_COUNT / 2, numberOfNodesWithSecondSetOfLabels.intValue() );
        assertEquals( RELATIONSHIP_COUNT / 2, numberOfRelationshipsWithFirstType.intValue() );
        assertEquals( RELATIONSHIP_COUNT / 2, numberOfRelationshipsWithSecondType.intValue() );
    }

    private static String labelsOf( Node node )
    {
        StringBuilder builder = new StringBuilder();
        for ( Label label : node.getLabels() )
        {
            builder.append( label.name() + " " );
        }
        return builder.toString();
    }

    private boolean nodeHasLabels( Node node, String[] labels )
    {
        for ( String name : labels )
        {
            if ( !node.hasLabel( Label.label( name ) ) )
            {
                return false;
            }
        }
        return true;
    }

    @Test
    void shouldImportOnlyNodes() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath() );
        // no relationships

        // THEN
        GraphDatabaseService db = getDatabaseApi();
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
            tx.commit();
        }
    }

    @Test
    void shouldImportGroupsOfOverlappingIds() throws Exception
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
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", nodeHeader( config, groupOne ) + "," +
                           nodeData( false, config, groupOneNodeIds, TRUE ),
                "--nodes", nodeHeader( config, groupTwo ) + "," +
                           nodeData( false, config, groupTwoNodeIds, TRUE ),
                "--relationships", relationshipHeader( config, groupOne, groupTwo, true ) + "," +
                                   relationshipData( false, config, rels.iterator(), TRUE, true ) );

        // THEN
        GraphDatabaseService db = getDatabaseApi();
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
            tx.commit();
        }
    }

    @Test
    void shouldBeAbleToMixSpecifiedAndUnspecifiedGroups() throws Exception
    {
        // GIVEN
        List<String> groupOneNodeIds = asList( "1", "2", "3" );
        List<String> groupTwoNodeIds = asList( "4", "5", "2" );
        Configuration config = Configuration.COMMAS;
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", nodeHeader( config, "MyGroup" ).getAbsolutePath() + "," +
                           nodeData( false, config, groupOneNodeIds, TRUE ).getAbsolutePath(),
                "--nodes", nodeHeader( config ).getAbsolutePath() + "," +
                           nodeData( false, config, groupTwoNodeIds, TRUE ).getAbsolutePath() );

        // THEN
        verifyData( 6, 0, Validators.emptyValidator(), Validators.emptyValidator() );
    }

    @Test
    void shouldImportWithoutTypeSpecifiedInRelationshipHeaderbutWithDefaultTypeInArgument() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        String type = randomType();

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                // there will be no :TYPE specified in the header of the relationships below
                "--relationships", type + "=" + relationshipData( true, config, nodeIds, TRUE, false ).getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    void shouldIncludeSourceInformationInNodeIdCollisionError() throws Exception
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
            runImport(
                    "--nodes", nodeHeaderFile.getAbsolutePath() + "," +
                               nodeData1.getAbsolutePath() + "," +
                               nodeData2.getAbsolutePath() );
            fail( "Should have failed with duplicate node IDs" );
        }
        catch ( Exception e )
        {
            // THEN
            assertExceptionContains( e, "'a' is defined more than once", DuplicateInputIdException.class );
        }
    }

    @Test
    void shouldSkipDuplicateNodesIfToldTo() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c", "d", "e", "f", "a", "g" );
        Configuration config = Configuration.COMMAS;
        File nodeHeaderFile = nodeHeader( config );
        File nodeData1 = nodeData( false, config, nodeIds, lines( 0, 4 ) );
        File nodeData2 = nodeData( false, config, nodeIds, lines( 4, nodeIds.size() ) );

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--skip-duplicate-nodes",
                "--nodes", nodeHeaderFile.getAbsolutePath() + "," +
                           nodeData1.getAbsolutePath() + "," +
                           nodeData2.getAbsolutePath() );

        // THEN there should not be duplicates of any node
        GraphDatabaseService db = getDatabaseApi();
        Set<String> expectedNodeIds = new HashSet<>( nodeIds );
        try ( Transaction tx = db.beginTx() )
        {
            Set<String> foundNodesIds = new HashSet<>();
            for ( Node node : db.getAllNodes() )
            {
                String id = (String) node.getProperty( "id" );
                assertTrue( foundNodesIds.add( id ), id + ", " + foundNodesIds );
                assertTrue( expectedNodeIds.contains( id ) );
            }
            assertEquals( expectedNodeIds, foundNodesIds );

            // also all nodes in the label index should exist
            for ( int i = 0; i < MAX_LABEL_ID; i++ )
            {
                Label label = label( labelName( i ) );
                try ( ResourceIterator<Node> nodesByLabel = db.findNodes( label ) )
                {
                    while ( nodesByLabel.hasNext() )
                    {
                        Node node = nodesByLabel.next();
                        if ( !node.hasLabel( label ) )
                        {
                            fail( "Expected " + node + " to have label " + label.name() + ", but instead had " +
                                    asList( node.getLabels() ) );
                        }
                    }
                }
            }

            tx.commit();
        }
    }

    @Test
    void shouldLogRelationshipsReferringToMissingNode() throws Exception
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
        File dbConfig = prepareDefaultConfigFile();

        // WHEN importing data where some relationships refer to missing nodes
        runImport(
                "--nodes", nodeData.getAbsolutePath(),
                "--report-file", bad.getAbsolutePath(),
                "--skip-bad-relationships",
                "--bad-tolerance", "2",
                "--additional-config", dbConfig.getAbsolutePath(),
                "--relationships", relationshipData1.getAbsolutePath() + "," +
                                   relationshipData2.getAbsolutePath() );

        // THEN
        String badContents = Files.readString( bad.toPath(), Charset.defaultCharset() );
        assertTrue( badContents.contains( "bogus" ), "Didn't contain first bad relationship" );
        assertTrue( badContents.contains( "missing" ), "Didn't contain second bad relationship" );
        verifyRelationships( relationships );
    }

    @Test
    void skipLoggingOfBadEntries() throws Exception
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

        File dbConfig = prepareDefaultConfigFile();

        // WHEN importing data where some relationships refer to missing nodes
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", nodeData.getAbsolutePath(),
                "--bad-tolerance", "2",
                "--skip-bad-entries-logging", "true",
                "--relationships", relationshipData1.getAbsolutePath() + "," +
                        relationshipData2.getAbsolutePath() );

        assertFalse( badFile().exists() );
        verifyRelationships( relationships );
    }

    @Test
    void shouldFailIfTooManyBadRelationships() throws Exception
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
        File relationshipData = relationshipData( true, config, relationships.iterator(), TRUE, true );
        File bad = badFile();

        // WHEN importing data where some relationships refer to missing nodes
        try
        {
            runImport(
                    "--nodes", nodeData.getAbsolutePath(),
                    "--report-file", bad.getAbsolutePath(),
                    "--bad-tolerance", "1",
                    "--relationships", relationshipData.getAbsolutePath() );
            fail();
        }
        catch ( Exception e )
        {
            // THEN
            assertExceptionContains( e, relationshipData.getAbsolutePath(), InputException.class );
        }
    }

    @Test
    void shouldBeAbleToDisableSkippingOfBadRelationships() throws Exception
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
            runImport(
                    "--nodes", nodeData.getAbsolutePath(),
                    "--report-file", bad.getAbsolutePath(),
                    "--skip-bad-relationships=false",
                    "--relationships", relationshipData1.getAbsolutePath() + "," +
                                       relationshipData2.getAbsolutePath() );
            fail();
        }
        catch ( Exception e )
        {
            // THEN
            assertExceptionContains( e, relationshipData1.getAbsolutePath(), InputException.class );
        }
    }

    @Test
    void shouldHandleAdditiveLabelsWithSpaces() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        final Label label1 = label( "My First Label" );
        final Label label2 = label( "My Other Label" );

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes=My First Label:My Other Label=" + nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, nodeIds, TRUE, true ).getAbsolutePath() );

        // THEN
        verifyData( node ->
        {
            assertTrue( node.hasLabel( label1 ) );
            assertTrue( node.hasLabel( label2 ) );
        }, Validators.emptyValidator() );
    }

    @Test
    void shouldImportFromInputDataEncodedWithSpecificCharset() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        Charset charset = StandardCharsets.UTF_16;

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--input-encoding", charset.name(),
                "--nodes", nodeData( true, config, nodeIds, TRUE, charset ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, nodeIds, TRUE, true, charset )
                        .getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    void shouldDisallowImportWithoutNodesInput() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        try
        {
            runImport(
                    "--relationships",
                    relationshipData( true, config, nodeIds, TRUE, true ).getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( MissingParameterException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "Missing required option '--nodes" ) );
        }
    }

    @Test
    void shouldBeAbleToImportAnonymousNodes() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "1", "", "", "", "3", "", "", "", "", "", "5" );
        Configuration config = Configuration.COMMAS;
        List<RelationshipDataLine> relationshipData = asList( relationship( "1", "3", "KNOWS" ) );
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, relationshipData.iterator(),
                        TRUE, true ).getAbsolutePath() );

        // THEN
        GraphDatabaseService db = getDatabaseApi();
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
            tx.commit();
        }
    }

    @Test
    void shouldDisallowMultilineFieldsByDefault() throws Exception
    {
        // GIVEN
        File data = data( ":ID,name", "1,\"This is a line with\nnewlines in\"" );

        // WHEN
        try
        {
            runImport(
                    "--nodes", data.getAbsolutePath() );
            fail();
        }
        catch ( Exception e )
        {
            // THEN
            assertExceptionContains( e, "Multi-line", IllegalMultilineFieldException.class );
        }
    }

    @Test
    void shouldNotTrimStringsByDefault() throws Exception
    {
        // GIVEN
        String name = "  This is a line with leading and trailing whitespaces   ";
        File data = data( ":ID,name", "1,\"" + name + "\"");
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", data.getAbsolutePath() );

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> allNodes = db.getAllNodes().iterator();
            Node node = Iterators.single( allNodes );
            allNodes.close();

            assertEquals( name, node.getProperty( "name" ) );

            tx.commit();
        }
    }

    @Test
    void shouldTrimStringsIfConfiguredTo() throws Exception
    {
        // GIVEN
        String name = "  This is a line with leading and trailing whitespaces   ";
        File data = data(
                ":ID,name",
                "1,\"" + name + "\"",
                "2," + name );

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--trim-strings", "true" );

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try ( Transaction tx = db.beginTx();
              ResourceIterator<Node> allNodes = db.getAllNodes().iterator() )
        {
            Set<String> names = new HashSet<>();
            while ( allNodes.hasNext() )
            {
                names.add( allNodes.next().getProperty( "name" ).toString() );
            }

            assertTrue( names.remove( name ) );
            assertTrue( names.remove( name.trim() ) );
            assertTrue( names.isEmpty() );

            tx.commit();
        }
    }

    @Test
    void shouldCollectUnlimitedNumberOfBadEntries() throws Exception
    {
        // GIVEN
        List<String> nodeIds = Collections.nCopies( 10_000, "A" );

        // WHEN
        runImport(
                "--nodes=" + nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(),
                "--skip-duplicate-nodes",
                "--bad-tolerance=-1" );

        // THEN
        // all those duplicates should just be accepted using the - for specifying bad tolerance
    }

    @Test
    void shouldAllowMultilineFieldsWhenEnabled() throws Exception
    {
        // GIVEN
        File data = data( ":ID,name", "1,\"This is a line with\nnewlines in\"" );
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--nodes", data.getAbsolutePath(),
                "--additional-config", dbConfig.getAbsolutePath(),
                "--multiline-fields", "true" );

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> allNodes = db.getAllNodes().iterator();
            Node node = Iterators.single( allNodes );
            allNodes.close();

            assertEquals( "This is a line with\nnewlines in", node.getProperty( "name" ) );

            tx.commit();
        }
    }

    @Test
    void shouldSkipEmptyFiles() throws Exception
    {
        // GIVEN
        File data = data( "" );

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", data.getAbsolutePath() );

        // THEN
        GraphDatabaseService graphDatabaseService = getDatabaseApi();
        try ( Transaction tx = graphDatabaseService.beginTx() )
        {
            ResourceIterator<Node> allNodes = graphDatabaseService.getAllNodes().iterator();
            assertFalse( allNodes.hasNext(), "Expected database to be empty" );
            tx.commit();
        }
    }

    @Test
    void shouldIgnoreEmptyQuotedStringsIfConfiguredTo() throws Exception
    {
        // GIVEN
        File data = data(
                ":ID,one,two,three",
                "1,\"\",,value" );
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--ignore-empty-strings", "true" );

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = single( db.getAllNodes() );
            assertFalse( node.hasProperty( "one" ) );
            assertFalse( node.hasProperty( "two" ) );
            assertEquals( "value", node.getProperty( "three" ) );
            tx.commit();
        }
    }

    @Test
    void shouldPrintUserFriendlyMessageAboutUnsupportedMultilineFields() throws Exception
    {
        // GIVEN
        File data = data(
                ":ID,name",
                "1,\"one\ntwo\nthree\"",
                "2,four" );

        try
        {
            runImport(
                    "--nodes", data.getAbsolutePath(),
                    "--multiline-fields=false" );
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
    void shouldAcceptRawAsciiCharacterCodeAsQuoteConfiguration() throws Exception
    {
        // GIVEN
        char weirdDelimiter = 1; // not '1', just the character represented with code 1, which seems to be SOH
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        File data = data(
                ":ID,name",
                "1," + name1,
                "2," + name2 );
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--quote", String.valueOf( weirdDelimiter ) );

        // THEN
        Set<String> names = asSet( "Weird", name2 );
        GraphDatabaseService db = getDatabaseApi();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                assertTrue( names.remove( name ), "Didn't expect node with name '" + name + "'" );
            }
            assertTrue( names.isEmpty() );
            tx.commit();
        }
    }

    @Test
    void shouldAcceptSpecialTabCharacterAsDelimiterConfiguration() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--delimiter", "\\t",
                "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                "--relationships", relationshipData( true, config, nodeIds, TRUE, true ).getAbsolutePath() );

        // THEN
        verifyData();
    }

    @Test
    void shouldReportBadDelimiterConfiguration() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN
        try
        {
            runImport(
                    "--delimiter", "\\bogus",
                    "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                    "--nodes", nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                    "--relationships", relationshipData( true, config, nodeIds, TRUE, true ).getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( ParameterException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "bogus" ) );
        }
    }

    @Test
    void shouldFailAndReportStartingLineForUnbalancedQuoteInMiddle() throws Exception
    {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        try
        {
            runImport(
                    "--nodes", nodeDataWithMissingQuote( 2 * unbalancedStartLine, unbalancedStartLine )
                            .getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( InputException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "Multi-line fields are illegal" ) );
        }
    }

    @Test
    void shouldAcceptRawEscapedAsciiCodeAsQuoteConfiguration() throws Exception
    {
        // GIVEN
        char weirdDelimiter = 1; // not '1', just the character represented with code 1, which seems to be SOH
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        File data = data(
                ":ID,name",
                "1," + name1,
                "2," + name2 );
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--quote", "\\1" );

        // THEN
        Set<String> names = asSet( "Weird", name2 );
        GraphDatabaseService db = getDatabaseApi();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                assertTrue( names.remove( name ), "Didn't expect node with name '" + name + "'" );
            }
            assertTrue( names.isEmpty() );
            tx.commit();
        }
    }

    @Test
    void shouldFailAndReportStartingLineForUnbalancedQuoteAtEnd() throws Exception
    {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        try
        {
            runImport(
                    "--nodes", nodeDataWithMissingQuote( unbalancedStartLine, unbalancedStartLine ).getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( InputException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( format( "Multi-line fields" ) ) );
        }
    }

    @Test
    void shouldBeEquivalentToUseRawAsciiOrCharacterAsQuoteConfiguration1() throws Exception
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
        File dbConfig = prepareDefaultConfigFile();

        // WHEN given as raw ascii
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--quote", weirdStringDelimiter );

        assertEquals( "~", "" + weirdDelimiter );
        // THEN
        assertEquals( "~".charAt( 0 ), weirdDelimiter );

        Set<String> names = asSet( "Weird", name2 );
        GraphDatabaseService db = getDatabaseApi();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                assertTrue( names.remove( name ), "Didn't expect node with name '" + name + "'" );
            }
            assertTrue( names.isEmpty() );
            tx.commit();
        }
    }

    @Test
    void shouldFailOnUnbalancedQuoteWithMultilinesEnabled() throws Exception
    {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        try
        {
            runImport(
                    "--multiline-fields", "true",
                    "--nodes",
                    nodeDataWithMissingQuote( 2 * unbalancedStartLine, unbalancedStartLine ).getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( InputException e )
        {   // THEN OK
        }
    }

    private File nodeDataWithMissingQuote( int totalLines, int unbalancedStartLine ) throws Exception
    {
        String[] lines = new String[totalLines + 1];

        lines[0] = "ID,:LABEL";

        for ( int i = 1; i <= totalLines; i++ )
        {
            StringBuilder line = new StringBuilder( format( "%d,", i ) );
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
    void shouldBeEquivalentToUseRawAsciiOrCharacterAsQuoteConfiguration2() throws Exception
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
        File dbConfig = prepareDefaultConfigFile();

        // WHEN given as string
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", data.getAbsolutePath(),
                "--quote", weirdStringDelimiter );

        // THEN
        assertEquals( weirdStringDelimiter, "" + weirdDelimiter );
        assertEquals( weirdStringDelimiter.charAt( 0 ), weirdDelimiter );

        Set<String> names = asSet( "Weird", name2 );
        GraphDatabaseService db = getDatabaseApi();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                String name = (String) node.getProperty( "name" );
                assertTrue( names.remove( name ), "Didn't expect node with name '" + name + "'" );
            }
            assertTrue( names.isEmpty() );
            tx.commit();
        }
    }

    @Test
    void useProvidedAdditionalConfig() throws Exception
    {
        // GIVEN
        int arrayBlockSize = 10;
        int stringBlockSize = 12;
        File dbConfig = file( "neo4j.properties" );
        store( stringMap(
                databases_root_path.name(), testDirectory.databaseLayout().getStoreLayout().storeDirectory().getAbsolutePath(),
                GraphDatabaseSettings.array_block_size.name(), String.valueOf( arrayBlockSize ),
                GraphDatabaseSettings.string_block_size.name(), String.valueOf( stringBlockSize ),
                transaction_logs_root_path.name(), getTransactionLogsRoot() ), dbConfig );
        List<String> nodeIds = nodeIds();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", nodeData( true, Configuration.COMMAS, nodeIds, value -> true ).getAbsolutePath() );

        // THEN
        NeoStores stores = getDatabaseApi().getDependencyResolver()
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        int headerSize = Standard.LATEST_RECORD_FORMATS.dynamic().getRecordHeaderSize();
        assertEquals( arrayBlockSize + headerSize, stores.getPropertyStore().getArrayStore().getRecordSize() );
        assertEquals( stringBlockSize + headerSize, stores.getPropertyStore().getStringStore().getRecordSize() );
    }

    @Test
    void shouldDisableLegacyStyleQuotingIfToldTo() throws Exception
    {
        // GIVEN
        String nodeId = "me";
        String labelName = "Alive";
        List<String> lines = new ArrayList<>();
        lines.add( ":ID,name,:LABEL" );
        lines.add( nodeId + "," + "\"abc\"\"def\\\"\"ghi\"" + "," + labelName );

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", data( lines.toArray( new String[lines.size()] ) ).getAbsolutePath(),
                "--legacy-style-quoting", "false");

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try ( Transaction tx = db.beginTx() )
        {
            assertNotNull( db.findNode( label( labelName ), "name", "abc\"def\\\"ghi" ) );
        }
    }

    @Test
    void shouldRespectBufferSizeSetting() throws Exception
    {
        // GIVEN
        List<String> lines = new ArrayList<>();
        lines.add( ":ID,name,:LABEL" );
        lines.add( "id," + repeat( 'l', 2_000 ) + ",Person" );

        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        try
        {
            runImport(
                    "--additional-config", dbConfig.getAbsolutePath(),
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
    void shouldRespectMaxMemoryPercentageSetting() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds( 10 );

        // WHEN
        runImport(
                "--nodes", nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(),
                "--max-memory", "60%" );
    }

    @Test
    void shouldFailOnInvalidMaxMemoryPercentageSetting() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds( 10 );

        try
        {
            // WHEN
            runImport( "--nodes",
                    nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(), "--max-memory", "110%" );
            fail( "Should have failed" );
        }
        catch ( ParameterException e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "percent" ) );
        }
    }

    @Test
    void shouldRespectMaxMemorySuffixedSetting() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds( 10 );

        // WHEN
        runImport(
                "--nodes", nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(),
                "--max-memory", "100M" );
    }

    @Test
    void shouldTreatRelationshipWithMissingStartOrEndIdOrTypeAsBadRelationship() throws Exception
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
        runImport(
                "--nodes", nodeData.getAbsolutePath(),
                "--report-file", bad.getAbsolutePath(),
                "--skip-bad-relationships", "true",
                "--relationships", relationshipData.getAbsolutePath() );

        String badContents = Files.readString( bad.toPath(), Charset.defaultCharset() );
        assertEquals( 3, occurencesOf( badContents, "is missing data" ), badContents );
    }

    @Test
    void shouldKeepStoreFilesAfterFailedImport() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        final var configFile = prepareDefaultConfigFile();
        // WHEN data file contains more columns than header file
        int extraColumns = 3;
        try
        {
            runImport(
                    "--additional-config=" + configFile.getAbsolutePath(),
                    "--nodes", nodeHeader( config ).getAbsolutePath() + "," +
                            nodeData( false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns ).getAbsolutePath() );
            fail( "Should have thrown exception" );
        }
        catch ( InputException e )
        {
            // THEN the store files should be there
            for ( StoreType storeType : StoreType.values() )
            {
                testDirectory.databaseLayout().file( storeType.getDatabaseFile() ).forEach( f -> assertTrue( f.exists() ) );
            }

            List<String> errorLines = suppressOutput.getErrorVoice().lines();
            assertContains( errorLines, "Starting a database on these store files will likely fail or observe inconsistent records" );
        }
    }

    @Test
    void shouldSupplyArgumentsAsFile() throws Exception
    {
        // given
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        File argumentFile = file( "args" );
        String nodesEscapedSpaces = nodeData( true, config, nodeIds, TRUE ).getAbsolutePath();
        String relationshipsEscapedSpaced = relationshipData( true, config, nodeIds, TRUE, true ).getAbsolutePath();
        File dbConfig = prepareDefaultConfigFile();
        String arguments = format(
                "--additional-config=%s%n" +
                "--nodes=%s%n" +
                "--relationships=%s%n",
                dbConfig.getAbsolutePath(),
                nodesEscapedSpaces, relationshipsEscapedSpaced );
        writeToFile( argumentFile, arguments, false );

        // when
        runImport( "@" + argumentFile.getAbsolutePath() );

        // then
        verifyData();
    }

    @Test
    void shouldCreateDebugLogInExpectedPlace() throws Exception
    {
        // The ImportTool is more embedded-db-focused where typically the debug.log ends up in in a `logs/debug.log` next to the db directory,
        // i.e. in <dbDir>/../logs/debug.log

        // given
        String dbDir = getDatabaseDirectory();
        runImport( "--nodes", nodeData( true, Configuration.COMMAS, nodeIds(), TRUE ).getAbsolutePath() );

        // THEN go and read the debug.log where it's expected to be and see if there's an IMPORT DONE line in it
        File dbDirParent = new File( dbDir ).getParentFile();
        File logsDir = new File( dbDirParent, logs_directory.defaultValue().toString() );
        File internalLogFile = new File( logsDir, Config.defaults().get( store_internal_log_path ).toFile().getName() );
        assertTrue( internalLogFile.exists() );
        List<String> lines = Files.readAllLines( internalLogFile.toPath() );
        assertTrue( lines.stream().anyMatch( line -> line.contains( "Import completed successfully" ) ) );
    }

    @Test
    void shouldNormalizeTypes() throws Exception
    {
        // GIVEN
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        File nodeData = createAndWriteFile( "nodes.csv", Charset.defaultCharset(), writer ->
        {
            writer.println( "id:ID,prop1:short,prop2:float" );
            writer.println( "1,123,456.789" );
            writer.println( "2,1000000,24850457689578965796.458348570" ); // <-- short too big, float too big
        } );
        File relationshipData = createAndWriteFile( "relationships.csv", Charset.defaultCharset(), writer ->
        {
            writer.println( ":START_ID,:END_ID,:TYPE,prop1:int,prop2:byte" );
            writer.println( "1,2,DC,123,12" );
            writer.println( "2,1,DC,9999999999,123456789" );
        } );
        runImport(
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes", nodeData.getAbsolutePath(),
                "--relationships", relationshipData.getAbsolutePath() );

        // THEN
        SuppressOutput.Voice out = suppressOutput.getOutputVoice();
        assertTrue( out.containsMessage( "IMPORT DONE" ) );
        assertTrue( out.containsMessage( format( "Property type of 'prop1' normalized from 'short' --> 'long' in %s", nodeData.getAbsolutePath() ) ) );
        assertTrue( out.containsMessage( format( "Property type of 'prop2' normalized from 'float' --> 'double' in %s", nodeData.getAbsolutePath() ) ) );
        assertTrue( out.containsMessage( format( "Property type of 'prop1' normalized from 'int' --> 'long' in %s", relationshipData.getAbsolutePath() ) ) );
        assertTrue( out.containsMessage( format( "Property type of 'prop2' normalized from 'byte' --> 'long' in %s", relationshipData.getAbsolutePath() ) ) );
        // The properties should have been normalized, let's verify that
        GraphDatabaseService db = getDatabaseApi();
        try ( Transaction tx = db.beginTx() )
        {
            Map<String,Node> nodes = new HashMap<>();
            db.getAllNodes().forEach( node -> nodes.put( node.getProperty( "id" ).toString(), node ) );
            Node node1 = nodes.get( "1" );
            assertEquals( 123L, node1.getProperty( "prop1" ) );
            assertEquals( 456.789D, node1.getProperty( "prop2" ) );
            Node node2 = nodes.get( "2" );
            assertEquals( 1000000L, node2.getProperty( "prop1" ) );
            assertEquals( 24850457689578965796.458348570D, node2.getProperty( "prop2" ) );

            Relationship relationship1 = single( node1.getRelationships( Direction.OUTGOING ) );
            assertEquals( 123L, relationship1.getProperty( "prop1" ) );
            assertEquals( 12L, relationship1.getProperty( "prop2" ) );
            Relationship relationship2 = single( node1.getRelationships( Direction.INCOMING ) );
            assertEquals( 9999999999L, relationship2.getProperty( "prop1" ) );
            assertEquals( 123456789L, relationship2.getProperty( "prop2" ) );

            tx.commit();
        }
    }

    @Test
    void shouldFailParsingOnTooLargeNumbersWithoutTypeNormalization() throws Exception
    {
        // GIVEN
        File dbConfig = prepareDefaultConfigFile();

        // WHEN
        File nodeData = createAndWriteFile( "nodes.csv", Charset.defaultCharset(), writer ->
        {
            writer.println( "id:ID,prop1:short,prop2:float" );
            writer.println( "1,1000000,24850457689578965796.458348570" ); // <-- short too big, float too big
        } );
        File relationshipData = createAndWriteFile( "relationships.csv", Charset.defaultCharset(), writer ->
        {
            writer.println( ":START_ID,:END_ID,:TYPE,prop1:int,prop2:byte" );
            writer.println( "1,1,DC,9999999999,123456789" );
        } );
        try
        {
            runImport(
                    "--additional-config", dbConfig.getAbsolutePath(),
                    "--normalize-types", "false",
                    "--nodes", nodeData.getAbsolutePath(),
                    "--relationships", relationshipData.getAbsolutePath() );
            fail();
        }
        catch ( InputException e )
        {
            String message = e.getMessage();
            assertThat( message, containsString( "1000000" ) );
            assertThat( message, containsString( "too big" ) );
        }
    }

    private static void assertContains( List<String> errorLines, String string )
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

    private void verifyData()
    {
        verifyData( Validators.emptyValidator(), Validators.emptyValidator() );
    }

    private void verifyData(
            Validator<Node> nodeAdditionalValidation,
            Validator<Relationship> relationshipAdditionalValidation )
    {
        verifyData( NODE_COUNT, RELATIONSHIP_COUNT, nodeAdditionalValidation, relationshipAdditionalValidation );
    }

    private void verifyData( int expectedNodeCount, int expectedRelationshipCount,
            Validator<Node> nodeAdditionalValidation,
            Validator<Relationship> relationshipAdditionalValidation )
    {
        GraphDatabaseService db = getDatabaseApi();
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
            assertEquals( expectedNodeCount, nodeCount );
            for ( Relationship relationship : db.getAllRelationships() )
            {
                assertTrue( relationship.hasProperty( "created" ) );
                relationshipAdditionalValidation.validate( relationship );
                relationshipCount++;
            }
            assertEquals( expectedRelationshipCount, relationshipCount );
            tx.commit();
        }
    }

    private void verifyRelationships( List<RelationshipDataLine> relationships )
    {
        GraphDatabaseService db = getDatabaseApi();
        Map<String,Node> nodesById = allNodesById( db );
        try ( Transaction tx = db.beginTx() )
        {
            for ( RelationshipDataLine relationship : relationships )
            {
                Node startNode = nodesById.get( relationship.startNodeId );
                Node endNode = nodesById.get( relationship.endNodeId );
                if ( startNode == null || endNode == null )
                {
                    // OK this is a relationship referring to a missing node, skip it
                    continue;
                }
                assertNotNull( findRelationship( startNode, endNode, relationship ), relationship.toString() );
            }
            tx.commit();
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
            tx.commit();
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
        return createAndWriteFile( "nodes.csv", encoding, writer ->
        {
            if ( includeHeader )
            {
                writeNodeHeader( writer, config, null );
            }
            writeNodeData( writer, config, nodeIds, linePredicate, extraColumns );
        } );
    }

    private File createAndWriteFile( String name, Charset encoding, Consumer<PrintStream> dataWriter ) throws Exception
    {
        File file = file( fileName( name ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            dataWriter.accept( writer );
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
        return createAndWriteFile( "nodes-header.csv", encoding, writer -> writeNodeHeader( writer, config, idGroup ) );
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
        return createAndWriteFile( "relationships.csv", encoding, writer ->
        {
            if ( includeHeader )
            {
                writeRelationshipHeader( writer, config, null, null, specifyType );
            }
            writeRelationshipData( writer, config, data, linePredicate, specifyType );
        } );
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
        return createAndWriteFile( "relationships-header.csv", encoding,
                writer -> writeRelationshipHeader( writer, config, startIdGroup, endIdGroup, specifyType ) );
    }

    private String fileName( String name )
    {
        return dataIndex++ + "-" + name;
    }

    private File file( String localname )
    {
        return testDirectory.databaseLayout().file( localname );
    }

    private File badFile()
    {
        return testDirectory.databaseLayout().file( CsvImporter.DEFAULT_REPORT_FILE_NAME );
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

    private String getTransactionLogsRoot()
    {
        return testDirectory.databaseLayout().getTransactionLogsDirectory().getParentFile().getAbsolutePath();
    }

    private File prepareDefaultConfigFile() throws IOException
    {
        File dbConfig = file( "neo4j.properties" );
        store( Map.of(
                transaction_logs_root_path.name(), getTransactionLogsRoot(),
                databases_root_path.name(), testDirectory.databaseLayout().getStoreLayout().storeDirectory().getAbsolutePath(),
                logs_directory.name(), testDirectory.storeDir().getAbsolutePath()
        ), dbConfig );
        return dbConfig;
    }

    private GraphDatabaseAPI getDatabaseApi()
    {
        if ( managementService == null )
        {
            managementService = new TestDatabaseManagementServiceBuilder().setDatabaseRootDirectory( testDirectory.storeDir() ).build();
        }
        return (GraphDatabaseAPI) managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    private String getDatabaseDirectory()
    {
        return testDirectory.databaseLayout().databaseDirectory().getAbsolutePath();
    }

    private void runImport( String... arguments )
    {
        runImport( testDirectory.storeDir().toPath().toAbsolutePath(), arguments );
    }

    private void runImport( Path homeDir, String... arguments )
    {
        final var ctx = new ExecutionContext( homeDir, homeDir.resolve( "conf" ), System.out, System.err, testDirectory.getFileSystem() );
        final var cmd = new ImportCommand( ctx );
        new CommandLine( cmd ).setUseSimplifiedAtFiles( true ).parseArgs( arguments );
        cmd.execute();
    }
}
