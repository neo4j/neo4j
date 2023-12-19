/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.bolt;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CypherOverBoltIT
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Rule
    public Neo4jRule graphDb = new Neo4jRule().withConfig( ServerSettings.script_enabled, "true" );

    private URL url;
    private final int lineCountInCSV = 3; // needs to be >= 2

    @Before
    public void setUp() throws Exception
    {
        url = prepareTestImportFile( lineCountInCSV );
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWork()
    {

        for ( int i = lineCountInCSV - 1; i < lineCountInCSV + 1; i++ ) // test with different periodic commit sizes
        {
            try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() ); Session session = driver.session() )
            {
                StatementResult result = session.run( "USING PERIODIC COMMIT " + i + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                        "MERGE (currentnode:Label1 {uuid:row[0]})\n" + "RETURN currentnode;" );
                int countOfNodes = 0;
                while ( result.hasNext() )
                {
                    Node node = result.next().get( 0 ).asNode();
                    assertTrue( node.hasLabel( "Label1" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
                assertEquals( lineCountInCSV, countOfNodes );
                session.reset();
            }
        }
    }

    @Test
    public void explainingPeriodicCommitInOpenTransactionShouldNotFail()
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() ); Session session = driver.session() )
        {
            ResultSummary summary = session.readTransaction( tx ->
                                                             {
                                                                 StatementResult result =
                                                                         tx.run( "EXPLAIN USING PERIODIC COMMIT " +
                                                                                 "100 LOAD CSV FROM $file AS row CREATE (n:Row) SET n.row = row" );
                                                                 return result.summary();
                                                             } );
            assertTrue( summary.hasPlan() );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWork2()
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() ); Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV + 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label1 {uuid:row[0]})\n" + "RETURN currentnode;" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Node node = result.next().get( 0 ).asNode();
                assertTrue( node.hasLabel( "Label1" ) );
                assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                countOfNodes++;
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWork3()
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() ); Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + lineCountInCSV + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label1 {uuid:row[0]})\n" + "RETURN currentnode;" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Node node = result.next().get( 0 ).asNode();
                assertTrue( node.hasLabel( "Label1" ) );
                assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                countOfNodes++;
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWorkWithLists() throws Exception
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() ); Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV - 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label2 {uuid:row[0]})\n" + "RETURN [currentnode];" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Iterator<Object> iterator = result.next().get( 0 ).asList().iterator();
                while ( iterator.hasNext() )
                {
                    Node node = (Node) iterator.next();
                    assertTrue( node.hasLabel( "Label2" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWorkWithListsOfLists() throws Exception
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() ); Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV - 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label3 {uuid:row[0]})\n" + "RETURN [[currentnode]];" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Iterator<Object> iterator = result.next().get( 0 ).asList().iterator();  // iterator over outer list
                assertTrue( iterator.hasNext() );
                iterator = ((List<Object>) iterator.next()).iterator();  // iterator over inner list
                while ( iterator.hasNext() )
                {
                    Node node = (Node) iterator.next();
                    assertTrue( node.hasLabel( "Label3" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWorkWithMaps() throws Exception
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() ); Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV - 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label4 {uuid:row[0]})\n" + "RETURN {node:currentnode};" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Iterator<Map.Entry<String,Object>> iterator = result.next().get( 0 ).asMap().entrySet().iterator();
                while ( iterator.hasNext() )
                {
                    Map.Entry<String,Object> entry = iterator.next();
                    assertEquals( "node", entry.getKey() );
                    Node node = (Node) entry.getValue();
                    assertTrue( node.hasLabel( "Label4" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWorkWithMapsWithinMaps() throws Exception
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() ); Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV - 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label5 {uuid:row[0]})\n" + "RETURN {outer:{node:currentnode}};" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Iterator<Map.Entry<String,Object>> iterator = result.next().get( 0 ).asMap().entrySet().iterator();
                assertTrue( iterator.hasNext() );
                iterator = ((Map<String,Object>) iterator.next().getValue()).entrySet().iterator();
                while ( iterator.hasNext() )
                {
                    Map.Entry<String,Object> entry = iterator.next();
                    assertEquals( "node", entry.getKey() );
                    Node node = (Node) entry.getValue();
                    assertTrue( node.hasLabel( "Label5" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
            }

            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWorkWithMapsWithLists() throws Exception
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() ); Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV - 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label6 {uuid:row[0]})\n" + "RETURN {outer:[currentnode]};" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Iterator<Map.Entry<String,Object>> mapIterator = result.next().get( 0 ).asMap().entrySet().iterator();
                assertTrue( mapIterator.hasNext() );
                Iterator<Object> iterator = ((List<Object>) mapIterator.next().getValue()).iterator();
                while ( iterator.hasNext() )
                {
                    Node node = (Node) iterator.next();
                    assertTrue( node.hasLabel( "Label6" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    private Config configuration()
    {
        return Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig();
    }

    private URL prepareTestImportFile( int lines ) throws IOException
    {
        File tempFile = File.createTempFile( "testImport", ".csv" );
        try ( PrintWriter writer = FileUtils.newFilePrintWriter( tempFile, StandardCharsets.UTF_8 ) )
        {
            for ( int i = 0; i < lines; i++ )
            {
                writer.println( i + " " + i + " " + i );
            }
        }
        return tempFile.toURI().toURL();
    }
}
