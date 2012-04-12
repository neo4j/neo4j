/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package examples;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class BatchInsertExampleTest
{
    @Test
    public void insert()
    {
        // START SNIPPET: insert
        BatchInserter inserter = BatchInserters.inserter( "target/batchinserter-example" );
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put( "name", "Mattias" );
        long mattiasNode = inserter.createNode( properties );
        properties.put( "name", "Chris" );
        long chrisNode = inserter.createNode( properties );
        RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
        // To set properties on the relationship, use a properties map
        // instead of null as the last parameter.
        inserter.createRelationship( mattiasNode, chrisNode, knows, null );
        inserter.shutdown();
        // END SNIPPET: insert

        // try it out from a normal db
        GraphDatabaseService db = new EmbeddedGraphDatabase(
                "target/batchinserter-example" );
        Node mNode = db.getNodeById( mattiasNode );
        Node cNode = mNode.getSingleRelationship( knows, Direction.OUTGOING )
                .getEndNode();
        assertEquals( "Chris", cNode.getProperty( "name" ) );
        db.shutdown();
    }

    @Test
    public void insertWithConfig()
    {
        // START SNIPPET: configuredInsert
        Map<String, String> config = new HashMap<String, String>();
        config.put( "neostore.nodestore.db.mapped_memory", "90M" );
        BatchInserter inserter = BatchInserters.inserter(
                "target/batchinserter-example-config", config );
        // Insert data here ... and then shut down:
        inserter.shutdown();
        // END SNIPPET: configuredInsert
    }

    @Test
    public void insertWithConfigFile() throws IOException
    {
        FileWriter fw = new FileWriter( "target/batchinsert-config" );
        fw.append( "neostore.nodestore.db.mapped_memory=90M\n"
                   + "neostore.relationshipstore.db.mapped_memory=3G\n"
                   + "neostore.propertystore.db.mapped_memory=50M\n"
                   + "neostore.propertystore.db.strings.mapped_memory=100M\n"
                   + "neostore.propertystore.db.arrays.mapped_memory=0M" );
        fw.close();

        // START SNIPPET: configFileInsert
        Map<String, String> config = MapUtil.load( new File(
                "target/batchinsert-config" ) );
        BatchInserter inserter = BatchInserters.inserter(
                "target/batchinserter-example-config", config );
        // Insert data here ... and then shut down:
        inserter.shutdown();
        // END SNIPPET: configFileInsert
    }

    @Test
    public void batchDb()
    {
        // START SNIPPET: batchDb
        GraphDatabaseService batchDb =
                BatchInserters.batchDatabase( "target/batchdb-example" );
        Node mattiasNode = batchDb.createNode();
        mattiasNode.setProperty( "name", "Mattias" );
        Node chrisNode = batchDb.createNode();
        chrisNode.setProperty( "name", "Chris" );
        RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
        mattiasNode.createRelationshipTo( chrisNode, knows );
        // END SNIPPET: batchDb
        long mattiasNodeId = mattiasNode.getId();
        // START SNIPPET: batchDb
        batchDb.shutdown();
        // END SNIPPET: batchDb

        // try it out from a normal db
        GraphDatabaseService db = new EmbeddedGraphDatabase(
                "target/batchinserter-example" );
        Node mNode = db.getNodeById( mattiasNodeId );
        Node cNode = mNode.getSingleRelationship( knows, Direction.OUTGOING )
                .getEndNode();
        assertEquals( "Chris", cNode.getProperty( "name" ) );
        db.shutdown();
    }
    
    @Test
    public void batchDbWithConfig()
    {
        // START SNIPPET: configuredBatchDb
        Map<String, String> config = new HashMap<String, String>();
        config.put( "neostore.nodestore.db.mapped_memory", "90M" );
        GraphDatabaseService batchDb =
                BatchInserters.batchDatabase( "target/batchdb-example-config", config );
        // Insert data here ... and then shut down:
        batchDb.shutdown();
        // END SNIPPET: configuredBatchDb
    }
}
