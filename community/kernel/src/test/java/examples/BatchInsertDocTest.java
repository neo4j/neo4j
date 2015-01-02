/**
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
package examples;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;

public class BatchInsertDocTest
{
    @Test
    public void insert()
    {
        // START SNIPPET: insert
        BatchInserter inserter = BatchInserters.inserter( "target/batchinserter-example", fileSystem );
        Label personLabel = DynamicLabel.label( "Person" );
        inserter.createDeferredSchemaIndex( personLabel ).on( "name" ).create();
        Map<String, Object> properties = new HashMap<>();
        properties.put( "name", "Mattias" );
        long mattiasNode = inserter.createNode( properties, personLabel );
        properties.put( "name", "Chris" );
        long chrisNode = inserter.createNode( properties, personLabel );
        RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
        // To set properties on the relationship, use a properties map
        // instead of null as the last parameter.
        inserter.createRelationship( mattiasNode, chrisNode, knows, null );
        inserter.shutdown();
        // END SNIPPET: insert

        // try it out from a normal db
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase(
                "target/batchinserter-example" );
        try ( Transaction tx = db.beginTx() )
        {
            Node mNode = db.getNodeById( mattiasNode );
            Node cNode = mNode.getSingleRelationship( knows, Direction.OUTGOING ).getEndNode();
            assertThat( (String) cNode.getProperty( "name" ), is( "Chris" ) );
            assertThat( db.schema()
                    .getIndexes( personLabel )
                    .iterator()
                    .hasNext(), is( true ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void insertWithConfig()
    {
        // START SNIPPET: configuredInsert
        Map<String, String> config = new HashMap<>();
        config.put( "neostore.nodestore.db.mapped_memory", "90M" );
        BatchInserter inserter = BatchInserters.inserter(
                "target/batchinserter-example-config", fileSystem, config );
        // Insert data here ... and then shut down:
        inserter.shutdown();
        // END SNIPPET: configuredInsert
    }

    @Test
    public void insertWithConfigFile() throws IOException
    {
        try ( Writer fw = fileSystem.openAsWriter( new File( "target/batchinsert-config" ), "utf-8", false ) )
        {
            fw.append( "neostore.nodestore.db.mapped_memory=90M\n"
                       + "neostore.relationshipstore.db.mapped_memory=3G\n"
                       + "neostore.propertystore.db.mapped_memory=50M\n"
                       + "neostore.propertystore.db.strings.mapped_memory=100M\n"
                       + "neostore.propertystore.db.arrays.mapped_memory=0M" );
        }

        // START SNIPPET: configFileInsert
        try ( InputStream input = fileSystem.openAsInputStream( new File( "target/batchinsert-config" ) ) )
        {
            Map<String, String> config = MapUtil.load( input );
            BatchInserter inserter = BatchInserters.inserter(
                    "target/batchinserter-example-config", fileSystem, config );
            // Insert data here ... and then shut down:
            inserter.shutdown();
        }
        // END SNIPPET: configFileInsert
    }

    @Test
    public void batchDb()
    {
        // START SNIPPET: batchDb
        GraphDatabaseService batchDb =
                BatchInserters.batchDatabase( "target/batchdb-example", fileSystem );
        Label personLabel = DynamicLabel.label( "Person" );
        Node mattiasNode = batchDb.createNode( personLabel );
        mattiasNode.setProperty( "name", "Mattias" );
        Node chrisNode = batchDb.createNode();
        chrisNode.setProperty( "name", "Chris" );
        chrisNode.addLabel( personLabel );
        RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
        mattiasNode.createRelationshipTo( chrisNode, knows );
        // END SNIPPET: batchDb
        long mattiasNodeId = mattiasNode.getId();
        // START SNIPPET: batchDb
        batchDb.shutdown();
        // END SNIPPET: batchDb

        // try it out from a normal db
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase(
                "target/batchdb-example" );
        try ( Transaction tx = db.beginTx() )
        {
            Node mNode = db.getNodeById( mattiasNodeId );
            Node cNode = mNode.getSingleRelationship( knows, Direction.OUTGOING )
                    .getEndNode();
            assertThat( cNode, inTx( db, hasProperty( "name" ).withValue( "Chris" ) ) );
        }
        finally
        {
            db.shutdown();
        }
    }
    
    @Test
    public void batchDbWithConfig()
    {
        // START SNIPPET: configuredBatchDb
        Map<String, String> config = new HashMap<>();
        config.put( "neostore.nodestore.db.mapped_memory", "90M" );
        GraphDatabaseService batchDb =
                BatchInserters.batchDatabase( "target/batchdb-example-config", fileSystem, config );
        // Insert data here ... and then shut down:
        batchDb.shutdown();
        // END SNIPPET: configuredBatchDb
    }

    @Rule
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private EphemeralFileSystemAbstraction fileSystem;
    
    @Before
    public void before() throws Exception
    {
        fileSystem = fileSystemRule.get();
        fileSystem.mkdirs( new File( "target" ) );
    }
}
