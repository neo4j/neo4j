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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BatchInsertDocTest
{
    @Test
    public void insert() throws InterruptedException
    {
        // START SNIPPET: insert
        BatchInserter inserter = BatchInserters.inserter( new File("target/batchinserter-example").getAbsolutePath(), fileSystem );
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
                new File("target/batchinserter-example").getAbsolutePath() );
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
        config.put( "dbms.pagecache.memory", "90M" );
        BatchInserter inserter = BatchInserters.inserter(
                new File("target/batchinserter-example-config").getAbsolutePath(), fileSystem, config );
        // Insert data here ... and then shut down:
        inserter.shutdown();
        // END SNIPPET: configuredInsert
    }

    @Test
    public void insertWithConfigFile() throws IOException
    {
        try ( Writer fw = fileSystem.openAsWriter( new File( "target/batchinsert-config" ).getAbsoluteFile(), "utf-8", false ) )
        {
            fw.append( "dbms.pagecache.memory=3G" );
        }

        // START SNIPPET: configFileInsert
        try ( InputStream input = fileSystem.openAsInputStream( new File( "target/batchinsert-config" ).getAbsoluteFile() ) )
        {
            Map<String, String> config = MapUtil.load( input );
            BatchInserter inserter = BatchInserters.inserter(
                    "target/batchinserter-example-config", fileSystem, config );
            // Insert data here ... and then shut down:
            inserter.shutdown();
        }
        // END SNIPPET: configFileInsert
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
