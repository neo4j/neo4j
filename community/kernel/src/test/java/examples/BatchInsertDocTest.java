/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BatchInsertDocTest
{
    @Rule
    public DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private DefaultFileSystemAbstraction fileSystem;

    @Before
    public void before() throws Exception
    {
        fileSystem = fileSystemRule.get();
        fileSystem.mkdirs( new File( "target" ) );
        fileSystem.mkdirs( new File( "target/docs" ) );
    }

    @Test
    public void insert() throws Exception
    {
        // Make sure our scratch directory is clean
        File tempStoreDir = clean( "target/batchinserter-example" ).getAbsoluteFile();

        // START SNIPPET: insert
        BatchInserter inserter = null;
        try
        {
            inserter = BatchInserters.inserter( tempStoreDir );

            Label personLabel = Label.label( "Person" );
            inserter.createDeferredSchemaIndex( personLabel ).on( "name" ).create();

            Map<String, Object> properties = new HashMap<>();

            properties.put( "name", "Mattias" );
            long mattiasNode = inserter.createNode( properties, personLabel );

            properties.put( "name", "Chris" );
            long chrisNode = inserter.createNode( properties, personLabel );

            RelationshipType knows = RelationshipType.withName( "KNOWS" );
            inserter.createRelationship( mattiasNode, chrisNode, knows, null );
        }
        finally
        {
            if ( inserter != null )
            {
                inserter.shutdown();
            }
        }
        // END SNIPPET: insert

        // try it out from a normal db
        GraphDatabaseService db =
                new GraphDatabaseFactory()
                        .newEmbeddedDatabaseBuilder( new File("target/batchinserter-example") )
                        .newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
        }
        try ( Transaction tx = db.beginTx() )
        {
            Label personLabelForTesting = Label.label( "Person" );
            Node mNode = db.findNode( personLabelForTesting, "name", "Mattias" );
            Node cNode = mNode.getSingleRelationship( RelationshipType.withName( "KNOWS" ), Direction.OUTGOING ).getEndNode();
            assertThat( (String) cNode.getProperty( "name" ), is( "Chris" ) );
            assertThat( db.schema()
                    .getIndexes( personLabelForTesting )
                    .iterator()
                    .hasNext(), is( true ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void insertWithConfig() throws IOException
    {
        clean( "target/batchinserter-example-config" );

        // START SNIPPET: configuredInsert
        Map<String, String> config = new HashMap<>();
        config.put( "dbms.memory.pagecache.size", "512m" );
        BatchInserter inserter = BatchInserters.inserter(
                new File( "target/batchinserter-example-config" ).getAbsoluteFile(), config );
        // Insert data here ... and then shut down:
        inserter.shutdown();
        // END SNIPPET: configuredInsert
    }

    @Test
    public void insertWithConfigFile() throws IOException
    {
        clean( "target/docs/batchinserter-example-config" );
        try ( Writer fw = fileSystem.openAsWriter( new File( "target/docs/batchinsert-config" ).getAbsoluteFile(),
                StandardCharsets.UTF_8, false ) )
        {
            fw.append( "dbms.memory.pagecache.size=8m" );
        }

        // START SNIPPET: configFileInsert
        try ( FileReader input = new FileReader( new File( "target/docs/batchinsert-config" ).getAbsoluteFile() ) )
        {
            Map<String, String> config = MapUtil.load( input );
            BatchInserter inserter = BatchInserters.inserter(
                    new File( "target/docs/batchinserter-example-config" ), config );
            // Insert data here ... and then shut down:
            inserter.shutdown();
        }
        // END SNIPPET: configFileInsert
    }

    private File clean( String fileName ) throws IOException
    {
        File directory = new File( fileName );
        fileSystem.deleteRecursively( directory );
        return directory;
    }
}
