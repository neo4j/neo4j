/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.BatchTransaction;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.test.BatchTransaction.beginBatchTx;

@Ignore( "Not a test. Here for show-off purposes" )
public class CreateAndLoadDenseNodeIT
{
    @Test
    public void loadSpecificTypeDirectionRelationshipsFast() throws Exception
    {
        // GIVEN
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.getNodeById( 0 );
            tx.success();
        }
        // WHEN
        loadRelationships( node, MyRelTypes.TEST, INCOMING );
        loadRelationships( node, MyRelTypes.TEST2, OUTGOING );
        loadRelationships( node, MyRelTypes.TEST_TRAVERSAL, INCOMING );
    }

    private int loadRelationships( Node node, RelationshipType type, Direction direction )
    {
        int count;
        try ( Transaction tx = db.beginTx() )
        {
            count = count( node.getRelationships( type, direction ) );
            int pCount = node.getDegree( type, direction );
            assertEquals( count, pCount );
            tx.success();
        }
        return count;
    }

    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( getClass() );
    private GraphDatabaseService db;

    @Before
    public void before()
    {
        createDbIfNecessary();
        dbRule.setConfig( GraphDatabaseSettings.allow_store_upgrade, "true" );
        db = dbRule.getGraphDatabaseService();
    }

    private void createDbIfNecessary()
    {
        if ( !new File( dbRule.getStoreDir(), "neostore" ).exists() )
        {
            db = dbRule.getGraphDatabaseService();
            try ( BatchTransaction tx = beginBatchTx( db ) )
            {
                Node node = db.createNode();
                createRelationships( tx, node, MyRelTypes.TEST, INCOMING, 1 );
                createRelationships( tx, node, MyRelTypes.TEST, OUTGOING, 3_000_000 );
                createRelationships( tx, node, MyRelTypes.TEST2, OUTGOING, 5 );
                createRelationships( tx, node, MyRelTypes.TEST2, BOTH, 5 );
                createRelationships( tx, node, MyRelTypes.TEST_TRAVERSAL, OUTGOING, 2_000_000 );
                createRelationships( tx, node, MyRelTypes.TEST_TRAVERSAL, INCOMING, 2 );
            }
            finally
            {
                dbRule.stopAndKeepFiles();
            }
        }
    }

    private void createRelationships( BatchTransaction tx, Node node, RelationshipType type, Direction direction,
            int count )
    {
        for ( int i = 0; i < count; i++ )
        {
            Node firstNode = direction == OUTGOING || direction == BOTH ? node : db.createNode();
            Node otherNode = direction == INCOMING || direction == BOTH ? node : db.createNode();
            firstNode.createRelationshipTo( otherNode, type );
            tx.increment();
        }
    }
}
