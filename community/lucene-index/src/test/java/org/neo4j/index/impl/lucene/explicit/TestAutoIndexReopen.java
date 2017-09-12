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
package org.neo4j.index.impl.lucene.explicit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableRelationshipIndex;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class TestAutoIndexReopen
{

    private GraphDatabaseAPI graphDb;

    private long id1 = -1;
    private long id2 = -1;
    private long id3 = -1;

    @Before
    public void startDb()
    {
        graphDb = (GraphDatabaseAPI) new TestGraphDatabaseFactory().
                newImpermanentDatabaseBuilder().setConfig(new HashMap<>()).newGraphDatabase();

        try ( Transaction tx = graphDb.beginTx() )
        {
            // Create the node and relationship auto-indexes
            graphDb.index().getNodeAutoIndexer().setEnabled(true);
            graphDb.index().getNodeAutoIndexer().startAutoIndexingProperty(
                    "nodeProp");
            graphDb.index().getRelationshipAutoIndexer().setEnabled(true);
            graphDb.index().getRelationshipAutoIndexer().startAutoIndexingProperty(
                    "type");

            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node1 = graphDb.createNode();
            Node node2 = graphDb.createNode();
            Node node3 = graphDb.createNode();
            id1 = node1.getId();
            id2 = node2.getId();
            id3 = node3.getId();
            Relationship rel = node1.createRelationshipTo(node2,
                    RelationshipType.withName("FOO"));
            rel.setProperty("type", "FOO");

            tx.success();

        }
    }

    @After
    public void stopDb()
    {
        if ( graphDb != null )
        {
            graphDb.shutdown();
        }
        graphDb = null;
    }

    private ReadableRelationshipIndex relationShipAutoIndex()
    {
        return graphDb.index().getRelationshipAutoIndexer().getAutoIndex();
    }

    @Test
    public void testForceOpenIfChanged()
    {
        // do some actions to force the indexreader to be reopened
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node1 = graphDb.getNodeById( id1 );
            Node node2 = graphDb.getNodeById( id2 );
            Node node3 = graphDb.getNodeById( id3 );

            node1.setProperty("np2", "test property");

            node1.getRelationships( RelationshipType.withName( "FOO" ) ).forEach( Relationship::delete );

            // check first node
            Relationship rel;
            try ( IndexHits<Relationship> hits = relationShipAutoIndex().get( "type", "FOO", node1, node3 ) )
            {
                assertEquals( 0, hits.size() );
            }
            // create second relation ship
            rel = node1.createRelationshipTo( node3, RelationshipType.withName( "FOO" ) );
            rel.setProperty("type", "FOO");

            // check second node -> crashs with old FullTxData
            try ( IndexHits<Relationship> indexHits = relationShipAutoIndex().get( "type", "FOO", node1, node2 ) )
            {
                assertEquals( 0, indexHits.size() );
            }
            // create second relation ship
            rel = node1.createRelationshipTo(node2, RelationshipType.withName("FOO"));
            rel.setProperty("type", "FOO");
            try ( IndexHits<Relationship> relationships = relationShipAutoIndex().get( "type", "FOO", node1, node2 ) )
            {
                assertEquals( 1, relationships.size() );
            }

            tx.success();
        }
    }
}

