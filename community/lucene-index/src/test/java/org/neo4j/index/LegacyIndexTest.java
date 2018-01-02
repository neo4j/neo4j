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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LegacyIndexTest
{
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "TYPE" );

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void removalOfNodeIndexDoesNotInfluenceRelationshipIndexWithSameName()
    {
        String indexName = "index";

        createNodeLegacyIndexWithSingleNode( db, indexName );
        createRelationshipLegacyIndexWithSingleRelationship( db, indexName );

        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = db.createNode().createRelationshipTo( db.createNode(), TYPE );
            Index<Relationship> relationshipIndex = db.index().forRelationships( indexName );
            relationshipIndex.add( relationship, "key", "otherValue" );

            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.delete();

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( db.index().existsForNodes( indexName ) );
            Index<Relationship> relationshipIndex = db.index().forRelationships( indexName );
            assertEquals( 2, sizeOf( relationshipIndex ) );
            tx.success();
        }
    }

    @Test
    public void removalOfRelationshipIndexDoesNotInfluenceNodeIndexWithSameName()
    {
        String indexName = "index";

        createNodeLegacyIndexWithSingleNode( db, indexName );
        createRelationshipLegacyIndexWithSingleRelationship( db, indexName );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.add( node, "key", "otherValue" );

            Index<Relationship> relationshipIndex = db.index().forRelationships( indexName );
            relationshipIndex.delete();

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( db.index().existsForRelationships( indexName ) );
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            assertEquals( 2, sizeOf( nodeIndex ) );
            tx.success();
        }
    }

    private static void createNodeLegacyIndexWithSingleNode( GraphDatabaseService db, String indexName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Index<Node> nodeIndex = db.index().forNodes( indexName );
            nodeIndex.add( node, "key", System.currentTimeMillis() );
            tx.success();
        }
    }

    private static void createRelationshipLegacyIndexWithSingleRelationship( GraphDatabaseService db, String indexName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = db.createNode().createRelationshipTo( db.createNode(), TYPE );
            Index<Relationship> relationshipIndexIndex = db.index().forRelationships( indexName );
            relationshipIndexIndex.add( relationship, "key", System.currentTimeMillis() );
            tx.success();
        }
    }

    private static int sizeOf( Index<?> index )
    {
        return index.query( "_id_:*" ).size();
    }
}
