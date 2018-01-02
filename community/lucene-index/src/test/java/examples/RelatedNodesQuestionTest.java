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

import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * Trying out code from: http://stackoverflow.com/questions/5346011
 * 
 * @author Anders Nawroth
 */
public class RelatedNodesQuestionTest
{
    @Test
    public void question5346011()
    {
        GraphDatabaseService service = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = service.beginTx() )
        {
            RelationshipIndex index = service.index().forRelationships( "exact" );
            // ...creation of the nodes and relationship
            Node node1 = service.createNode();
            Node node2 = service.createNode();
            String a_uuid = "xyz";
            Relationship relationship = node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "related" ) );
            index.add( relationship, "uuid", a_uuid );
            // query
            IndexHits<Relationship> hits = index.get( "uuid", a_uuid, node1, node2 );
            assertEquals( 1, hits.size() );
            tx.success();
        }
        service.shutdown();
    }
}
