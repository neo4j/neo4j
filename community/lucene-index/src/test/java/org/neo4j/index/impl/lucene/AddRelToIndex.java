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
package org.neo4j.index.impl.lucene;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;

public class AddRelToIndex
{
    public static void main( String[] args )
    {
        String path = args[0];
        String indexName = "myIndex";
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
        Transaction tx = db.beginTx();
        Index<Relationship> index = db.index().forRelationships( indexName );
        Node node = db.createNode();
        Relationship relationship = db.createNode().createRelationshipTo( node, DynamicRelationshipType.withName( "KNOWS" ) );
        index.add( relationship, "key", "value" );
        tx.success();
        tx.finish();
        // Skip shutdown
    }
}
