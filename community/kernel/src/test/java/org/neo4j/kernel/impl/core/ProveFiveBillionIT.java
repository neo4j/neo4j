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
package org.neo4j.kernel.impl.core;

import java.io.File;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static java.lang.Math.pow;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.deleteFileOrDirectory;

@Ignore( "Requires a lot of disk space" )
public class ProveFiveBillionIT
{
    private static final String PATH = "target/var/5b";
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "CHAIN" );
    
    @Test
    public void proveIt() throws Exception
    {
        deleteFileOrDirectory( new File( PATH ) );
        BatchInserter inserter = BatchInserters.inserter( PATH/*, stringMap(
                "neostore.nodestore.db.mapped_memory", "300M",
                "neostore.relationshipstore.db.mapped_memory", "800M",
                "neostore.propertystore.db.mapped_memory", "100M",
                "neostore.propertystore.db.strings.mapped_memory", "100M" ) */);
        
        // Create one giant chain of nodes n1->n2->n3 where each node will have
        // an int property and each rel a long string property. This will yield
        // 5b nodes/relationships, 10b property records and 5b dynamic records.
        
        // Start off by creating the first 4 billion (or so) entities with the
        // batch inserter just to speed things up a little
        long first = inserter.createNode(map());
        int max = (int) pow( 2, 32 )-1000;
        Map<String, Object> nodeProperties = map( "number", 123 );
        Map<String, Object> relationshipProperties =
                map( "string", "A long string, which is longer than shortstring boundaries" );
        long i = 0;
        for ( ; i < max; i++ )
        {
            long second = inserter.createNode( nodeProperties );
            inserter.createRelationship( first, second, TYPE, relationshipProperties );
            if ( i > 0 && i % 1000000 == 0 ) System.out.println( (i/1000000) + "M" );
            first = second;
        }
        inserter.shutdown();
        System.out.println( "Switch to embedded" );
        
        // Then create the rest with embedded graph db.
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( PATH );
        Node firstNode = db.getNodeById( first );
        Transaction tx = db.beginTx();
        for ( ; i < 5000000000L; i++ )
        {
            Node secondNode = db.createNode();
            firstNode.createRelationshipTo( secondNode, TYPE );
            firstNode = secondNode;
            if ( i % 100000 == 0 )
            {
                tx.success();
                tx.close();
                System.out.println( (i/1000000) + "M" );
                tx = db.beginTx();
            }
        }
        
        // Here we have a huge db. Loop through it and count chain length.
/*        long count = 0;
        Node node = db.getReferenceNode();
        while ( true )
        {
            Relationship relationship = node.getSingleRelationship( TYPE, Direction.OUTGOING );
            if ( relationship == null )
            {
                break;
            }
        }
        System.out.println( count );
        assertTrue( count > 4900000000L );*/
        
        db.shutdown();
    }
}
