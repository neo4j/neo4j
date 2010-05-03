/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.core;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestReadOnlyNeo4j extends AbstractNeo4jTestCase
{
    @Test
    public void testSimple()
    {
        GraphDatabaseService readGraphDb = new EmbeddedReadOnlyGraphDatabase( 
            getStorePath( "neo-test" ) );
        Transaction tx = readGraphDb.beginTx();
        int count = 0;
        for ( Node node : readGraphDb.getAllNodes() )
        {
            for ( Relationship rel : node.getRelationships() )
            {
                rel.getOtherNode( node );
                for ( String key : rel.getPropertyKeys() )
                {
                    rel.getProperty( key );
                }
            }
            for ( String key : node.getPropertyKeys() )
            {
                node.getProperty( key );
            }
            if ( count++ >= 10 )
            {
                break;
            }
        }
        tx.success();
        tx.finish();
        tx = readGraphDb.beginTx();
        try
        {
            readGraphDb.createNode();
        }
        catch ( ReadOnlyDbException e )
        {
            // good
        }
        tx.finish();
        readGraphDb.shutdown();
    }
    
    @Test
    public void testReadOnlyOperationsAndNoTransaction()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, 
                DynamicRelationshipType.withName( "TEST" ) );
        node1.setProperty( "key1", "value1" );
        rel.setProperty( "key1", "value1" );
        commit();
        
        // make sure write operations still throw exception
        try
        {
            getGraphDb().createNode();
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            node1.createRelationshipTo( node2, 
                    DynamicRelationshipType.withName( "TEST2" ) );
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            node1.setProperty( "key1", "value2" );
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }

        try
        {
            rel.removeProperty( "key1" );
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        
        // clear caches and try reads
        getEmbeddedGraphDb().getConfig().getGraphDbModule().
            getNodeManager().clearCache();
        
        assertEquals( node1, getGraphDb().getNodeById( node1.getId() ) );
        assertEquals( node2, getGraphDb().getNodeById( node2.getId() ) );
        assertEquals( rel, getGraphDb().getRelationshipById( rel.getId() ) );
        getEmbeddedGraphDb().getConfig().getGraphDbModule().
            getNodeManager().clearCache();
        
        assertEquals( "value1", node1.getProperty( "key1" ) );
        Relationship loadedRel = node1.getSingleRelationship( 
                DynamicRelationshipType.withName( "TEST" ), Direction.OUTGOING );
        assertEquals( rel, loadedRel );
        assertEquals( "value1", loadedRel.getProperty( "key1" ) );
    }
}
