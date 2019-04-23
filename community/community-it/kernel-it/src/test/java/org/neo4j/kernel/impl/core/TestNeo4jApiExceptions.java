/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class TestNeo4jApiExceptions
{
    private Transaction tx;
    private GraphDatabaseService graph;
    private DatabaseManagementService managementService;

    @BeforeEach
    void init()
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        graph = managementService.database( DEFAULT_DATABASE_NAME );
        newTransaction();
    }

    @AfterEach
    void cleanUp()
    {
        if ( graph != null )
        {
            rollback();
            managementService.shutdown();
        }
    }

    @Test
    void testNotInTransactionException()
    {
        Node node1 = graph.createNode();
        node1.setProperty( "test", 1 );
        Node node2 = graph.createNode();
        Node node3 = graph.createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( "test", 11 );
        commit();
        assertThrows( NotInTransactionException.class, () -> graph.createNode() );
        assertThrows( NotInTransactionException.class, () -> node1.createRelationshipTo( node2, MyRelTypes.TEST ) );
        assertThrows( NotInTransactionException.class, () -> node1.setProperty( "test", 2 ) );
        assertThrows( NotInTransactionException.class, () -> rel.setProperty( "test", 22 ) );
        assertThrows( NotInTransactionException.class, node3::delete );
        assertThrows( NotInTransactionException.class, rel::delete );

        newTransaction();
        assertEquals( node1.getProperty( "test" ), 1 );
        assertEquals( rel.getProperty( "test" ), 11 );
        assertEquals( rel, node1.getSingleRelationship( MyRelTypes.TEST, Direction.OUTGOING ) );
        node1.delete();
        node2.delete();
        rel.delete();
        node3.delete();

        // Finally
        rollback();
    }

    @Test
    void testNotFoundException()
    {
        Node node1 = graph.createNode();
        Node node2 = graph.createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        long nodeId = node1.getId();
        long relId = rel.getId();
        rel.delete();
        node2.delete();
        node1.delete();
        newTransaction();
        assertThrows( NotFoundException.class, () -> graph.getNodeById( nodeId ) );
        assertThrows( NotFoundException.class, () -> graph.getRelationshipById( relId ) );

        // Finally
        rollback();
    }

    @Test
    void shouldGiveNiceErrorWhenShutdownKernelApi()
    {
        GraphDatabaseService graphDb = graph;
        Node node = graphDb.createNode();
        commit();
        managementService.shutdown();

        assertThrows( NotInTransactionException.class, () -> node.getLabels().iterator() );
    }

    @Test
    void shouldGiveNiceErrorWhenShutdownLegacy()
    {
        GraphDatabaseService graphDb = graph;
        Node node = graphDb.createNode();
        commit();
        managementService.shutdown();

        assertThrows( NotInTransactionException.class, node::getRelationships );
        assertThrows( NotInTransactionException.class, graphDb::createNode );
    }

    private void newTransaction()
    {
        if ( tx != null )
        {
            tx.success();
            tx.close();
        }
        tx = graph.beginTx();
    }

    public void commit()
    {
        if ( tx != null )
        {
            tx.success();
            tx.close();
            tx = null;
        }
    }

    public void rollback()
    {
        if ( tx != null )
        {
            tx.close();
            tx = null;
        }
    }

}
