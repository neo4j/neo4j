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
package org.neo4j.kernel.impl.coreapi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.RelationshipType.withName;

@DbmsExtension
public class NodeAccessRequireTransactionIT
{
    @Inject
    private GraphDatabaseAPI databaseAPI;
    private InternalTransaction transaction;

    @BeforeEach
    void setUp()
    {
        transaction = (InternalTransaction) databaseAPI.beginTx();
    }

    @AfterEach
    void tearDown()
    {
        if ( transaction != null )
        {
            transaction.close();
        }
    }

    @Test
    void deleteNodeRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, node::delete );
    }

    @Test
    void relationshipsAccessRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, node::getRelationships );
    }

    @Test
    void hasRelationshipCheckRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, node::hasRelationship );
    }

    @Test
    void hasRelationshipByTypeCheckRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.hasRelationship( withName( "any" ) ) );
    }

    @Test
    void hasRelationshipByTypeAndDirectionRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.hasRelationship( OUTGOING, withName( "any" ) ));
    }

    @Test
    void relationshipsByTypeRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.getRelationships( withName( "any" ) ));
    }

    @Test
    void relationshipsByTypeAndDirectionRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.getRelationships( OUTGOING, withName( "any" ) ));
    }

    @Test
    void relationshipsByDirectionAccessRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.getRelationships( BOTH ) );
    }

    @Test
    void singleRelationshipRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.getSingleRelationship( withName( "any" ), BOTH ) );
    }

    @Test
    void createRelationshipRequireTransaction()
    {
        var node = detachedNode();
        try ( var tx = databaseAPI.beginTx() )
        {
            assertThrows( NotInTransactionException.class, () -> node.createRelationshipTo( tx.createNode(), withName( "any" ) ) );
        }
    }

    @Test
    void degreeByRelTypeRequiresTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.getDegree( withName( "any" ) ) );
    }

    @Test
    void degreeByDirectionRequiresTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.getDegree( OUTGOING ) );
    }

    @Test
    void degreeByDirectionAndRelTypeRequiresTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.getDegree( withName( "any" ), OUTGOING ) );
    }

    @Test
    void degreeRequiresTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, node::getDegree );
    }

    @Test
    void labelAddRequiresTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.addLabel( Label.label( "any" ) ) );
    }

    @Test
    void labelRemoveRequiresTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.removeLabel( Label.label( "any" ) ) );
    }

    @Test
    void labelCheckRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, () -> node.hasLabel( Label.label( "any" ) ) );
    }

    @Test
    void labelsAccessRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, node::getLabels );
    }

    @Test
    void relTypesAccessRequireTransaction()
    {
        var node = detachedNode();
        assertThrows( NotInTransactionException.class, node::getRelationshipTypes );
    }

    private Node detachedNode()
    {
        var node = transaction.createNode();
        transaction.close();
        return node;
    }
}
