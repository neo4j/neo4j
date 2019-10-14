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

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.RelationshipType.withName;

@DbmsExtension
public class RelationshipAccessRequireTransactionIT
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
    void deleteRelationshipRequireTransaction()
    {
        var relationship = detachedRelationship();
        assertThrows( NotInTransactionException.class, relationship::delete );
    }

    @Test
    void startNodeAccessRequireTransaction()
    {
        var relationship = detachedRelationship();
        assertThrows( NotInTransactionException.class, relationship::getStartNode );
    }

    @Test
    void endNodeAccessRequireTransaction()
    {
        var relationship = detachedRelationship();
        assertThrows( NotInTransactionException.class, relationship::getEndNode );
    }

    @Test
    void otherNodeAccessRequireTransaction()
    {
        var relationship = detachedRelationship();
        assertThrows( NotInTransactionException.class, () -> relationship.getOtherNode( new NodeEntity( null, 7 ) ) );
    }

    @Test
    void nodesAccessRequireTransaction()
    {
        var relationship = detachedRelationship();
        assertThrows( NotInTransactionException.class, relationship::getNodes );
    }

    @Test
    void relationshipTypeAccessRequireTransaction()
    {
        var relationship = detachedRelationship();
        assertThrows( NotInTransactionException.class, relationship::getType );
    }

    @Test
    void relationshipTypeCheckAccessRequireTransaction()
    {
        var relationship = detachedRelationship();
        assertThrows( NotInTransactionException.class, () -> relationship.isType( withName( "any" ) ) );
    }

    @Test
    void startNodeIdAccessRequireTransaction()
    {
        var relationship = detachedRelationship();
        assertThrows( NotInTransactionException.class, relationship::getStartNodeId );
    }

    @Test
    void endNodeIdAccessRequireTransaction()
    {
        var relationship = detachedRelationship();
        assertThrows( NotInTransactionException.class, relationship::getEndNodeId );
    }

    private Relationship detachedRelationship()
    {
        var startNode = transaction.createNode();
        var endNode = transaction.createNode();
        var relationship = transaction.getRelationshipById( startNode.createRelationshipTo( endNode, withName( "type" ) ).getId() );
        transaction.close();
        return relationship;
    }
}
