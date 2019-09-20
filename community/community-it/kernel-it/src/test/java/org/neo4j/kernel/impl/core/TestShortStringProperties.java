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

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

@ImpermanentDbmsExtension
class TestShortStringProperties
{
    private static final String LONG_STRING = "this is a really long string, believe me!";

    @Inject
    private GraphDatabaseService graphdb;

    @Test
    void canAddMultipleShortStringsToTheSameNode()
    {
        Node node;
        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = transaction.createNode();
            node.setProperty( "key", "value" );
            node.setProperty( "reverse", "esrever" );
            transaction.commit();
        }
        assertThat( node, inTx( graphdb, hasProperty( "key" ).withValue( "value" )  ) );
        assertThat( node, inTx( graphdb, hasProperty( "reverse" ).withValue( "esrever" )  ) );
    }

    @Test
    void canAddShortStringToRelationship()
    {
        Relationship rel;
        try ( Transaction transaction = graphdb.beginTx() )
        {
            rel = transaction.createNode().createRelationshipTo( transaction.createNode(), withName( "REL_TYPE" ) );
            rel.setProperty( "type", "dimsedut" );
            transaction.commit();
        }
        assertThat( rel, inTx( graphdb, hasProperty( "type" ).withValue( "dimsedut" ) ) );
    }

    @Test
    void canUpdateShortStringInplace()
    {
        Node node;
        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = transaction.createNode();
            node.setProperty( "key", "value" );
            transaction.commit();
        }

        try ( Transaction transaction = graphdb.beginTx() )
        {
            var n = transaction.getNodeById( node.getId() );
            assertEquals( "value", n.getProperty( "key" ) );
            n.setProperty( "key", "other" );
            transaction.commit();
        }

        assertThat( node, inTx( graphdb, hasProperty( "key" ).withValue( "other" )  ) );
    }

    @Test
    void canReplaceLongStringWithShortString()
    {
        Node node;
        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = transaction.createNode();
            node.setProperty( "key", LONG_STRING );
            transaction.commit();
        }

        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = transaction.getNodeById( node.getId() );
            assertEquals( LONG_STRING, node.getProperty( "key" ) );
            node.setProperty( "key", "value" );
            transaction.commit();
        }

        assertThat( node, inTx( graphdb, hasProperty( "key" ).withValue( "value" )  ) );
    }

    @Test
    void canReplaceShortStringWithLongString()
    {
        Node node;
        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = transaction.createNode();
            node.setProperty( "key", "value" );
            transaction.commit();
        }

        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = transaction.getNodeById( node.getId() );
            assertEquals( "value", node.getProperty( "key" ) );
            node.setProperty( "key", LONG_STRING );
            transaction.commit();
        }

        assertThat( node, inTx( graphdb, hasProperty( "key" ).withValue( LONG_STRING )  ) );
    }

    @Test
    void canRemoveShortStringProperty()
    {
        Node node;
        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = transaction.createNode();
            node.setProperty( "key", "value" );
            transaction.commit();
        }

        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = transaction.getNodeById( node.getId() );
            assertEquals( "value", node.getProperty( "key" ) );

            node.removeProperty( "key" );
            transaction.commit();
        }

        try ( Transaction transaction = graphdb.beginTx() )
        {
            assertThat( transaction.getNodeById( node.getId() ), not( hasProperty( "key" ) ) );
        }
    }
}
