/*
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
package org.neo4j.kernel.impl.core;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class NodeProxyTest
{
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "TYPE" );
    private static final Label LABEL = DynamicLabel.label( "LABEL" );
    private static final String PROPERTY_KEY = "PROPERTY_KEY";

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldThrowHumaneExceptionsWhenPropertyDoesNotExistOnNode() throws Exception
    {
        // Given a database with PROPERTY_KEY in it
        createNodeWith( PROPERTY_KEY );

        // When trying to get property from node without it
        try ( Transaction ignored = db.beginTx() )
        {
            Node node = db.createNode();
            node.getProperty( PROPERTY_KEY );
            fail( "Expected exception to have been thrown" );
        }
        // Then
        catch ( NotFoundException exception )
        {
            assertThat( exception.getMessage(), containsString( PROPERTY_KEY ) );
        }
    }

    @Test
    public void shouldThrowHumaneExceptionsWhenPropertyDoesNotExist() throws Exception
    {
        // Given a database without PROPERTY_KEY in it

        // When
        try ( Transaction ignored = db.beginTx() )
        {
            Node node = db.createNode();
            node.getProperty( PROPERTY_KEY );
        }
        // Then
        catch ( NotFoundException exception )
        {
            assertThat( exception.getMessage(), containsString( PROPERTY_KEY ) );
        }
    }

    @Test( expected = NotFoundException.class )
    public void deletionOfSameNodeTwiceInOneTransactionShouldNotRollbackIt()
    {
        // Given
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            tx.success();
        }

        // When
        Exception exceptionThrownBySecondDelete = null;

        try ( Transaction tx = db.beginTx() )
        {
            node.delete();
            try
            {
                node.delete();
            }
            catch ( Exception e )
            {
                exceptionThrownBySecondDelete = e;
            }
            tx.success();
        }

        // Then
        assertThat( exceptionThrownBySecondDelete, instanceOf( NotFoundException.class ) );

        try ( Transaction tx = db.beginTx() )
        {
            db.getNodeById( node.getId() ); // should throw NotFoundException
            tx.success();
        }
    }

    @Test( expected = NotFoundException.class )
    public void deletionOfAlreadyDeletedNodeShouldThrow()
    {
        // Given
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            node.delete();
            tx.success();
        }

        // When
        try ( Transaction tx = db.beginTx() )
        {
            node.delete(); // should throw NotFoundException as this node is already deleted
            tx.success();
        }
    }

    @Test
    public void shouldThrowWhenSettingPropertyOnADeletedNode()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL ).setProperty( PROPERTY_KEY, "foo" );
            tx.success();
        }

        long nodeId = -1;
        try ( Transaction ignored = db.beginTx() )
        {
            // When
            Node node = db.findNode( LABEL, PROPERTY_KEY, "foo" );
            nodeId = node.getId();
            node.delete();
            node.setProperty( PROPERTY_KEY, "bar" );
            fail( "Setting property on a deleted node should not work" );
        }
        catch ( NotFoundException e )
        {
            // Then
            // exception is thrown - expected
        }
    }

    @Test
    public void shouldThrowWhenSettingPropertyOnANodeDeletedInSameTx()
    {
        long nodeId = -1;
        try ( Transaction ignored = db.beginTx() )
        {
            // Given
            db.createNode( LABEL ).setProperty( PROPERTY_KEY, "foo" );

            // When
            Node node = db.findNode( LABEL, PROPERTY_KEY, "foo" );
            nodeId = node.getId();
            node.delete();
            node.setProperty( PROPERTY_KEY, "bar" );
            fail( "Setting property on a deleted node should not work" );
        }
        catch ( NotFoundException e )
        {
            // Then
            // exception is thrown - expected
        }
    }

    @Test
    public void shouldThrowWhenAddingRelationshipToADeletedNode()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL ).setProperty( PROPERTY_KEY, "foo" );
            tx.success();
        }

        long nodeId = -1;
        try ( Transaction ignored = db.beginTx() )
        {
            // When
            Node node = db.findNode( LABEL, PROPERTY_KEY, "foo" );
            nodeId = node.getId();
            node.delete();
            node.createRelationshipTo( db.createNode(), TYPE );
            fail( "Adding relationship to a deleted node should not work" );
        }
        catch ( NotFoundException e )
        {
            // Then
            // exception is thrown - expected
        }
    }

    @Test
    public void shouldThrowWhenAddingRelationshipToANodeDeletedInSameTx()
    {
        long nodeId = -1;
        try ( Transaction ignored = db.beginTx() )
        {
            // Given
            db.createNode( LABEL ).setProperty( PROPERTY_KEY, "foo" );

            // When
            Node node = db.findNode( LABEL, PROPERTY_KEY, "foo" );
            nodeId = node.getId();
            node.delete();
            node.createRelationshipTo( db.createNode(), TYPE );
            fail( "Adding relationship to a deleted node should not work" );
        }
        catch ( NotFoundException e )
        {
            // Then
            // exception is thrown - expected
        }
    }

    private void createNodeWith( String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( key, 1 );
            tx.success();
        }
    }
}
