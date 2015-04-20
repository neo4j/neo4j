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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class NodeProxyTest
{
    public final
    @Rule
    DatabaseRule dbRule = new ImpermanentDatabaseRule();
    private final String PROPERTY_KEY = "PROPERTY_KEY";
    private GraphDatabaseService db;

    @Before
    public void init()
    {
        db = dbRule.getGraphDatabaseService();

    }

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
        assertThat( exceptionThrownBySecondDelete, instanceOf( IllegalStateException.class ) );

        try ( Transaction tx = db.beginTx() )
        {
            db.getNodeById( node.getId() ); // should throw NotFoundException
            tx.success();
        }
    }

    @Test( expected = IllegalStateException.class )
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
            node.delete(); // should throw IllegalStateException as this node is already deleted
            tx.success();
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
