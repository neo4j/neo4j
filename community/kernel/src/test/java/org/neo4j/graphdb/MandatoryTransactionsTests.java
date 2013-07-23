/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.graphdb;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;

public class MandatoryTransactionsTests
{
    private GraphDatabaseService graphDatabaseService;

    @Before
    public void setUp() throws Exception
    {
        graphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @Test
    public void shouldRequireTransactionsForNodeGetProperty() throws Exception
    {
        Node node = createNodeWithProperty( graphDatabaseService );

        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            node.getProperty( "foo" );
        }
        finally
        {
            transaction.finish();
        }

        try
        {
            node.getProperty( "foo" );

            fail( "Transactions are mandatory, also for reads" );
        }
        catch ( NotInTransactionException e )
        {

        }
    }

    @Test
    public void shouldRequireTransactionsForNodeGetPropertyWithDefault() throws Exception
    {
        Node node = createNodeWithProperty( graphDatabaseService );

        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            node.getProperty( "foo", "baz" );
        }
        finally
        {
            transaction.finish();
        }

        try
        {
            node.getProperty( "foo", "baz" );

            fail( "Transactions are mandatory, also for reads" );
        }
        catch ( NotInTransactionException e )
        {

        }
    }

    @Test
    public void shouldRequireTransactionsForNodeGetPropertyKeys() throws Exception
    {
        Node node = createNodeWithProperty( graphDatabaseService );

        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            node.getPropertyKeys();
        }
        finally
        {
            transaction.finish();
        }

        try
        {
            node.getPropertyKeys();

            fail( "Transactions are mandatory, also for reads" );
        }
        catch ( NotInTransactionException e )
        {

        }
    }

    @Test
    public void shouldRequireTransactionsForNodeHasProperty() throws Exception
    {
        Node node = createNodeWithProperty( graphDatabaseService );

        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            node.hasProperty( "foo" );
        }
        finally
        {
            transaction.finish();
        }

        try
        {
            node.hasProperty( "foo" );

            fail( "Transactions are mandatory, also for reads" );
        }
        catch ( NotInTransactionException e )
        {

        }
    }

    @Test
    public void shouldRequireTransactionsForNodeGetPropertyValues() throws Exception
    {
        Node node = createNodeWithProperty( graphDatabaseService );

        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            for ( Object o : node.getPropertyValues() )
            {

            }
        }
        finally
        {
            transaction.finish();
        }

        try
        {
            for ( Object o : node.getPropertyValues() )
            {

            }

            fail( "Transactions are mandatory, also for reads" );
        }
        catch ( NotInTransactionException e )
        {

        }
    }

    @Test
    public void shouldRequireTransactionsForNodeGetLabels() throws Exception
    {
        Node node = createNodeWithLabel( graphDatabaseService );

        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            for ( Label label : node.getLabels() )
            {

            }
        }
        finally
        {
            transaction.finish();
        }

        try
        {
            for ( Label label : node.getLabels() )
            {

            }

            fail( "Transactions are mandatory, also for reads" );
        }
        catch ( NotInTransactionException e )
        {

        }
    }

    @Test
    public void shouldRequireTransactionsForNodeHasLabel() throws Exception
    {
        Node node = createNodeWithLabel( graphDatabaseService );

        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            node.hasLabel( label( "foo" ) );
        }
        finally
        {
            transaction.finish();
        }

        try
        {
            node.hasLabel( label( "foo" ) );

            fail( "Transactions are mandatory, also for reads" );
        }
        catch ( NotInTransactionException e )
        {

        }
    }

    @Test
    public void shouldRequireTransactionsForNodeGetRelationships() throws Exception
    {
        Node node = createNodeWithRelationship( graphDatabaseService );

        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            for ( Relationship relationship : node.getRelationships() )
            {

            }
        }
        finally
        {
            transaction.finish();
        }

        try
        {
            for ( Relationship relationship : node.getRelationships() )
            {

            }

            fail( "Transactions are mandatory, also for reads" );
        }
        catch ( NotInTransactionException e )
        {

        }
    }

    @Test
    @Ignore
    public void shouldCoverEntireInterfaceToDriveOutAssertionPoints() throws Exception
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    private Node createNodeWithLabel( GraphDatabaseService graphDatabaseService )
    {
        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            Node node = graphDatabaseService.createNode( label( "foo" ) );
            node.setProperty( "bar", "baz" );
            transaction.success();
            return node;
        }
        finally
        {
            transaction.finish();
        }
    }

    private Node createNodeWithProperty( GraphDatabaseService graphDatabaseService )
    {
        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            Node node = graphDatabaseService.createNode();
            node.setProperty( "foo", "bar" );
            transaction.success();
            return node;
        }
        finally
        {
            transaction.finish();
        }
    }

    private Node createNodeWithRelationship( GraphDatabaseService graphDatabaseService )
    {
        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            Node node1 = graphDatabaseService.createNode();
            Node node2 = graphDatabaseService.createNode();
            Relationship relationship = node1.createRelationshipTo( node2, DynamicRelationshipType.withName(
                    "THE_REL" ) );
            transaction.success();
            return node1;
        }
        finally
        {
            transaction.finish();
        }
    }
}
