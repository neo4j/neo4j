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
package org.neo4j.kernel.impl.api;

import org.junit.jupiter.api.Test;

import java.util.function.Function;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

@ImpermanentDbmsExtension
class DataAndSchemaTransactionSeparationIT
{
    @Inject
    private GraphDatabaseAPI db;

    private Function<Transaction, Void> expectFailureAfterSchemaOperation(
            final Function<Transaction, ?> function )
    {
        return transaction ->
        {
            // given
            transaction.schema().indexFor( label( "Label1" ) ).on( "key1" ).create();

            // when
            var exception = assertThrows( Exception.class, () -> function.apply( transaction ) );
            assertEquals( "Cannot perform data updates in a transaction that has performed schema updates.", exception.getMessage() );
            return null;
        };
    }

    private Function<Transaction, Void> succeedAfterSchemaOperation(
            final Function<Transaction, ?> function )
    {
        return transaction ->
        {
            // given
            transaction.schema().indexFor( label( "Label1" ) ).on( "key1" ).create();

            // when/then
            function.apply( transaction );
            return null;
        };
    }

    @Test
    void shouldNotAllowNodeCreationInSchemaTransaction()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            expectFailureAfterSchemaOperation( createNode() ).apply( transaction );
            transaction.commit();
        }
    }

    @Test
    void shouldNotAllowRelationshipCreationInSchemaTransaction()
    {
        // given
        Pair<Node,Node> nodes;
        try ( var transaction = db.beginTx() )
        {
            nodes = aPairOfNodes().apply( transaction );
            transaction.commit();
        }
        // then
        try ( var transaction = db.beginTx() )
        {
            expectFailureAfterSchemaOperation( relate( nodes ) ).apply( transaction );
        }
    }

    @Test
    @SuppressWarnings( "unchecked" )
    void shouldNotAllowPropertyWritesInSchemaTransaction()
    {
        // given
        Pair<Node,Node> nodes;
        try ( var transaction = db.beginTx() )
        {
            nodes = aPairOfNodes().apply( transaction );
            transaction.commit();
        }
        Relationship relationship;
        try ( var tx = db.beginTx() )
        {
            relationship = relate( nodes ).apply( tx );
            tx.commit();
        }
        // when
        for ( Function<Transaction, ?> operation : new Function[]{
                propertyWrite( Node.class, nodes.first(), "key1", "value1" ),
                propertyWrite( Relationship.class, relationship, "key1", "value1" ),
        } )
        {
            // then
            try ( var transaction = db.beginTx() )
            {
                expectFailureAfterSchemaOperation( operation ).apply( transaction );
            }
        }
    }

    @Test
    @SuppressWarnings( "unchecked" )
    void shouldAllowPropertyReadsInSchemaTransaction()
    {
        // given
        Pair<Node,Node> nodes;
        try ( var transaction = db.beginTx() )
        {
            nodes = aPairOfNodes().apply( transaction );
            transaction.commit();
        }
        Relationship relationship;
        try ( var tx = db.beginTx() )
        {
            relationship = relate( nodes ).apply( tx );
            tx.commit();
        }
        try ( var tx = db.beginTx() )
        {
            var node = tx.getNodeById( nodes.first().getId() );
            propertyWrite( Node.class, node, "key1", "value1" ).apply( tx );
            tx.commit();
        }
        try ( var tx = db.beginTx() )
        {
            propertyWrite( Relationship.class,
                    tx.getRelationshipById( relationship.getId() ), "key1", "value1" ).apply( tx );
            tx.commit();
        }

        // when
        for ( Function<Transaction, ?> operation : new Function[]{
                propertyRead( Node.class, nodes.first(), "key1" ),
                propertyRead( Relationship.class, relationship, "key1" ),
        } )
        {
            // then
            try ( var transaction = db.beginTx() )
            {
                succeedAfterSchemaOperation( operation ).apply( transaction );
            }
        }
    }

    private static Function<Transaction, Node> createNode()
    {
        return Transaction::createNode;
    }

    private static <T extends Entity> Function<Transaction, Object> propertyRead(
            Class<T> type, final T entity, final String key )
    {
        return new FailureRewrite<>( type.getSimpleName() + ".getProperty()" )
        {
            @Override
            Object perform( Transaction transaction )
            {
                if ( entity instanceof Node )
                {
                    return transaction.getNodeById( entity.getId() ).getProperty( key );
                }
                else
                {
                    return transaction.getRelationshipById( entity.getId() ).getProperty( key );
                }
            }
        };
    }

    private static <T extends Entity> Function<Transaction, Void> propertyWrite(
            Class<T> type, final T entity, final String key, final Object value )
    {
        return new FailureRewrite<>( type.getSimpleName() + ".setProperty()" )
        {
            @Override
            Void perform( Transaction transaction )
            {
                if ( entity instanceof Node )
                {
                    transaction.getNodeById( entity.getId() ).setProperty( key, value );
                }
                else
                {
                    transaction.getRelationshipById( entity.getId() ).setProperty( key, value );
                }
                return null;
            }
        };
    }

    private static Function<Transaction, Pair<Node, Node>> aPairOfNodes()
    {
        return tx -> Pair.of( tx.createNode(), tx.createNode() );
    }

    private static Function<Transaction, Relationship> relate( final Pair<Node, Node> nodes )
    {
        return tx -> tx.getNodeById( nodes.first().getId() ).createRelationshipTo( nodes.other(), withName( "RELATED" ) );
    }

    private abstract static class FailureRewrite<T> implements Function<Transaction, T>
    {
        private final String message;

        FailureRewrite( String message )
        {
            this.message = message;
        }

        @Override
        public T apply( Transaction transaction )
        {
            try
            {
                return perform( transaction );
            }
            catch ( AssertionError e )
            {
                throw new AssertionError( message + ": " + e.getMessage(), e );
            }
        }

        abstract T perform( Transaction transaction );
    }
}
