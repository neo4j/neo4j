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
package org.neo4j.kernel.impl.api;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Pair;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class DataAndSchemaTransactionSeparationIT
{
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule();

    private static Function<GraphDatabaseService, Void> expectFailureAfterSchemaOperation(
            final Function<GraphDatabaseService, ?> function )
    {
        return new Function<GraphDatabaseService, Void>()
        {
            @Override
            public Void apply( GraphDatabaseService graphDb )
            {
                // given
                graphDb.schema().indexFor( label( "Label1" ) ).on( "key1" ).create();

                // when
                try
                {
                    function.apply( graphDb );

                    fail( "expected exception" );
                }
                // then
                catch ( Exception e )
                {
                    assertEquals( "Cannot perform data updates in a transaction that has performed schema updates.",
                            e.getMessage() );
                }
                return null;
            }
        };
    }

    private static Function<GraphDatabaseService, Void> succeedAfterSchemaOperation(
            final Function<GraphDatabaseService, ?> function )
    {
        return new Function<GraphDatabaseService, Void>()
        {
            @Override
            public Void apply( GraphDatabaseService graphDb )
            {
                // given
                graphDb.schema().indexFor( label( "Label1" ) ).on( "key1" ).create();

                // when/then
                function.apply( graphDb );
                return null;
            }
        };
    }

    @Test
    public void shouldNotAllowNodeCreationInSchemaTransaction() throws Exception
    {
        db.executeAndRollback( expectFailureAfterSchemaOperation( createNode() ) );
    }

    @Test
    public void shouldNotAllowRelationshipCreationInSchemaTransaction() throws Exception
    {
        // given
        final Pair<Node, Node> nodes = db.executeAndCommit( aPairOfNodes() );
        // then
        db.executeAndRollback( expectFailureAfterSchemaOperation( relate( nodes ) ) );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotAllowPropertyWritesInSchemaTransaction() throws Exception
    {
        // given
        Pair<Node, Node> nodes = db.executeAndCommit( aPairOfNodes() );
        Relationship relationship = db.executeAndCommit( relate( nodes ) );
        // when
        for ( Function<GraphDatabaseService, ?> operation : new Function[]{
                propertyWrite( Node.class, nodes.first(), "key1", "value1" ),
                propertyWrite( Relationship.class, relationship, "key1", "value1" ),
        } )
        {
            // then
            db.executeAndRollback( expectFailureAfterSchemaOperation( operation ) );
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldAllowPropertyReadsInSchemaTransaction() throws Exception
    {
        // given
        Pair<Node, Node> nodes = db.executeAndCommit( aPairOfNodes() );
        Relationship relationship = db.executeAndCommit( relate( nodes ) );
        db.executeAndCommit( propertyWrite( Node.class, nodes.first(), "key1", "value1" ) );
        db.executeAndCommit( propertyWrite( Relationship.class, relationship, "key1", "value1" ) );

        // when
        for ( Function<GraphDatabaseService, ?> operation : new Function[]{
                propertyRead( Node.class, nodes.first(), "key1" ),
                propertyRead( Relationship.class, relationship, "key1" ),
        } )
        {
            // then
            db.executeAndRollback( succeedAfterSchemaOperation( operation ) );
        }
    }

    private static Function<GraphDatabaseService, Node> createNode()
    {
        return new Function<GraphDatabaseService, Node>()
        {
            @Override
            public Node apply( GraphDatabaseService graphDb )
            {
                return graphDb.createNode();
            }
        };
    }

    private static <T extends PropertyContainer> Function<GraphDatabaseService, Object> propertyRead(
            Class<T> type, final T entity, final String key )
    {
        return new FailureRewrite<Object>( type.getSimpleName() + ".getProperty()" )
        {
            @Override
            Object perform( GraphDatabaseService graphDb )
            {
                return entity.getProperty( key );
            }
        };
    }

    private static <T extends PropertyContainer> Function<GraphDatabaseService, Void> propertyWrite(
            Class<T> type, final T entity, final String key, final Object value )
    {
        return new FailureRewrite<Void>( type.getSimpleName() + ".setProperty()" )
        {
            @Override
            Void perform( GraphDatabaseService graphDb )
            {
                entity.setProperty( key, value );
                return null;
            }
        };
    }

    private static Function<GraphDatabaseService, Pair<Node, Node>> aPairOfNodes()
    {
        return new Function<GraphDatabaseService, Pair<Node, Node>>()
        {
            @Override
            public Pair<Node, Node> apply( GraphDatabaseService graphDb )
            {
                return Pair.of( graphDb.createNode(), graphDb.createNode() );
            }
        };
    }

    private static Function<GraphDatabaseService, Relationship> relate( final Pair<Node, Node> nodes )
    {
        return new Function<GraphDatabaseService, Relationship>()
        {
            @Override
            public Relationship apply( GraphDatabaseService graphDb )
            {
                return nodes.first().createRelationshipTo( nodes.other(), withName( "RELATED" ) );
            }
        };
    }

    private static abstract class FailureRewrite<T> implements Function<GraphDatabaseService, T>
    {
        private final String message;

        FailureRewrite( String message )
        {
            this.message = message;
        }

        @Override
        public T apply( GraphDatabaseService graphDb )
        {
            try
            {
                return perform( graphDb );
            }
            catch ( AssertionError e )
            {
                AssertionError error = new AssertionError( message + ": " + e.getMessage() );
                error.setStackTrace( e.getStackTrace() );
                throw error;
            }
        }

        abstract T perform( GraphDatabaseService graphDb );
    }
}
